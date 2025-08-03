"""
SurrealDB Connection Pool Implementation

Provides efficient connection pooling for SurrealDB operations to resolve
connection exhaustion issues and improve performance.

Based on surrealengine AsyncConnectionPool pattern but simplified for our needs.
"""

import asyncio
import os
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional, Set
from weakref import WeakKeyDictionary

from loguru import logger
from surrealdb import AsyncSurreal


@dataclass
class PoolConfig:
    """Configuration for SurrealDB connection pool."""
    max_size: int = 10
    min_size: int = 2
    connection_timeout: int = 30
    pool_timeout: int = 10
    url: str = ""
    username: str = ""
    password: str = ""
    namespace: str = ""
    database: str = ""

    @classmethod
    def from_env(cls) -> "PoolConfig":
        """Create configuration from environment variables."""
        return cls(
            max_size=int(os.environ.get("SURREAL_POOL_SIZE", "10")),
            min_size=int(os.environ.get("SURREAL_POOL_MIN_SIZE", "2")),
            connection_timeout=int(os.environ.get("SURREAL_CONNECTION_TIMEOUT", "30")),
            pool_timeout=int(os.environ.get("SURREAL_POOL_TIMEOUT", "10")),
            url=os.environ["SURREAL_URL"],
            username=os.environ["SURREAL_USER"],
            password=os.environ["SURREAL_PASSWORD"],
            namespace=os.environ["SURREAL_NAMESPACE"],
            database=os.environ["SURREAL_DATABASE"],
        )


class AsyncSurrealConnectionPool:
    """Async connection pool for SurrealDB."""
    
    def __init__(self, config: PoolConfig):
        self.config = config
        self._pool: Set[AsyncSurreal] = set()
        self._in_use: Set[AsyncSurreal] = set()
        self._lock: Optional[asyncio.Lock] = None
        self._condition: Optional[asyncio.Condition] = None
        self._closed = False
        self._initialized = False
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        
    def _ensure_lock(self):
        """Ensure lock exists for current event loop."""
        current_loop = asyncio.get_running_loop()
        if self._loop is not current_loop:
            self._loop = current_loop
            self._lock = asyncio.Lock()
            self._condition = asyncio.Condition(self._lock)
            logger.trace(f"Created locks for event loop {id(current_loop)}")
        
    async def _create_connection(self) -> AsyncSurreal:
        """Create a new SurrealDB connection."""
        try:
            connection = AsyncSurreal(self.config.url)
            
            # Set timeout for connection operations
            await asyncio.wait_for(
                connection.signin({
                    "username": self.config.username,
                    "password": self.config.password,
                }),
                timeout=self.config.connection_timeout
            )
            
            await asyncio.wait_for(
                connection.use(self.config.namespace, self.config.database),
                timeout=self.config.connection_timeout
            )
            
            logger.trace("Created new SurrealDB connection")
            return connection
            
        except Exception as e:
            logger.error(f"Failed to create SurrealDB connection: {e}")
            raise
    
    async def _validate_connection(self, connection: AsyncSurreal) -> bool:
        """Validate that a connection is healthy."""
        try:
            # Simple health check query using SurrealDB syntax
            await asyncio.wait_for(
                connection.query("return 1"),
                timeout=5
            )
            return True
        except Exception as e:
            logger.trace(f"Connection validation failed: {e}")
            return False
    
    async def _ensure_initialized(self):
        """Ensure the pool is initialized with minimum connections."""
        if self._initialized:
            return
            
        self._ensure_lock()
        async with self._lock:
            if self._initialized:
                return
                
            # Create minimum connections
            for _ in range(self.config.min_size):
                try:
                    connection = await self._create_connection()
                    self._pool.add(connection)
                except Exception as e:
                    logger.warning(f"Failed to create initial connection: {e}")
                    
            self._initialized = True
            logger.info(f"Initialized SurrealDB connection pool with {len(self._pool)} connections")
    
    async def get_connection(self) -> AsyncSurreal:
        """Get a connection from the pool."""
        self._ensure_lock()
        await self._ensure_initialized()
        
        async with self._condition:
            # Wait for available connection or ability to create new one
            while True:
                if self._closed:
                    raise RuntimeError("Connection pool is closed")
                
                # Try to get an available connection
                if self._pool:
                    connection = self._pool.pop()
                    
                    # Validate connection
                    if await self._validate_connection(connection):
                        self._in_use.add(connection)
                        logger.trace(f"Retrieved connection from pool (in_use: {len(self._in_use)}, available: {len(self._pool)})")
                        return connection
                    else:
                        # Connection is invalid, close it and try again
                        try:
                            await asyncio.wait_for(connection.close(), timeout=5)
                        except Exception:
                            pass
                        continue
                
                # Create new connection if under max size
                if len(self._in_use) + len(self._pool) < self.config.max_size:
                    try:
                        connection = await self._create_connection()
                        self._in_use.add(connection)
                        logger.trace(f"Created new connection (in_use: {len(self._in_use)}, available: {len(self._pool)})")
                        return connection
                    except Exception as e:
                        logger.error(f"Failed to create new connection: {e}")
                        # Fall through to wait for available connection
                
                # Wait for a connection to be returned
                try:
                    await asyncio.wait_for(
                        self._condition.wait(),
                        timeout=self.config.pool_timeout
                    )
                except asyncio.TimeoutError:
                    raise RuntimeError(f"Timeout waiting for connection from pool (timeout: {self.config.pool_timeout}s)")
    
    async def return_connection(self, connection: AsyncSurreal):
        """Return a connection to the pool."""
        self._ensure_lock()
        async with self._condition:
            if connection in self._in_use:
                self._in_use.remove(connection)
                
                # Validate connection before returning to pool
                if await self._validate_connection(connection):
                    self._pool.add(connection)
                    logger.trace(f"Returned connection to pool (in_use: {len(self._in_use)}, available: {len(self._pool)})")
                else:
                    # Connection is invalid, close it
                    try:
                        await asyncio.wait_for(connection.close(), timeout=5)
                    except Exception:
                        pass
                    logger.trace(f"Discarded invalid connection (in_use: {len(self._in_use)}, available: {len(self._pool)})")
                
                self._condition.notify()
    
    async def close(self):
        """Close all connections in the pool."""
        self._ensure_lock()
        async with self._condition:
            self._closed = True
            
            # Close all connections
            all_connections = list(self._pool) + list(self._in_use)
            for connection in all_connections:
                try:
                    await asyncio.wait_for(connection.close(), timeout=5)
                except Exception:
                    pass
            
            self._pool.clear()
            self._in_use.clear()
            
            logger.info("Closed SurrealDB connection pool")
    
    def get_stats(self) -> dict:
        """Get pool statistics."""
        return {
            "total_connections": len(self._pool) + len(self._in_use),
            "available_connections": len(self._pool),
            "in_use_connections": len(self._in_use),
            "max_size": self.config.max_size,
            "min_size": self.config.min_size,
            "is_closed": self._closed,
        }


# Thread-safe registry of pools per event loop
_pools_lock = threading.Lock()
_pools: WeakKeyDictionary[asyncio.AbstractEventLoop, AsyncSurrealConnectionPool] = WeakKeyDictionary()


async def get_connection_pool() -> AsyncSurrealConnectionPool:
    """Get or create a connection pool for the current event loop."""
    loop = asyncio.get_running_loop()
    
    with _pools_lock:
        if loop not in _pools:
            config = PoolConfig.from_env()
            _pools[loop] = AsyncSurrealConnectionPool(config)
            logger.info(f"Created SurrealDB connection pool for event loop {id(loop)} with max_size={config.max_size}")
        
        return _pools[loop]


@asynccontextmanager
async def pooled_db_connection():
    """Context manager for getting a database connection from the pool."""
    pool = await get_connection_pool()
    connection = await pool.get_connection()
    
    try:
        yield connection
    finally:
        await pool.return_connection(connection)


async def close_connection_pool():
    """Close the connection pool for the current event loop."""
    loop = asyncio.get_running_loop()
    
    with _pools_lock:
        if loop in _pools:
            await _pools[loop].close()
            del _pools[loop]
            logger.info(f"Closed SurrealDB connection pool for event loop {id(loop)}")


async def get_pool_stats() -> dict:
    """Get connection pool statistics for the current event loop."""
    loop = asyncio.get_running_loop()
    
    with _pools_lock:
        if loop not in _pools:
            return {"error": "Pool not initialized for this event loop"}
        
        return _pools[loop].get_stats()