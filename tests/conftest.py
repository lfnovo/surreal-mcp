"""Pytest configuration and fixtures for SurrealDB MCP Server tests."""

import asyncio
import os
import pytest
import pytest_asyncio
import uuid
from typing import AsyncGenerator, Dict, Any

from database import close_database_pool, repo_query


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
async def setup_test_db():
    """Set up test database environment variables."""
    # Use test-specific database to avoid conflicts
    test_suffix = uuid.uuid4().hex[:8]
    
    # Set test environment variables if not already set
    os.environ.setdefault("SURREAL_URL", "ws://localhost:8018/rpc")
    os.environ.setdefault("SURREAL_USER", "root")
    os.environ.setdefault("SURREAL_PASSWORD", "root")
    os.environ.setdefault("SURREAL_NAMESPACE", "test")
    os.environ.setdefault("SURREAL_DATABASE", f"test_{test_suffix}")
    
    # Import the server module AFTER setting env vars
    from surreal_mcp.server import mcp
    
    yield
    
    # Cleanup: close connection pool
    await close_database_pool()


@pytest_asyncio.fixture
async def clean_db(setup_test_db):
    """Ensure clean database state before each test."""
    # Get all tables and clean them
    try:
        db_info = await repo_query("INFO FOR DB")
        if isinstance(db_info, dict) and "tables" in db_info:
            for table_name in db_info["tables"].keys():
                if table_name:  # Skip empty table name
                    await repo_query(f"DELETE FROM {table_name}")
    except Exception:
        # If we can't clean, it's okay - might be first run
        pass
    
    yield
    
    # Cleanup after test (optional, but good practice)
    try:
        db_info = await repo_query("INFO FOR DB")
        if isinstance(db_info, dict) and "tables" in db_info:
            for table_name in db_info["tables"].keys():
                if table_name:  # Skip empty table name
                    await repo_query(f"DELETE FROM {table_name}")
    except Exception:
        pass


@pytest.fixture
def sample_user_data() -> Dict[str, Any]:
    """Sample user data for testing."""
    return {
        "name": "Test User",
        "email": "test@example.com",
        "age": 25,
        "active": True
    }


@pytest.fixture
def sample_product_data() -> Dict[str, Any]:
    """Sample product data for testing."""
    return {
        "name": "Test Product",
        "price": 99.99,
        "category": "Electronics",
        "in_stock": True
    }


@pytest.fixture
def sample_products_bulk() -> list[Dict[str, Any]]:
    """Sample bulk product data for testing."""
    return [
        {"name": "Laptop", "price": 999.99, "category": "Electronics"},
        {"name": "Mouse", "price": 29.99, "category": "Accessories"},
        {"name": "Keyboard", "price": 79.99, "category": "Accessories"},
        {"name": "Monitor", "price": 299.99, "category": "Electronics"},
        {"name": "USB Cable", "price": 9.99, "category": "Accessories"}
    ]


@pytest_asyncio.fixture
async def created_user(clean_db, sample_user_data) -> Dict[str, Any]:
    """Create a test user and return its data."""
    # Import tools after env vars are set
    from surreal_mcp.server import create
    result = await create.fn(
        table="user",
        data=sample_user_data
    )
    return result["data"]


@pytest_asyncio.fixture
async def created_product(clean_db, sample_product_data) -> Dict[str, Any]:
    """Create a test product and return its data."""
    from surreal_mcp.server import create
    result = await create.fn(
        table="product",
        data=sample_product_data
    )
    return result["data"]


@pytest_asyncio.fixture
async def created_products(clean_db, sample_products_bulk) -> list[Dict[str, Any]]:
    """Create multiple test products and return their data."""
    from surreal_mcp.server import insert
    result = await insert.fn(
        table="product",
        data=sample_products_bulk
    )
    return result["data"]