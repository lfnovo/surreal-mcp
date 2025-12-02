from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union

from loguru import logger
from surrealdb import RecordID  # type: ignore

from .connection_pool import (
    close_connection_pool,
    get_pool_stats,
    override_db_connection,
    pooled_db_connection,
)

T = TypeVar("T", Dict[str, Any], List[Dict[str, Any]])


def parse_record_ids(obj: Any) -> Any:
    """Recursively parse and convert RecordIDs into strings."""
    if isinstance(obj, dict):
        return {k: parse_record_ids(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [parse_record_ids(item) for item in obj]
    elif isinstance(obj, RecordID):
        return str(obj)
    return obj


def ensure_record_id(value: Union[str, RecordID]) -> RecordID:
    """Ensure a value is a RecordID."""
    if isinstance(value, RecordID):
        return value
    return RecordID.parse(value)


@asynccontextmanager
async def db_connection(
    namespace: Optional[str] = None,
    database: Optional[str] = None,
):
    """
    Database connection context manager using connection pool or override connection.

    If namespace and database are provided, creates a one-off connection with those values.
    Otherwise, uses the pooled connection with default namespace/database from env vars.

    Args:
        namespace: Optional namespace override
        database: Optional database override

    Yields:
        An authenticated AsyncSurreal connection
    """
    if namespace is not None and database is not None:
        # Use override connection for custom namespace/database
        async with override_db_connection(namespace, database) as connection:
            yield connection
    else:
        # Use pooled connection for default namespace/database
        async with pooled_db_connection() as connection:
            yield connection


async def repo_query(
    query_str: str,
    vars: Optional[Dict[str, Any]] = None,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Execute a SurrealQL query and return the results.

    Args:
        query_str: The SurrealQL query to execute
        vars: Optional variables for the query
        namespace: Optional namespace override (uses env var if not provided)
        database: Optional database override (uses env var if not provided)

    Returns:
        The query results as a list of dictionaries
    """
    async with db_connection(namespace, database) as connection:
        try:
            result = parse_record_ids(await connection.query(query_str, vars))
            if isinstance(result, str):
                raise RuntimeError(result)
            return result
        except Exception as e:
            logger.error(f"Query: {query_str[:200]} vars: {vars}")
            logger.exception(e)
            raise


async def repo_create(
    table: str,
    data: Dict[str, Any],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new record in the specified table.

    Args:
        table: The table to create the record in
        data: The record data
        namespace: Optional namespace override (uses env var if not provided)
        database: Optional database override (uses env var if not provided)

    Returns:
        The created record
    """
    # Remove 'id' attribute if it exists in data
    data.pop("id", None)
    data["created"] = datetime.now(timezone.utc)
    data["updated"] = datetime.now(timezone.utc)
    try:
        async with db_connection(namespace, database) as connection:
            return parse_record_ids(await connection.insert(table, data))
    except Exception as e:
        logger.exception(e)
        raise RuntimeError("Failed to create record")


async def repo_relate(
    source: str,
    relationship: str,
    target: str,
    data: Optional[Dict[str, Any]] = None,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Create a relationship between two records with optional data.

    Args:
        source: The source record ID
        relationship: The relationship/edge name
        target: The target record ID
        data: Optional data to store on the relationship
        namespace: Optional namespace override (uses env var if not provided)
        database: Optional database override (uses env var if not provided)

    Returns:
        The created relationship records
    """
    if data is None:
        data = {}
    query = f"RELATE {source}->{relationship}->{target} CONTENT $data;"

    return await repo_query(
        query,
        {"data": data},
        namespace=namespace,
        database=database,
    )


async def repo_upsert(
    table: str,
    id: Optional[str],
    data: Dict[str, Any],
    add_timestamp: bool = False,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Create or update a record in the specified table.

    Args:
        table: The table name
        id: Optional record ID (if provided, upserts that specific record)
        data: The record data to upsert
        add_timestamp: Whether to add/update the 'updated' timestamp
        namespace: Optional namespace override (uses env var if not provided)
        database: Optional database override (uses env var if not provided)

    Returns:
        The upserted record(s)
    """
    data.pop("id", None)
    if add_timestamp:
        data["updated"] = datetime.now(timezone.utc)
    query = f"UPSERT {id if id else table} MERGE $data;"
    return await repo_query(query, {"data": data}, namespace=namespace, database=database)


async def repo_update(
    table: str,
    id: str,
    data: Dict[str, Any],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Update an existing record by table and id.

    Args:
        table: The table name
        id: The record ID
        data: The data to update
        namespace: Optional namespace override (uses env var if not provided)
        database: Optional database override (uses env var if not provided)

    Returns:
        The updated record(s)
    """
    # If id already contains the table name, use it as is
    try:
        if isinstance(id, RecordID) or (":" in id and id.startswith(f"{table}:")):
            record_id = id
        else:
            record_id = f"{table}:{id}"

        data["updated"] = datetime.now(timezone.utc)
        query = f"UPDATE {record_id} MERGE $data;"
        result = await repo_query(query, {"data": data}, namespace=namespace, database=database)
        return parse_record_ids(result)
    except Exception as e:
        raise RuntimeError(f"Failed to update record: {str(e)}")


async def repo_get_news_by_jota_id(jota_id: str) -> Dict[str, Any]:
    try:
        results = await repo_query(
            "SELECT * omit embedding FROM news where jota_id=$jota_id",
            {"jota_id": jota_id},
        )
        return parse_record_ids(results)
    except Exception as e:
        logger.exception(e)
        raise RuntimeError(f"Failed to fetch record: {str(e)}")


async def repo_delete(
    record_id: Union[str, RecordID],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
):
    """Delete a record by record id.

    Args:
        record_id: The record ID to delete
        namespace: Optional namespace override (uses env var if not provided)
        database: Optional database override (uses env var if not provided)

    Returns:
        The deletion result
    """
    try:
        async with db_connection(namespace, database) as connection:
            return await connection.delete(record_id)
    except Exception as e:
        logger.exception(e)
        raise RuntimeError(f"Failed to delete record: {str(e)}")


async def repo_insert(
    table: str,
    data: List[Dict[str, Any]],
    ignore_duplicates: bool = False,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Insert multiple records into a table.

    Args:
        table: The table to insert into
        data: List of records to insert
        ignore_duplicates: Whether to ignore duplicate key errors
        namespace: Optional namespace override (uses env var if not provided)
        database: Optional database override (uses env var if not provided)

    Returns:
        The inserted records
    """
    try:
        async with db_connection(namespace, database) as connection:
            return parse_record_ids(await connection.insert(table, data))
    except Exception as e:
        if ignore_duplicates and "already contains" in str(e):
            return []
        logger.exception(e)
        raise RuntimeError("Failed to create record")


# Connection pool utilities
async def get_connection_pool_stats() -> Dict[str, Any]:
    """Get connection pool statistics for monitoring."""
    return await get_pool_stats()


async def close_database_pool():
    """Close the database connection pool. Call this on application shutdown."""
    await close_connection_pool()
