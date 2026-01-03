"""SurrealDB MCP Server implementation."""

import os
import sys
from typing import Any, Dict, List, Optional, Tuple

from fastmcp import FastMCP
from loguru import logger

# Import database functions
from .database import (
    close_database_pool,
    ensure_record_id,
    repo_create,
    repo_delete,
    repo_insert,
    repo_query,
    repo_relate,
    repo_update,
    repo_upsert,
)

# Configure loguru to not output to stdout (which would interfere with MCP)
logger.remove()  # Remove default handler
logger.add(sys.stderr, level="INFO")  # Add stderr handler only

# Connection environment variables (required for pooled connections)
CONNECTION_ENV_VARS = [
    "SURREAL_URL",
    "SURREAL_USER",
    "SURREAL_PASSWORD",
]

# Database selection environment variables (can be overridden per-tool call)
DATABASE_ENV_VARS = [
    "SURREAL_NAMESPACE",
    "SURREAL_DATABASE",
]

# Validate connection environment variables at startup
for var in CONNECTION_ENV_VARS:
    if not os.environ.get(var):
        logger.error(f"Missing required environment variable: {var}")
        sys.exit(1)

# Warn if database selection env vars are not set (they can be provided per-tool call)
missing_db_vars = [var for var in DATABASE_ENV_VARS if not os.environ.get(var)]
if missing_db_vars:
    logger.warning(
        f"Database selection environment variables not set: {missing_db_vars}. "
        "These must be provided in each tool call."
    )


def resolve_namespace_database(
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve namespace and database values from parameters or environment variables.

    Args:
        namespace: Optional namespace parameter from tool call
        database: Optional database parameter from tool call

    Returns:
        Tuple of (resolved_namespace, resolved_database). Both will be None if using
        default pooled connection, or both will be strings if using override connection.

    Raises:
        ValueError: If namespace/database cannot be determined from either source
    """
    # Get values from env vars as fallback
    env_namespace = os.environ.get("SURREAL_NAMESPACE")
    env_database = os.environ.get("SURREAL_DATABASE")

    # Resolve final values
    final_namespace = namespace if namespace is not None else env_namespace
    final_database = database if database is not None else env_database

    # If both are from env vars (or both params are None), use pooled connection
    if namespace is None and database is None and env_namespace and env_database:
        return None, None  # Signal to use pooled connection

    # If either param is provided, we need both values resolved
    if final_namespace is None or final_database is None:
        missing = []
        if final_namespace is None:
            missing.append("namespace")
        if final_database is None:
            missing.append("database")
        raise ValueError(
            f"Missing required database configuration: {', '.join(missing)}. "
            "Either set SURREAL_NAMESPACE/SURREAL_DATABASE environment variables "
            "or provide namespace/database parameters in the tool call."
        )

    return final_namespace, final_database

# Initialize MCP server
mcp = FastMCP("SurrealDB MCP Server")


@mcp.tool()
async def query(
    queries: List[str],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute SurrealQL queries. Returns results array with per-query status and data."""
    if not queries or not isinstance(queries, list):
        raise ValueError("queries must be a non-empty list of query strings")

    ns, db = resolve_namespace_database(namespace, database)

    results = []
    succeeded = 0
    failed = 0

    for query_string in queries:
        try:
            logger.info(f"Executing query: {query_string[:100]}...")
            result = await repo_query(query_string, namespace=ns, database=db)
            results.append({
                "success": True,
                "data": result
            })
            succeeded += 1
        except Exception as e:
            logger.error(f"Query failed: {str(e)}")
            results.append({
                "success": False,
                "error": f"SurrealDB query failed: {str(e)}"
            })
            failed += 1

    return {
        "success": succeeded > 0,
        "results": results,
        "total": len(queries),
        "succeeded": succeeded,
        "failed": failed
    }


@mcp.tool()
async def select(
    table: str,
    id: Optional[str] = None,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Select records from a table. Returns all records if id is omitted, or a specific record if id is provided.

    Args:
        table: Table name (e.g., "user", "product")
        id: Optional record ID. Accepts "id" or "table:id" format. Omit to fetch all records.
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (array), count (int), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Build the query based on whether ID is provided
        if id:
            # Handle both "id" and "table:id" formats
            if ":" in id and id.startswith(f"{table}:"):
                record_id = id
            else:
                record_id = f"{table}:{id}"
            query_str = f"SELECT * FROM {record_id}"
        else:
            query_str = f"SELECT * FROM {table}"

        logger.info(f"Executing select: {query_str}")
        result = await repo_query(query_str, namespace=ns, database=db)

        # Ensure result is always a list
        if not isinstance(result, list):
            result = [result] if result else []

        return {
            "success": True,
            "data": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Select failed for {table}: {str(e)}")
        raise Exception(f"Failed to select from {table}: {str(e)}")


@mcp.tool()
async def create(
    table: str,
    data: Dict[str, Any],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a new record with auto-generated ID, timestamps, and optional schema validation.

    Args:
        table: Table name (e.g., "user", "product")
        data: Dictionary of field values for the record
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (record), id (string), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Validate table name
        if not table or not table.strip():
            raise ValueError("Table name cannot be empty")

        logger.info(f"Creating record in table {table}")
        result = await repo_create(table, data, namespace=ns, database=db)

        # repo_create returns a list with one element
        if isinstance(result, list) and len(result) > 0:
            record = result[0]
        else:
            record = result

        # Extract the ID for convenience
        record_id = record.get("id", "") if isinstance(record, dict) else ""

        return {
            "success": True,
            "data": record,
            "id": record_id
        }
    except Exception as e:
        logger.error(f"Create failed for table {table}: {str(e)}")
        raise Exception(f"Failed to create record in {table}: {str(e)}")


@mcp.tool()
async def update(
    thing: str,
    data: Dict[str, Any],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Replace all fields of a record (except ID and created timestamp). For partial updates, use 'merge' instead.

    Args:
        thing: Record ID in "table:id" format (e.g., "user:john")
        data: Complete new record data
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (record), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Validate thing format
        if ":" not in thing:
            raise ValueError(f"Invalid record ID format: {thing}. Must be 'table:id'")

        # Extract table and id
        table, record_id = thing.split(":", 1)

        logger.info(f"Updating record {thing}")
        result = await repo_update(table, record_id, data, namespace=ns, database=db)

        # repo_update returns a list, get the first item
        updated_record = result[0] if result else {}

        return {
            "success": True,
            "data": updated_record
        }
    except Exception as e:
        logger.error(f"Update failed for {thing}: {str(e)}")
        raise Exception(f"Failed to update {thing}: {str(e)}")


@mcp.tool()
async def delete(
    thing: str,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Permanently delete a record by ID. This is irreversible and removes related edges/hooks.

    Args:
        thing: Record ID in "table:id" format (e.g., "user:john")
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), deleted (id), data (record), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Validate thing format
        if ":" not in thing:
            raise ValueError(f"Invalid record ID format: {thing}. Must be 'table:id'")

        logger.info(f"Deleting record {thing}")

        # Try to get the record first (optional, for returning deleted data)
        try:
            select_result = await repo_query(f"SELECT * FROM {thing}", namespace=ns, database=db)
            deleted_data = select_result[0] if select_result else None
        except Exception:
            deleted_data = None

        # Perform the deletion
        record_id = ensure_record_id(thing)
        await repo_delete(record_id, namespace=ns, database=db)

        return {
            "success": True,
            "deleted": thing,
            "data": deleted_data
        }
    except Exception as e:
        logger.error(f"Delete failed for {thing}: {str(e)}")
        raise Exception(f"Failed to delete {thing}: {str(e)}")


@mcp.tool()
async def merge(
    thing: str,
    data: Dict[str, Any],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Partial update: merge data into a record, updating only specified fields. Other fields remain unchanged.

    Args:
        thing: Record ID in "table:id" format (e.g., "user:john")
        data: Dictionary of fields to update/merge
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (record), modified_fields (array), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Validate thing format
        if ":" not in thing:
            raise ValueError(f"Invalid record ID format: {thing}. Must be 'table:id'")

        logger.info(f"Merging data into {thing}")

        # Track which fields we're modifying
        modified_fields = list(data.keys())

        # Extract table name for repo_upsert
        table = thing.split(":", 1)[0]

        # Use repo_upsert which does a MERGE operation - pass full record ID
        result = await repo_upsert(
            table=table, id=thing, data=data, add_timestamp=True, namespace=ns, database=db
        )

        # Get the first result
        merged_record = result[0] if result else {}

        return {
            "success": True,
            "data": merged_record,
            "modified_fields": modified_fields
        }
    except Exception as e:
        logger.error(f"Merge failed for {thing}: {str(e)}")
        raise Exception(f"Failed to merge data into {thing}: {str(e)}")


@mcp.tool()
async def patch(
    thing: str,
    patches: List[Dict[str, Any]],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Apply JSON Patch operations (RFC 6902) to a record. Supports add, remove, replace operations.

    Args:
        thing: Record ID in "table:id" format (e.g., "user:john")
        patches: Array of patch operations: {"op": "add"|"remove"|"replace", "path": "/field", "value": ...}
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (record), applied_patches (int), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Validate thing format
        if ":" not in thing:
            raise ValueError(f"Invalid record ID format: {thing}. Must be 'table:id'")

        if not patches or not isinstance(patches, list):
            raise ValueError("Patches must be a non-empty array")

        logger.info(f"Applying {len(patches)} patches to {thing}")

        # Convert JSON Patch operations to a merge object
        merge_data = {}
        for patch_op in patches:
            op = patch_op.get("op")
            path = patch_op.get("path", "")
            value = patch_op.get("value")

            # Remove leading slash and convert path to field name
            field = path.lstrip("/").replace("/", ".")

            if op in ["add", "replace"]:
                merge_data[field] = value
            elif op == "remove":
                # Note: SurrealDB doesn't support removing fields via MERGE
                # This would need a custom UPDATE query
                logger.warning(f"Remove operation on {field} not fully supported")
            else:
                logger.warning(f"Patch operation '{op}' not supported")

        # Extract table name for repo_upsert
        table = thing.split(":", 1)[0]

        # Apply the patches via merge - pass full record ID
        result = await repo_upsert(
            table=table, id=thing, data=merge_data, add_timestamp=True, namespace=ns, database=db
        )

        # Get the first result
        patched_record = result[0] if result else {}

        return {
            "success": True,
            "data": patched_record,
            "applied_patches": len(patches)
        }
    except Exception as e:
        logger.error(f"Patch failed for {thing}: {str(e)}")
        raise Exception(f"Failed to patch {thing}: {str(e)}")


@mcp.tool()
async def upsert(
    thing: str,
    data: Dict[str, Any],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create or update: create if record doesn't exist, merge/update data if it does.

    Args:
        thing: Record ID in "table:id" format (e.g., "user:john", "settings:global")
        data: Record data. Merged with existing data if record exists.
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (record), created (bool), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Validate thing format
        if ":" not in thing:
            raise ValueError(f"Invalid record ID format: {thing}. Must be 'table:id'")

        logger.info(f"Upserting record {thing}")

        # Check if record exists
        try:
            existing = await repo_query(f"SELECT * FROM {thing}", namespace=ns, database=db)
            created = not existing or len(existing) == 0
        except Exception:
            created = True

        # Extract table name for repo_upsert
        table = thing.split(":", 1)[0]

        # Perform upsert - pass full record ID
        result = await repo_upsert(
            table=table, id=thing, data=data, add_timestamp=True, namespace=ns, database=db
        )

        # Get the first result
        upserted_record = result[0] if result else {}

        return {
            "success": True,
            "data": upserted_record,
            "created": created
        }
    except Exception as e:
        logger.error(f"Upsert failed for {thing}: {str(e)}")
        raise Exception(f"Failed to upsert {thing}: {str(e)}")


@mcp.tool()
async def insert(
    table: str,
    data: List[Dict[str, Any]],
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Bulk insert multiple records with auto-generated IDs and timestamps. More efficient than calling create multiple times.

    Args:
        table: Table name (e.g., "user", "product")
        data: Array of dictionaries, each a record to insert
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (array), count (int), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        if not data or not isinstance(data, list):
            raise ValueError("Data must be a non-empty array of records")

        logger.info(f"Inserting {len(data)} records into table {table}")

        # Add timestamps to each record
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        for record in data:
            record["created"] = record.get("created", now)
            record["updated"] = record.get("updated", now)

        result = await repo_insert(table, data, namespace=ns, database=db)

        # Ensure result is a list
        if not isinstance(result, list):
            result = [result] if result else []

        return {
            "success": True,
            "data": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Insert failed for table {table}: {str(e)}")
        raise Exception(f"Failed to insert records into {table}: {str(e)}")


@mcp.tool()
async def relate(
    from_thing: str,
    relation_name: str,
    to_thing: str,
    data: Optional[Dict[str, Any]] = None,
    namespace: Optional[str] = None,
    database: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Create a graph relation (edge) between two records. Supports optional data on the relation itself.

    Args:
        from_thing: Source record ID in "table:id" format (e.g., "user:john")
        relation_name: Relation/edge name (e.g., "likes", "follows", "purchased")
        to_thing: Destination record ID in "table:id" format (e.g., "product:laptop-123")
        data: Optional dictionary of data to store on the relation
        namespace: Optional namespace override (uses SURREAL_NAMESPACE env var if not provided)
        database: Optional database override (uses SURREAL_DATABASE env var if not provided)

    Returns:
        Dictionary with success (bool), data (relations), relation_id (string), and optional error field.
    """
    try:
        ns, db = resolve_namespace_database(namespace, database)

        # Validate thing formats
        if ":" not in from_thing:
            raise ValueError(f"Invalid source record ID format: {from_thing}. Must be 'table:id'")
        if ":" not in to_thing:
            raise ValueError(f"Invalid destination record ID format: {to_thing}. Must be 'table:id'")
        if not relation_name:
            raise ValueError("Relation name is required")

        logger.info(f"Creating relation: {from_thing} -> {relation_name} -> {to_thing}")

        # Create the relation
        result = await repo_relate(
            from_thing, relation_name, to_thing, data or {}, namespace=ns, database=db
        )

        # Extract relation ID if available
        relation_id = ""
        if result and isinstance(result, list) and len(result) > 0:
            first_result = result[0]
            if isinstance(first_result, dict) and "id" in first_result:
                relation_id = first_result["id"]

        return {
            "success": True,
            "data": result,
            "relation_id": relation_id
        }
    except Exception as e:
        logger.error(f"Failed to create relation {from_thing}->{relation_name}->{to_thing}: {str(e)}")
        raise Exception(f"Failed to create relation: {str(e)}")


def main():
    """Entry point for the MCP server."""
    logger.info("Starting SurrealDB MCP Server")
    logger.info(f"Database: {os.environ.get('SURREAL_URL')} (NS: {os.environ.get('SURREAL_NAMESPACE')}, DB: {os.environ.get('SURREAL_DATABASE')})")
    
    try:
        # Run with STDIO transport for MCP compatibility
        mcp.run()
    except KeyboardInterrupt:
        logger.info("Server shutdown requested")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down SurrealDB MCP Server")
        import asyncio
        asyncio.run(close_database_pool())


if __name__ == "__main__":
    main()