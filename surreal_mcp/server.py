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
    """
    Execute one or more SurrealQL queries against the connected SurrealDB database.

    This tool allows you to run any valid SurrealQL queries directly. Use this for complex
    queries that don't fit the other tool patterns, such as:
    - Complex SELECT queries with JOINs, GROUP BY, or aggregations
    - Custom DEFINE statements for schemas
    - Transaction blocks with BEGIN/COMMIT
    - Graph traversal queries

    Queries are executed sequentially. If a query fails, execution continues with the
    remaining queries, and the error is captured in that query's result.

    Args:
        queries: A list of SurrealQL queries to execute. Examples:
            - ["SELECT * FROM user WHERE age > 18"]
            - ["SELECT * FROM user", "SELECT * FROM product"]
            - ["CREATE user:alice SET name = 'Alice'", "CREATE user:bob SET name = 'Bob'"]
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if at least one query executed successfully
        - results: Array of per-query results, each containing:
            - success: Boolean indicating if this specific query succeeded
            - data: The query results (only present on success)
            - error: Error message (only present on failure)
        - total: Total number of queries executed
        - succeeded: Number of queries that succeeded
        - failed: Number of queries that failed

    Example:
        >>> await query(["SELECT * FROM user", "SELECT * FROM product"])
        {
            "success": true,
            "results": [
                {"success": true, "data": [{"id": "user:1", "name": "Alice"}]},
                {"success": true, "data": [{"id": "product:1", "name": "Laptop"}]}
            ],
            "total": 2,
            "succeeded": 2,
            "failed": 0
        }
    """
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
    Select all records from a table or a specific record by ID.

    This tool provides a simple way to retrieve data from SurrealDB tables. Use this when you need to:
    - Fetch all records from a table
    - Retrieve a specific record by its ID
    - Get data for display or further processing

    Args:
        table: The name of the table to select from (e.g., "user", "product", "order")
        id: Optional ID of a specific record to select. Can be:
            - Just the ID part (e.g., "john") - will be combined with table name
            - Full record ID (e.g., "user:john") - will be used as-is
            - None/omitted - selects all records from the table
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if the selection was successful
        - data: Array of records (even for single record selection)
        - count: Number of records returned
        - error: Error message if selection failed (only present on failure)

    Examples:
        >>> await select("user")  # Get all users
        {"success": true, "data": [...], "count": 42}

        >>> await select("user", "john")  # Get specific user
        {"success": true, "data": [{"id": "user:john", "name": "John Doe", ...}], "count": 1}

        >>> await select("product", "product:laptop-123")  # Using full ID
        {"success": true, "data": [{"id": "product:laptop-123", ...}], "count": 1}
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
    Create a new record in a SurrealDB table with the specified data.

    This tool creates a new record with an auto-generated ID. The system will automatically:
    - Generate a unique ID for the record
    - Add created/updated timestamps
    - Validate the data against any defined schema

    Args:
        table: The name of the table to create the record in (e.g., "user", "product")
        data: A dictionary containing the field values for the new record. Examples:
            - {"name": "Alice", "email": "alice@example.com", "age": 30}
            - {"title": "Laptop", "price": 999.99, "category": "electronics"}
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if creation was successful
        - data: The created record including its generated ID and timestamps
        - id: The ID of the newly created record (convenience field)
        - error: Error message if creation failed (only present on failure)

    Examples:
        >>> await create("user", {"name": "Alice", "email": "alice@example.com"})
        {
            "success": true,
            "data": {"id": "user:ulid", "name": "Alice", "email": "alice@example.com", "created": "2024-01-01T10:00:00Z"},
            "id": "user:ulid"
        }

    Note: If you need to specify a custom ID, use the 'upsert' tool instead.
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
    Update a specific record with new data, completely replacing its content.

    This tool performs a full update, replacing all fields (except ID and timestamps) with the provided data.
    For partial updates that only modify specific fields, use 'merge' or 'patch' instead.

    Args:
        thing: The full record ID to update in format "table:id" (e.g., "user:john", "product:laptop-123")
        data: Complete new data for the record. All existing fields will be replaced except:
            - The record ID (cannot be changed)
            - The 'created' timestamp (preserved from original)
            - The 'updated' timestamp (automatically set to current time)
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if update was successful
        - data: The updated record with all new values
        - error: Error message if update failed (only present on failure)

    Examples:
        >>> await update("user:john", {"name": "John Smith", "email": "john.smith@example.com", "age": 31})
        {
            "success": true,
            "data": {"id": "user:john", "name": "John Smith", "email": "john.smith@example.com", "age": 31, "updated": "2024-01-01T10:00:00Z"}
        }

    Warning: This replaces ALL fields. If you only want to update specific fields, use 'merge' instead.
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
    Delete a specific record from the database by its ID.

    This tool permanently removes a record from the database. Use with caution as this operation
    cannot be undone. The deletion will also:
    - Remove any graph edges (relations) connected to this record
    - Trigger any defined deletion events/hooks
    - Fail if the record is referenced by FOREIGN KEY constraints

    Args:
        thing: The full record ID to delete in format "table:id" (e.g., "user:john", "product:laptop-123")
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if deletion was successful
        - deleted: The ID of the deleted record
        - data: The deleted record data (if available)
        - error: Error message if deletion failed (only present on failure)

    Examples:
        >>> await delete("user:john")
        {"success": true, "deleted": "user:john", "data": {"id": "user:john", "name": "John Doe"}}

        >>> await delete("product:nonexistent")
        {"success": true, "deleted": "product:nonexistent", "data": null}  # No error even if record didn't exist

    Note: This operation is irreversible. Consider using soft deletes (status fields) for recoverable deletions.
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
    Merge data into a specific record, updating only the specified fields.

    This tool performs a partial update, only modifying the fields provided in the data parameter.
    All other fields remain unchanged. This is useful when you want to:
    - Update specific fields without affecting others
    - Add new fields to an existing record
    - Modify nested properties without replacing the entire object

    Args:
        thing: The full record ID to merge data into in format "table:id" (e.g., "user:john")
        data: Dictionary containing only the fields to update. Examples:
            - {"email": "newemail@example.com"} - updates only email
            - {"profile": {"bio": "New bio"}} - updates nested field
            - {"tags": ["python", "mcp"]} - replaces the tags array
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if merge was successful
        - data: The complete record after merging, with all fields
        - modified_fields: List of field names that were modified
        - error: Error message if merge failed (only present on failure)

    Examples:
        >>> await merge("user:john", {"email": "john.new@example.com", "verified": true})
        {
            "success": true,
            "data": {"id": "user:john", "name": "John Doe", "email": "john.new@example.com", "verified": true, "age": 30},
            "modified_fields": ["email", "verified"]
        }

    Note: This is equivalent to the 'patch' tool but uses object merging syntax instead of JSON Patch.
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
    Apply JSON Patch operations to a specific record (RFC 6902).

    This tool applies a sequence of patch operations to modify a record. However, since SurrealDB
    doesn't natively support JSON Patch, this implementation converts patches to a merge operation.
    Supported operations:
    - add: Add a new field or array element
    - remove: Remove a field (limited support)
    - replace: Replace a field value

    Args:
        thing: The full record ID to patch in format "table:id" (e.g., "user:john")
        patches: Array of patch operations. Each operation should have:
            - op: The operation type ("add", "remove", "replace", "move", "copy", "test")
            - path: The field path (e.g., "/email", "/profile/bio")
            - value: The value for add/replace operations
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if patch was successful
        - data: The complete record after applying patches
        - applied_patches: Number of patch operations applied
        - error: Error message if patch failed (only present on failure)

    Examples:
        >>> await patch("user:john", [
        ...     {"op": "replace", "path": "/email", "value": "john@newdomain.com"},
        ...     {"op": "add", "path": "/verified", "value": true}
        ... ])
        {
            "success": true,
            "data": {"id": "user:john", "email": "john@newdomain.com", "verified": true, ...},
            "applied_patches": 2
        }

    Note: This provides compatibility with JSON Patch but internally uses SurrealDB's merge.
    Complex operations like "move" or "test" are not fully supported.
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
    Upsert a record: create if it doesn't exist, merge/update if it does.

    This tool is perfect when you want to ensure a record exists with specific data, regardless
    of whether it already exists. It will:
    - Create a new record with the specified ID if it doesn't exist
    - Merge the provided data into the existing record if it does exist
    - Always succeed (unless there's a database error)

    Args:
        thing: The full record ID in format "table:id" (e.g., "user:john", "settings:global")
        data: The data for the record. If record exists, this will be merged with existing data
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if upsert was successful
        - data: The record after upserting
        - created: Boolean indicating if a new record was created (vs updated)
        - error: Error message if upsert failed (only present on failure)

    Examples:
        >>> await upsert("user:john", {"name": "John Doe", "email": "john@example.com"})
        {"success": true, "data": {"id": "user:john", "name": "John Doe", ...}, "created": true}

        >>> await upsert("user:john", {"email": "newemail@example.com"})  # Update existing
        {"success": true, "data": {"id": "user:john", "name": "John Doe", "email": "newemail@example.com", ...}, "created": false}

        >>> await upsert("settings:global", {"theme": "dark", "language": "en"})
        {"success": true, "data": {"id": "settings:global", "theme": "dark", "language": "en"}, "created": true}
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
    Insert multiple records into a table in a single operation.

    This tool is optimized for bulk inserts when you need to create many records at once.
    It's more efficient than calling 'create' multiple times. Each record will get:
    - An auto-generated unique ID
    - Automatic created/updated timestamps
    - Schema validation (if defined)

    Args:
        table: The name of the table to insert records into (e.g., "user", "product")
        data: Array of dictionaries, each representing a record to insert. Example:
            [
                {"name": "Alice", "email": "alice@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
                {"name": "Charlie", "email": "charlie@example.com"}
            ]
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if insertion was successful
        - data: Array of all inserted records with their generated IDs
        - count: Number of records successfully inserted
        - error: Error message if insertion failed (only present on failure)

    Examples:
        >>> await insert("user", [
        ...     {"name": "Alice", "role": "admin"},
        ...     {"name": "Bob", "role": "user"}
        ... ])
        {
            "success": true,
            "data": [
                {"id": "user:ulid1", "name": "Alice", "role": "admin", "created": "..."},
                {"id": "user:ulid2", "name": "Bob", "role": "user", "created": "..."}
            ],
            "count": 2
        }

    Note: For single record creation, use the 'create' tool instead.
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
    Create a graph relation (edge) between two records in SurrealDB.

    This tool creates relationships in SurrealDB's graph structure, allowing you to:
    - Connect records with named relationships
    - Store data on the relationship itself
    - Build complex graph queries later
    - Model many-to-many relationships efficiently

    Args:
        from_thing: The source record ID in format "table:id" (e.g., "user:john")
        relation_name: The name of the relation/edge table (e.g., "likes", "follows", "purchased")
        to_thing: The destination record ID in format "table:id" (e.g., "product:laptop-123")
        data: Optional dictionary containing data to store on the relation itself. Examples:
            - {"rating": 5, "review": "Great product!"}
            - {"quantity": 2, "price": 99.99}
            - {"since": "2024-01-01", "type": "friend"}
        namespace: Optional SurrealDB namespace override. If not provided, uses SURREAL_NAMESPACE env var.
        database: Optional SurrealDB database override. If not provided, uses SURREAL_DATABASE env var.

    Returns:
        A dictionary containing:
        - success: Boolean indicating if relation was created successfully
        - data: The created relation record(s)
        - relation_id: The ID of the created relation
        - error: Error message if creation failed (only present on failure)

    Examples:
        >>> await relate("user:john", "likes", "product:laptop-123", {"rating": 5})
        {
            "success": true,
            "data": [{"id": "likes:xyz", "in": "user:john", "out": "product:laptop-123", "rating": 5}],
            "relation_id": "likes:xyz"
        }

        >>> await relate("user:alice", "follows", "user:bob")
        {
            "success": true,
            "data": [{"id": "follows:abc", "in": "user:alice", "out": "user:bob"}],
            "relation_id": "follows:abc"
        }

    Note: You can query these relations later using graph syntax:
        SELECT * FROM user:john->likes->product
        SELECT * FROM user:alice->follows->user
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