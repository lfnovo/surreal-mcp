# Feature: Per-Tool Namespace and Database Override

## Feature Description
Allow AI agents to specify a different namespace and/or database when calling any MCP tool, instead of being locked to the values configured via environment variables. This enables multi-tenant scenarios, cross-database queries, and more flexible database operations within a single MCP server instance.

The parameters should be optional when environment variables are set (using env vars as defaults), but required when env vars are not configured.

## User Story
As an AI agent developer
I want to specify namespace and database per tool call
So that I can access multiple databases/namespaces without running separate MCP server instances

## Problem Statement
Currently, the SurrealDB MCP server is locked to a single namespace and database configured via environment variables at startup. This prevents:
- Querying across multiple databases in the same session
- Multi-tenant applications where different users have different databases
- Development workflows that need to switch between test/staging/production databases
- Graph traversals that span multiple namespaces

## Solution Statement
Add optional `namespace` and `database` parameters to all 10 MCP tools. The solution will:
1. Accept optional `namespace` and `database` parameters on each tool
2. Use environment variable values as defaults when parameters are omitted
3. Fail with a clear error message if neither env vars nor parameters provide the required values
4. Create temporary connections with the specified namespace/database for the operation
5. Maintain backward compatibility - existing tool calls without these parameters continue to work

## Relevant Files
Use these files to implement the feature:

- **surreal_mcp/server.py** - Main server file containing all 10 tool definitions. Each tool function needs to accept new optional parameters and pass them to the database layer.
- **surreal_mcp/database/__init__.py** - Database repository functions that execute queries. These need to accept and use namespace/database overrides.
- **surreal_mcp/database/connection_pool.py** - Connection pool implementation. May need a method to get connections with different namespace/database without affecting the pool.
- **tests/test_tools.py** - Existing tool tests. Need to add tests for namespace/database override functionality.
- **tests/conftest.py** - Test fixtures. May need new fixtures for testing with different namespaces/databases.

### New Files
- **tests/test_namespace_override.py** - New test file dedicated to testing namespace/database override functionality across all tools.

## Implementation Plan

### Phase 1: Foundation
Modify the database layer to support namespace/database overrides:
1. Update `connection_pool.py` to support creating connections with custom namespace/database
2. Update `database/__init__.py` repository functions to accept optional namespace/database parameters
3. Create a helper function for connection resolution that handles the fallback logic

### Phase 2: Core Implementation
Update all 10 tools in `server.py` to:
1. Add optional `namespace` and `database` parameters to each tool function
2. Implement validation logic (fail if no env var AND no parameter provided)
3. Pass the override values to the database layer
4. Update docstrings to document the new parameters

### Phase 3: Integration
1. Add comprehensive tests for the new functionality
2. Update documentation
3. Test backward compatibility with existing tool calls

## Step by Step Tasks

### Step 1: Add Connection Override Support to Connection Pool
- Add a new async function `get_override_connection(namespace: str, database: str)` in `connection_pool.py`
- This function creates a one-off connection (not pooled) with the specified namespace/database
- Add corresponding async context manager `override_db_connection(namespace, database)`
- Keep the existing pooled connection for default operations (better performance for repeated calls)

### Step 2: Update Database Repository Functions
- Modify `db_connection()` context manager in `database/__init__.py` to accept optional namespace/database
- Update `repo_query()` to accept optional `namespace` and `database` parameters
- Update `repo_create()` to accept optional `namespace` and `database` parameters
- Update `repo_upsert()` to accept optional `namespace` and `database` parameters
- Update `repo_update()` to accept optional `namespace` and `database` parameters
- Update `repo_delete()` to accept optional `namespace` and `database` parameters
- Update `repo_insert()` to accept optional `namespace` and `database` parameters
- Update `repo_relate()` to accept optional `namespace` and `database` parameters

### Step 3: Create Validation Helper
- Create a helper function `resolve_namespace_database(namespace: Optional[str], database: Optional[str]) -> Tuple[str, str]` in `server.py`
- The function should:
  - Return provided values if given
  - Fall back to environment variables if not provided
  - Raise a clear error if neither source provides the values

### Step 4: Update Query Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_query()`
- Update docstring with new parameter documentation

### Step 5: Update Select Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_query()`
- Update docstring with new parameter documentation

### Step 6: Update Create Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_create()`
- Update docstring with new parameter documentation

### Step 7: Update Update Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_update()`
- Update docstring with new parameter documentation

### Step 8: Update Delete Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_delete()` and `repo_query()`
- Update docstring with new parameter documentation

### Step 9: Update Merge Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_upsert()`
- Update docstring with new parameter documentation

### Step 10: Update Patch Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_upsert()`
- Update docstring with new parameter documentation

### Step 11: Update Upsert Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_query()` and `repo_upsert()`
- Update docstring with new parameter documentation

### Step 12: Update Insert Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_insert()`
- Update docstring with new parameter documentation

### Step 13: Update Relate Tool
- Add `namespace: Optional[str] = None` and `database: Optional[str] = None` parameters
- Call validation helper to resolve final values
- Pass resolved values to `repo_relate()`
- Update docstring with new parameter documentation

### Step 14: Update Server Startup Validation
- Modify the startup validation in `server.py` to NOT require env vars at startup
- Instead, log a warning if env vars are not set (but don't exit)
- The validation will happen at tool call time via the helper function

### Step 15: Create Override Connection Tests
- Create `tests/test_namespace_override.py`
- Add test fixtures for secondary test namespace/database
- Test that tools work with explicit namespace/database parameters
- Test that tools fall back to env vars when parameters not provided
- Test that tools fail with clear error when neither env vars nor parameters provided
- Test cross-database operations (create in one db, query in another)

### Step 16: Update Existing Tests
- Ensure all existing tests in `test_tools.py` still pass (backward compatibility)
- Add a few tests that explicitly pass namespace/database matching env vars (should work identically)

### Step 17: Run Validation Commands
- Run `uv run pytest` to validate all tests pass
- Run `uv run pytest --cov=surreal_mcp` to check coverage

## Testing Strategy

### Unit Tests
- Test `resolve_namespace_database()` helper with various input combinations
- Test each tool with explicit namespace/database parameters
- Test each tool without parameters (should use env var defaults)
- Test error handling when neither env vars nor parameters provided

### Integration Tests
- Test creating records in one database and querying in another
- Test that pooled connections still work for default database
- Test that override connections are properly closed after use

### Edge Cases
- Empty string vs None for namespace/database parameters
- Switching namespace/database mid-session
- Invalid namespace/database names
- Connection failures with override values
- Concurrent requests with different namespace/database values

## Acceptance Criteria
1. All 10 tools accept optional `namespace` and `database` parameters
2. Tools use environment variable values when parameters are not provided
3. Tools fail with clear error message when neither env vars nor parameters are set
4. Existing tool calls without new parameters continue to work (backward compatibility)
5. All existing tests pass without modification
6. New tests cover the override functionality for all tools
7. Docstrings for all tools document the new parameters
8. Server can start without namespace/database env vars (validation deferred to tool call)

## Validation Commands
Execute every command to validate the feature works correctly with zero regressions.

- `uv run pytest tests/test_server.py -v` - Run server configuration tests
- `uv run pytest tests/test_tools.py -v` - Run existing tool tests to verify backward compatibility
- `uv run pytest tests/test_namespace_override.py -v` - Run new override tests
- `uv run pytest --cov=surreal_mcp --cov-report=term-missing` - Run all tests with coverage report
- `uv run python -c "from surreal_mcp.server import mcp; print('Server imports successfully')"` - Verify server can be imported

## Notes
- The override connections should NOT be pooled since they may use different namespace/database combinations. This is a trade-off: flexibility vs performance. For high-volume override operations, users should consider running dedicated MCP server instances.
- Consider adding a `use_pool: bool = True` parameter in the future to allow users to opt-in to pooling for specific override combinations they use frequently.
- The SurrealDB Python SDK's `connection.use(namespace, database)` method can switch namespace/database on an existing connection, but this would affect other concurrent operations using the same pooled connection. Creating separate connections for overrides is safer.
