# SurrealDB MCP Server

<div align="center">
  <img src="assets/images/surreal-logo.jpg" alt="SurrealDB Logo" width="200">
  
  **A Model Context Protocol (MCP) server that enables AI assistants to interact with SurrealDB databases**
  
  [![Test](https://github.com/YOUR_USERNAME/surreal-mcp/actions/workflows/test.yml/badge.svg)](https://github.com/YOUR_USERNAME/surreal-mcp/actions/workflows/test.yml)
  [![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
  [![FastMCP](https://img.shields.io/badge/FastMCP-2.11%2B-green)](https://github.com/jlowin/fastmcp)
  [![SurrealDB](https://img.shields.io/badge/SurrealDB-2.0%2B-purple)](https://surrealdb.com/)
</div>

<a href="https://glama.ai/mcp/servers/@lfnovo/surreal-mcp">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@lfnovo/surreal-mcp/badge" alt="surreal-mcp MCP server" />
</a>

## =� Overview

The SurrealDB MCP Server bridges the gap between AI assistants and SurrealDB, providing a standardized interface for database operations through the Model Context Protocol. This enables LLMs to:

- Execute complex SurrealQL queries
- Perform CRUD operations on records
- Manage graph relationships
- Handle bulk operations efficiently
- Work with SurrealDB's unique features like record IDs and graph edges

## ( Features

- **Full SurrealQL Support**: Execute any SurrealQL query directly
- **Comprehensive CRUD Operations**: Create, read, update, delete with ease
- **Graph Database Operations**: Create and traverse relationships between records
- **Bulk Operations**: Efficient multi-record inserts
- **Smart Updates**: Full updates, merges, and patches
- **Type-Safe**: Proper handling of SurrealDB's RecordIDs
- **Connection Pooling**: Efficient database connection management
- **Detailed Documentation**: Extensive docstrings for AI comprehension

## =� Prerequisites

- Python 3.10 or higher
- SurrealDB instance (local or remote)
- MCP-compatible client (Claude Desktop, MCP CLI, etc.)

## =� Installation

### Using uvx (Simplest - No Installation Required)

```bash
# Run directly from PyPI (once published)
uvx surreal-mcp

# Or run from GitHub
uvx --from git+https://github.com/yourusername/surreal-mcp.git surreal-mcp
```

### Using uv (Recommended for Development)

```bash
# Clone the repository
git clone https://github.com/yourusername/surreal-mcp.git
cd surreal-mcp

# Install dependencies
uv sync

# Run the server (multiple ways)
uv run surreal-mcp
# or
uv run python -m surreal_mcp
# or
uv run python main.py
```

### Using pip

```bash
# Clone the repository
git clone https://github.com/yourusername/surreal-mcp.git
cd surreal-mcp

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install package
pip install -e .

# Run the server
surreal-mcp
# or
python -m surreal_mcp
```

## � Configuration

The server requires the following environment variables:

| Variable | Description | Example |
|----------|-------------|---------|
| `SURREAL_URL` | SurrealDB connection URL | `ws://localhost:8000/rpc` |
| `SURREAL_USER` | Database username | `root` |
| `SURREAL_PASSWORD` | Database password | `root` |
| `SURREAL_NAMESPACE` | SurrealDB namespace | `test` |
| `SURREAL_DATABASE` | SurrealDB database | `test` |

### Setting Environment Variables

You can copy `.env.example` to `.env` and update with your values:

```bash
cp .env.example .env
# Edit .env with your database credentials
```

Or set them manually:

```bash
export SURREAL_URL="ws://localhost:8000/rpc"
export SURREAL_USER="root"
export SURREAL_PASSWORD="root"
export SURREAL_NAMESPACE="test"
export SURREAL_DATABASE="test"
```

### MCP Client Configuration

Add to your MCP client settings (e.g., Claude Desktop):

**Using uvx (recommended):**
```json
{
  "mcpServers": {
    "surrealdb": {
      "command": "uvx",
      "args": ["surreal-mcp"],
      "env": {
        "SURREAL_URL": "ws://localhost:8000/rpc",
        "SURREAL_USER": "root",
        "SURREAL_PASSWORD": "root",
        "SURREAL_NAMESPACE": "test",
        "SURREAL_DATABASE": "test"
      }
    }
  }
}
```

**Using local installation:**
```json
{
  "mcpServers": {
    "surrealdb": {
      "command": "uv",
      "args": ["run", "surreal-mcp"],
      "env": {
        "SURREAL_URL": "ws://localhost:8000/rpc",
        "SURREAL_USER": "root",
        "SURREAL_PASSWORD": "root",
        "SURREAL_NAMESPACE": "test",
        "SURREAL_DATABASE": "test"
      }
    }
  }
}
```

## =' Available Tools

### 1. query
Execute raw SurrealQL queries for complex operations.

```surrealql
-- Example: Complex query with graph traversal
SELECT *, ->purchased->product FROM user WHERE age > 25
```

### 2. select
Retrieve all records from a table or a specific record by ID.

```python
# Get all users
select("user")

# Get specific user
select("user", "john")
```

### 3. create
Create a new record with auto-generated ID.

```python
create("user", {
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30
})
```

### 4. update
Replace entire record content (preserves ID and timestamps).

```python
update("user:john", {
    "name": "John Smith",
    "email": "john.smith@example.com",
    "age": 31
})
```

### 5. delete
Permanently remove a record from the database.

```python
delete("user:john")
```

### 6. merge
Partially update specific fields without affecting others.

```python
merge("user:john", {
    "email": "newemail@example.com",
    "verified": True
})
```

### 7. patch
Apply JSON Patch operations (RFC 6902) to records.

```python
patch("user:john", [
    {"op": "replace", "path": "/email", "value": "new@example.com"},
    {"op": "add", "path": "/verified", "value": True}
])
```

### 8. upsert
Create or update a record with specific ID.

```python
upsert("settings:global", {
    "theme": "dark",
    "language": "en"
})
```

### 9. insert
Bulk insert multiple records efficiently.

```python
insert("product", [
    {"name": "Laptop", "price": 999.99},
    {"name": "Mouse", "price": 29.99},
    {"name": "Keyboard", "price": 79.99}
])
```

### 10. relate
Create graph relationships between records.

```python
relate(
    "user:john",           # from
    "purchased",           # relation name
    "product:laptop-123",  # to
    {"quantity": 1, "date": "2024-01-15"}  # relation data
)
```

## =� Examples

### Basic CRUD Operations

```python
# Create a user
user = create("user", {"name": "Alice", "email": "alice@example.com"})

# Update specific fields
merge(user["id"], {"verified": True, "last_login": "2024-01-01"})

# Query with conditions
results = query("SELECT * FROM user WHERE verified = true ORDER BY created DESC")

# Delete when done
delete(user["id"])
```

### Working with Relationships

```python
# Create entities
user = create("user", {"name": "John"})
product = create("product", {"name": "Laptop", "price": 999})

# Create relationship
relate(user["id"], "purchased", product["id"], {
    "quantity": 1,
    "total": 999,
    "date": "2024-01-15"
})

# Query relationships
purchases = query(f"SELECT * FROM {user['id']}->purchased->product")
```

### Bulk Operations

```python
# Insert multiple records
products = insert("product", [
    {"name": "Laptop", "category": "Electronics", "price": 999},
    {"name": "Mouse", "category": "Electronics", "price": 29},
    {"name": "Desk", "category": "Furniture", "price": 299}
])

# Bulk update with query
query("UPDATE product SET on_sale = true WHERE category = 'Electronics'")
```

## <� Architecture

The server is built with:
- **FastMCP**: Simplified MCP server implementation
- **SurrealDB Python SDK**: Official database client
- **Connection Pooling**: Efficient connection management
- **Async/Await**: Non-blocking database operations

## >� Testing

The project includes a comprehensive test suite using pytest.

### Prerequisites
- SurrealDB instance running locally
- Test database access (uses temporary test databases)

### Running Tests

```bash
# Make sure SurrealDB is running
surreal start --user root --pass root

# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=surreal_mcp

# Run specific test file
uv run pytest tests/test_tools.py

# Run specific test class or method
uv run pytest tests/test_tools.py::TestQueryTool
uv run pytest tests/test_tools.py::TestQueryTool::test_query_simple

# Run with verbose output
uv run pytest -v

# Run only tests matching a pattern
uv run pytest -k "test_create"
```

### Test Structure

```
tests/
├── __init__.py
├── conftest.py         # Fixtures and test configuration
├── test_tools.py       # Tests for all MCP tools
└── test_server.py      # Tests for server configuration
```

### Writing Tests

The test suite includes fixtures for common test data:
- `clean_db` - Ensures clean database state
- `sample_user_data` - Sample user data
- `created_user` - Pre-created user record
- `created_product` - Pre-created product record

Example test:
```python
@pytest.mark.asyncio
async def test_create_user(clean_db, sample_user_data):
    result = await mcp._tools["create"].func(
        table="user",
        data=sample_user_data
    )
    assert result["success"] is True
    assert result["data"]["email"] == sample_user_data["email"]
```

## > Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## =� License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## =O Acknowledgments

- [SurrealDB](https://surrealdb.com/) for the amazing graph database
- [FastMCP](https://github.com/jlowin/fastmcp) for simplifying MCP server development
- [Model Context Protocol](https://modelcontextprotocol.io/) for the standardized AI-tool interface

## =� Support

- =� Email: your.email@example.com
- =� Discord: [Join our server](https://discord.gg/yourserver)
- = Issues: [GitHub Issues](https://github.com/yourusername/surreal-mcp/issues)

---

<div align="center">
  Made with d for the SurrealDB and MCP communities
</div>