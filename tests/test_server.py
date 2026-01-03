"""Tests for SurrealDB MCP Server startup and configuration."""

import os
import pytest
import sys
from unittest.mock import patch, MagicMock

from surreal_mcp.server import mcp, main


class TestServerConfiguration:
    """Test server configuration and environment variables."""
    
    def test_mcp_server_initialized(self):
        """Test that MCP server is properly initialized."""
        assert mcp is not None
        assert mcp.name == "SurrealDB MCP Server"
    
    def test_tools_registered(self):
        """Test that all tools are registered."""
        expected_tools = [
            "query", "select", "create", "update", "delete",
            "merge", "patch", "upsert", "insert", "relate"
        ]
        
        # For FastMCP, tools are registered synchronously even if decorated async
        tools = mcp._tool_manager._tools
        registered_tools = list(tools.keys())
        
        for tool in expected_tools:
            assert tool in registered_tools, f"Tool '{tool}' not registered"
        
        assert len(registered_tools) == len(expected_tools)
    
    def test_tool_signatures(self):
        """Test that tools have correct signatures."""
        tools = mcp._tool_manager._tools

        # Test query tool
        query_tool = tools["query"]
        params = query_tool.fn.__code__.co_varnames
        assert "query_string" in params
        assert "namespace" in params
        assert "database" in params

        # Test select tool
        select_tool = tools["select"]
        params = select_tool.fn.__code__.co_varnames
        assert "table" in params
        assert "id" in params
        assert "namespace" in params
        assert "database" in params

        # Test create tool
        create_tool = tools["create"]
        params = create_tool.fn.__code__.co_varnames
        assert "table" in params
        assert "data" in params
        assert "namespace" in params
        assert "database" in params

        # Test relate tool
        relate_tool = tools["relate"]
        params = relate_tool.fn.__code__.co_varnames
        assert "from_thing" in params
        assert "relation_name" in params
        assert "to_thing" in params
        assert "data" in params
        assert "namespace" in params
        assert "database" in params


class TestEnvironmentValidation:
    """Test environment variable validation."""

    def test_connection_env_vars_required(self):
        """Test server exits when connection env vars are missing."""
        # Connection vars are required at startup
        connection_vars = ["SURREAL_URL", "SURREAL_USER", "SURREAL_PASSWORD"]

        # Save original env vars
        original_env = {var: os.environ.get(var) for var in connection_vars}

        # Test each missing connection variable
        for missing_var in connection_vars:
            # Clear all connection vars
            for var in connection_vars:
                if var in os.environ:
                    del os.environ[var]

            # Set all except the one we're testing
            for var in connection_vars:
                if var != missing_var:
                    os.environ[var] = "test_value"

            # Import should fail due to missing env var
            with pytest.raises(SystemExit):
                # We need to reload the module to trigger the env var check
                if "surreal_mcp.server" in sys.modules:
                    del sys.modules["surreal_mcp.server"]
                import surreal_mcp.server

        # Restore original env vars
        for var, value in original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

    def test_database_env_vars_optional(self):
        """Test that database env vars (SURREAL_NAMESPACE, SURREAL_DATABASE) are optional at startup."""
        # These are only required at tool call time, not at startup
        from surreal_mcp.server import mcp

        # Server should be initialized even without database env vars
        assert mcp is not None

    def test_connection_env_vars_present(self):
        """Test that connection env vars are set in test environment."""
        connection_vars = ["SURREAL_URL", "SURREAL_USER", "SURREAL_PASSWORD"]

        for var in connection_vars:
            assert var in os.environ, f"Missing required connection env var: {var}"


class TestMainFunction:
    """Test the main entry point function."""

    def test_main_function_exists(self):
        """Test that main function exists and is callable."""
        assert callable(main)

    def test_main_function_imports(self):
        """Test that main function can be imported."""
        from surreal_mcp.server import main as imported_main

        assert callable(imported_main)


class TestToolDocstrings:
    """Test that all tools have comprehensive docstrings."""
    
    def test_all_tools_have_docstrings(self):
        """Verify all tools have docstrings."""
        tools = mcp._tool_manager._tools
        for tool_name, tool in tools.items():
            assert tool.fn.__doc__ is not None, f"Tool '{tool_name}' missing docstring"
            
            # Check docstring has minimum content
            doc = tool.fn.__doc__
            assert len(doc) > 100, f"Tool '{tool_name}' has too short docstring"
            assert "Args:" in doc, f"Tool '{tool_name}' missing Args section"
            assert "Returns:" in doc, f"Tool '{tool_name}' missing Returns section"
            assert "Example" in doc, f"Tool '{tool_name}' missing Example section"