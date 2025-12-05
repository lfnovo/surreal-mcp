"""Tests for namespace and database override functionality."""

import os
import pytest
from unittest.mock import patch


class TestResolveNamespaceDatabase:
    """Test the resolve_namespace_database helper function."""

    def test_resolve_with_env_vars_only(self):
        """Test resolution using only environment variables."""
        from surreal_mcp.server import resolve_namespace_database

        # When no params provided and env vars are set, returns None, None (use pooled)
        ns, db = resolve_namespace_database()
        assert ns is None
        assert db is None

    def test_resolve_with_explicit_params(self):
        """Test resolution with explicit parameters."""
        from surreal_mcp.server import resolve_namespace_database

        ns, db = resolve_namespace_database(namespace="custom_ns", database="custom_db")
        assert ns == "custom_ns"
        assert db == "custom_db"

    def test_resolve_with_partial_params_uses_env_fallback(self):
        """Test that partial params fall back to env vars for missing values."""
        from surreal_mcp.server import resolve_namespace_database

        # Only namespace provided, database from env
        ns, db = resolve_namespace_database(namespace="custom_ns")
        assert ns == "custom_ns"
        assert db == os.environ.get("SURREAL_DATABASE")

    def test_resolve_fails_without_env_or_params(self):
        """Test that resolution fails when neither env vars nor params are set."""
        from surreal_mcp.server import resolve_namespace_database

        # Temporarily unset env vars
        with patch.dict(os.environ, {"SURREAL_NAMESPACE": "", "SURREAL_DATABASE": ""}, clear=False):
            # Also need to handle the case where they're completely missing
            env_backup = {}
            for key in ["SURREAL_NAMESPACE", "SURREAL_DATABASE"]:
                if key in os.environ:
                    env_backup[key] = os.environ.pop(key)

            try:
                with pytest.raises(ValueError) as exc_info:
                    resolve_namespace_database()
                assert "Missing required database configuration" in str(exc_info.value)
            finally:
                # Restore env vars
                os.environ.update(env_backup)


class TestQueryToolWithOverride:
    """Test the query tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_query_with_default_namespace(self, clean_db):
        """Test query using default namespace/database from env vars."""
        from surreal_mcp.server import query

        result = await query.fn(queries=["RETURN 1"])
        assert result["success"] is True
        assert result["results"][0]["data"] == 1

    @pytest.mark.asyncio
    async def test_query_with_explicit_namespace(self, clean_db):
        """Test query with explicit namespace/database parameters."""
        from surreal_mcp.server import query

        # Use the same namespace/database as env vars to verify it works
        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await query.fn(
            queries=["RETURN 1"],
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["results"][0]["data"] == 1


class TestSelectToolWithOverride:
    """Test the select tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_select_with_explicit_namespace(self, clean_db, created_user):
        """Test select with explicit namespace/database parameters."""
        from surreal_mcp.server import select

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await select.fn(
            table="user",
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["count"] == 1


class TestCreateToolWithOverride:
    """Test the create tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_create_with_explicit_namespace(self, clean_db):
        """Test create with explicit namespace/database parameters."""
        from surreal_mcp.server import create

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await create.fn(
            table="test_override",
            data={"name": "Override Test"},
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["data"]["name"] == "Override Test"


class TestUpdateToolWithOverride:
    """Test the update tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_update_with_explicit_namespace(self, clean_db, created_user):
        """Test update with explicit namespace/database parameters."""
        from surreal_mcp.server import update

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await update.fn(
            thing=created_user["id"],
            data={"name": "Updated via override"},
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["data"]["name"] == "Updated via override"


class TestDeleteToolWithOverride:
    """Test the delete tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_delete_with_explicit_namespace(self, clean_db, created_user):
        """Test delete with explicit namespace/database parameters."""
        from surreal_mcp.server import delete

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await delete.fn(
            thing=created_user["id"],
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["deleted"] == created_user["id"]


class TestMergeToolWithOverride:
    """Test the merge tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_merge_with_explicit_namespace(self, clean_db, created_user):
        """Test merge with explicit namespace/database parameters."""
        from surreal_mcp.server import merge

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await merge.fn(
            thing=created_user["id"],
            data={"new_field": "via_override"},
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["data"]["new_field"] == "via_override"
        assert result["data"]["name"] == created_user["name"]  # Original preserved


class TestPatchToolWithOverride:
    """Test the patch tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_patch_with_explicit_namespace(self, clean_db, created_user):
        """Test patch with explicit namespace/database parameters."""
        from surreal_mcp.server import patch

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await patch.fn(
            thing=created_user["id"],
            patches=[{"op": "add", "path": "/patched", "value": True}],
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["data"]["patched"] is True


class TestUpsertToolWithOverride:
    """Test the upsert tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_upsert_with_explicit_namespace(self, clean_db):
        """Test upsert with explicit namespace/database parameters."""
        from surreal_mcp.server import upsert

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await upsert.fn(
            thing="override_test:specific_id",
            data={"name": "Upsert Override Test"},
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["created"] is True
        assert result["data"]["name"] == "Upsert Override Test"


class TestInsertToolWithOverride:
    """Test the insert tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_insert_with_explicit_namespace(self, clean_db):
        """Test insert with explicit namespace/database parameters."""
        from surreal_mcp.server import insert

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await insert.fn(
            table="override_bulk",
            data=[
                {"name": "Item 1"},
                {"name": "Item 2"},
            ],
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert result["count"] == 2


class TestRelateToolWithOverride:
    """Test the relate tool with namespace/database override."""

    @pytest.mark.asyncio
    async def test_relate_with_explicit_namespace(self, clean_db, created_user, created_product):
        """Test relate with explicit namespace/database parameters."""
        from surreal_mcp.server import relate

        ns = os.environ.get("SURREAL_NAMESPACE")
        db = os.environ.get("SURREAL_DATABASE")

        result = await relate.fn(
            from_thing=created_user["id"],
            relation_name="tested_override",
            to_thing=created_product["id"],
            data={"via": "override"},
            namespace=ns,
            database=db,
        )
        assert result["success"] is True
        assert len(result["data"]) > 0


class TestCrossDatabaseOperations:
    """Test operations that use different databases."""

    @pytest.mark.asyncio
    async def test_create_in_different_database(self, clean_db):
        """Test creating records in a different database using override."""
        from surreal_mcp.server import create, select, query

        # Get current namespace
        ns = os.environ.get("SURREAL_NAMESPACE")

        # Create a secondary test database
        secondary_db = "test_secondary_db"

        # Create a record in the secondary database
        result = await create.fn(
            table="cross_db_test",
            data={"source": "secondary"},
            namespace=ns,
            database=secondary_db,
        )
        assert result["success"] is True

        # Verify it's in the secondary database
        select_result = await select.fn(
            table="cross_db_test",
            namespace=ns,
            database=secondary_db,
        )
        assert select_result["count"] == 1
        assert select_result["data"][0]["source"] == "secondary"

        # Verify it's NOT in the primary database (env var database)
        primary_result = await select.fn(table="cross_db_test")
        assert primary_result["count"] == 0

        # Clean up secondary database
        await query.fn(
            queries=["DELETE cross_db_test"],
            namespace=ns,
            database=secondary_db,
        )
