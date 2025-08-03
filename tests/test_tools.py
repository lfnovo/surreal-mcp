"""Comprehensive tests for all SurrealDB MCP Server tools."""

import pytest
from typing import Dict, Any


class TestQueryTool:
    """Test the query tool."""
    
    @pytest.mark.asyncio
    async def test_query_simple(self, clean_db):
        """Test simple query execution."""
        from surreal_mcp.server import query
        result = await query.fn(query_string="RETURN 1")
        assert result["success"] is True
        assert result["data"] == 1
    
    @pytest.mark.asyncio
    async def test_query_info_db(self, clean_db):
        """Test INFO FOR DB query."""
        from surreal_mcp.server import query
        result = await query.fn(query_string="INFO FOR DB")
        assert result["success"] is True
        assert isinstance(result["data"], dict)
    
    @pytest.mark.asyncio
    async def test_query_complex(self, clean_db, created_user):
        """Test complex query with WHERE clause."""
        from surreal_mcp.server import query
        result = await query.fn(
            query_string=f"SELECT * FROM user WHERE email = '{created_user['email']}'"
        )
        assert result["success"] is True
        assert len(result["data"]) == 1
        assert result["data"][0]["email"] == created_user["email"]
    
    @pytest.mark.asyncio
    async def test_query_invalid(self, clean_db):
        """Test invalid query handling."""
        from surreal_mcp.server import query
        with pytest.raises(Exception) as exc_info:
            await query.fn(query_string="INVALID QUERY SYNTAX")
        assert "SurrealDB query failed" in str(exc_info.value)


class TestSelectTool:
    """Test the select tool."""
    
    @pytest.mark.asyncio
    async def test_select_all_empty(self, clean_db):
        """Test selecting from empty table."""
        from surreal_mcp.server import select
        result = await select.fn(table="user")
        assert result["success"] is True
        assert result["data"] == []
        assert result["count"] == 0
    
    @pytest.mark.asyncio
    async def test_select_all_with_data(self, clean_db, created_user):
        """Test selecting all records."""
        from surreal_mcp.server import select
        result = await select.fn(table="user")
        assert result["success"] is True
        assert result["count"] == 1
        assert result["data"][0]["id"] == created_user["id"]
    
    @pytest.mark.asyncio
    async def test_select_by_id(self, clean_db, created_user):
        """Test selecting specific record by ID."""
        from surreal_mcp.server import select
        # Test with full ID
        result = await select.fn(table="user", id=created_user["id"])
        assert result["success"] is True
        assert result["count"] == 1
        assert result["data"][0]["email"] == created_user["email"]
        
        # Test with just the ID part
        id_part = created_user["id"].split(":")[1]
        result = await select.fn(table="user", id=id_part)
        assert result["success"] is True
        assert result["count"] == 1
    
    @pytest.mark.asyncio
    async def test_select_nonexistent(self, clean_db):
        """Test selecting non-existent record."""
        from surreal_mcp.server import select
        result = await select.fn(table="user", id="nonexistent")
        assert result["success"] is True
        assert result["data"] == []
        assert result["count"] == 0


class TestCreateTool:
    """Test the create tool."""
    
    @pytest.mark.asyncio
    async def test_create_simple(self, clean_db, sample_user_data):
        """Test creating a simple record."""
        from surreal_mcp.server import create
        result = await create.fn(table="user", data=sample_user_data)
        assert result["success"] is True
        assert "id" in result
        assert result["data"]["name"] == sample_user_data["name"]
        assert result["data"]["email"] == sample_user_data["email"]
        assert "created" in result["data"]
        assert "updated" in result["data"]
    
    @pytest.mark.asyncio
    async def test_create_with_nested_data(self, clean_db):
        """Test creating record with nested data."""
        from surreal_mcp.server import create
        data = {
            "name": "Complex User",
            "profile": {
                "bio": "Test bio",
                "social": {
                    "twitter": "@test",
                    "github": "testuser"
                }
            },
            "tags": ["developer", "tester"]
        }
        result = await create.fn(table="user", data=data)
        assert result["success"] is True
        assert result["data"]["profile"]["bio"] == "Test bio"
        assert result["data"]["tags"] == ["developer", "tester"]
    
    @pytest.mark.asyncio
    async def test_create_invalid_table(self, clean_db):
        """Test creating in invalid table."""
        from surreal_mcp.server import create
        with pytest.raises(Exception) as exc_info:
            await create.fn(table="", data={"test": "data"})
        assert "Failed to create record" in str(exc_info.value)


class TestUpdateTool:
    """Test the update tool."""
    
    @pytest.mark.asyncio
    async def test_update_full(self, clean_db, created_user):
        """Test full record update."""
        from surreal_mcp.server import update, select
        new_data = {
            "name": "Updated User",
            "email": "updated@example.com",
            "age": 30,
            "active": False
        }
        result = await update.fn(thing=created_user["id"], data=new_data)
        assert result["success"] is True
        assert result["data"]["name"] == new_data["name"]
        assert result["data"]["email"] == new_data["email"]
        assert result["data"]["age"] == new_data["age"]
        
        # Verify old fields are replaced
        select_result = await select.fn(table="user", id=created_user["id"])
        assert "active" in select_result["data"][0]
    
    @pytest.mark.asyncio
    async def test_update_invalid_id(self, clean_db):
        """Test updating with invalid ID format."""
        from surreal_mcp.server import update
        with pytest.raises(Exception) as exc_info:
            await update.fn(thing="invalid_format", data={"name": "Test"})
        assert "Invalid record ID format" in str(exc_info.value)


class TestDeleteTool:
    """Test the delete tool."""
    
    @pytest.mark.asyncio
    async def test_delete_existing(self, clean_db, created_user):
        """Test deleting existing record."""
        from surreal_mcp.server import delete, select
        result = await delete.fn(thing=created_user["id"])
        assert result["success"] is True
        assert result["deleted"] == created_user["id"]
        assert result["data"] is not None
        
        # Verify deletion
        select_result = await select.fn(table="user", id=created_user["id"])
        assert select_result["count"] == 0
    
    @pytest.mark.asyncio
    async def test_delete_nonexistent(self, clean_db):
        """Test deleting non-existent record."""
        from surreal_mcp.server import delete
        result = await delete.fn(thing="user:nonexistent")
        assert result["success"] is True
        assert result["deleted"] == "user:nonexistent"
        assert result["data"] is None


class TestMergeTool:
    """Test the merge tool."""
    
    @pytest.mark.asyncio
    async def test_merge_partial_update(self, clean_db, created_user):
        """Test partial update with merge."""
        from surreal_mcp.server import merge
        merge_data = {
            "verified": True,
            "last_login": "2024-01-01T10:00:00Z"
        }
        result = await merge.fn(thing=created_user["id"], data=merge_data)
        assert result["success"] is True
        assert result["data"]["verified"] is True
        assert result["data"]["last_login"] == merge_data["last_login"]
        assert result["data"]["name"] == created_user["name"]  # Original field preserved
        assert result["modified_fields"] == ["verified", "last_login"]
    
    @pytest.mark.asyncio
    async def test_merge_nested_update(self, clean_db, created_user):
        """Test merging nested data."""
        from surreal_mcp.server import merge
        merge_data = {
            "profile": {
                "bio": "New bio",
                "verified": True
            }
        }
        result = await merge.fn(thing=created_user["id"], data=merge_data)
        assert result["success"] is True
        assert result["data"]["profile"]["bio"] == "New bio"


class TestPatchTool:
    """Test the patch tool."""
    
    @pytest.mark.asyncio
    async def test_patch_operations(self, clean_db, created_user):
        """Test patch operations."""
        from surreal_mcp.server import patch
        patches = [
            {"op": "replace", "path": "/email", "value": "patched@example.com"},
            {"op": "add", "path": "/verified", "value": True}
        ]
        result = await patch.fn(thing=created_user["id"], patches=patches)
        assert result["success"] is True
        assert result["applied_patches"] == 2
        assert result["data"]["email"] == "patched@example.com"
        assert result["data"]["verified"] is True
    
    @pytest.mark.asyncio
    async def test_patch_invalid_format(self, clean_db, created_user):
        """Test patch with invalid format."""
        from surreal_mcp.server import patch
        with pytest.raises(Exception) as exc_info:
            await patch.fn(thing=created_user["id"], patches="not an array")
        assert "Patches must be a non-empty array" in str(exc_info.value)


class TestUpsertTool:
    """Test the upsert tool."""
    
    @pytest.mark.asyncio
    async def test_upsert_create(self, clean_db):
        """Test upsert creating new record."""
        from surreal_mcp.server import upsert
        data = {
            "name": "Upserted User",
            "email": "upsert@example.com"
        }
        result = await upsert.fn(thing="user:specific_id", data=data)
        assert result["success"] is True
        assert result["created"] is True
        assert result["data"]["id"] == "user:specific_id"
        assert result["data"]["name"] == data["name"]
    
    @pytest.mark.asyncio
    async def test_upsert_update(self, clean_db):
        """Test upsert updating existing record."""
        from surreal_mcp.server import upsert
        # First create
        result1 = await upsert.fn(
            thing="user:test_id",
            data={"name": "Original", "value": 1}
        )
        assert result1["created"] is True
        
        # Then update
        result2 = await upsert.fn(
            thing="user:test_id",
            data={"name": "Updated", "value": 2, "new_field": True}
        )
        assert result2["success"] is True
        assert result2["created"] is False
        assert result2["data"]["name"] == "Updated"
        assert result2["data"]["new_field"] is True


class TestInsertTool:
    """Test the insert tool."""
    
    @pytest.mark.asyncio
    async def test_insert_bulk(self, clean_db, sample_products_bulk):
        """Test bulk insert."""
        from surreal_mcp.server import insert
        result = await insert.fn(table="product", data=sample_products_bulk)
        assert result["success"] is True
        assert result["count"] == len(sample_products_bulk)
        assert len(result["data"]) == len(sample_products_bulk)
        
        # Verify all products have IDs and timestamps
        for product in result["data"]:
            assert "id" in product
            assert "created" in product
            assert "updated" in product
    
    @pytest.mark.asyncio
    async def test_insert_empty(self, clean_db):
        """Test insert with empty array."""
        from surreal_mcp.server import insert
        with pytest.raises(Exception) as exc_info:
            await insert.fn(table="product", data=[])
        assert "Data must be a non-empty array" in str(exc_info.value)


class TestRelateTool:
    """Test the relate tool."""
    
    @pytest.mark.asyncio
    async def test_relate_simple(self, clean_db, created_user, created_product):
        """Test creating simple relation."""
        from surreal_mcp.server import relate
        result = await relate.fn(
            from_thing=created_user["id"],
            relation_name="purchased",
            to_thing=created_product["id"]
        )
        assert result["success"] is True
        assert len(result["data"]) > 0
        assert "relation_id" in result
    
    @pytest.mark.asyncio
    async def test_relate_with_data(self, clean_db, created_user, created_product):
        """Test creating relation with data."""
        from surreal_mcp.server import relate
        relation_data = {
            "quantity": 2,
            "price": 199.98,
            "date": "2024-01-15"
        }
        result = await relate.fn(
            from_thing=created_user["id"],
            relation_name="purchased",
            to_thing=created_product["id"],
            data=relation_data
        )
        assert result["success"] is True
        assert result["data"][0]["quantity"] == 2
        assert result["data"][0]["price"] == 199.98
    
    @pytest.mark.asyncio
    async def test_relate_invalid_format(self, clean_db):
        """Test relate with invalid ID format."""
        from surreal_mcp.server import relate
        with pytest.raises(Exception) as exc_info:
            await relate.fn(
                from_thing="invalid",
                relation_name="likes",
                to_thing="also_invalid"
            )
        assert "Invalid source record ID format" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_relate_query_verification(self, clean_db, created_user, created_products):
        """Test querying relations after creation."""
        from surreal_mcp.server import relate, query
        # Create multiple relations
        for product in created_products[:3]:
            await relate.fn(
                from_thing=created_user["id"],
                relation_name="likes",
                to_thing=product["id"],
                data={"rating": 5}
            )
        
        # Query the relations
        result = await query.fn(
            query_string=f"SELECT * FROM {created_user['id']}->likes->product"
        )
        assert result["success"] is True
        assert len(result["data"]) == 3