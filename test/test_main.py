"""Tests for main MCP server module."""

import pytest
from unittest.mock import patch
import asyncio
from databricks_mcp.main import (
    execute_sql_query,
    describe_uc_table,
    describe_uc_catalog,
    describe_uc_schema,
    list_uc_catalogs,
    format_exception_md,
)
from databricks_mcp.databricks_sdk_utils import DatabricksConfigError


class TestFormatExceptionMd:
    """Test cases for format_exception_md function."""

    def test_format_exception(self):
        """Test formatting exception into markdown."""
        result = format_exception_md("Test Error", "This is a test error message")

        assert "# Test Error" in result
        assert "**Details:**" in result
        assert "This is a test error message" in result

    def test_format_exception_multiline(self):
        """Test formatting exception with multiline details."""
        details = "Line 1\nLine 2\nLine 3"
        result = format_exception_md("Multiline Error", details)

        assert "# Multiline Error" in result
        assert "Line 1" in result
        assert "Line 2" in result
        assert "Line 3" in result


class TestExecuteSqlQuery:
    """Test cases for execute_sql_query tool."""

    @pytest.mark.asyncio
    async def test_execute_sql_success(self, setup_env_vars, mock_sql_success_result):
        """Test successful SQL query execution."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.return_value = mock_sql_success_result

            result = await execute_sql_query("SELECT * FROM table")

            assert "Alice" in result
            assert "Bob" in result
            mock_execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_sql_failed(self, setup_env_vars):
        """Test SQL query execution with failed status."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.return_value = {
                "status": "failed",
                "error": "Syntax error",
                "details": "Invalid SQL syntax",
            }

            result = await execute_sql_query("INVALID SQL")

            assert "SQL Query Failed" in result
            assert "Syntax error" in result
            assert "Invalid SQL syntax" in result

    @pytest.mark.asyncio
    async def test_execute_sql_error(self, setup_env_vars):
        """Test SQL query execution with error status."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.return_value = {
                "status": "error",
                "error": "Connection failed",
                "details": "Network timeout",
            }

            result = await execute_sql_query("SELECT 1")

            assert "Error during SQL Execution" in result
            assert "Connection failed" in result

    @pytest.mark.asyncio
    async def test_execute_sql_unexpected_status(self, setup_env_vars):
        """Test SQL query execution with unexpected status."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.return_value = {"status": "unknown"}

            result = await execute_sql_query("SELECT 1")

            assert "unexpected status" in result

    @pytest.mark.asyncio
    async def test_execute_sql_config_error(self, setup_env_vars):
        """Test SQL query execution with config error."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.side_effect = DatabricksConfigError("Config missing")

            result = await execute_sql_query("SELECT 1")

            assert "Databricks not configured" in result
            assert "Config missing" in result

    @pytest.mark.asyncio
    async def test_execute_sql_unexpected_exception(self, setup_env_vars):
        """Test SQL query execution with unexpected exception."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.side_effect = Exception("Unexpected error")

            result = await execute_sql_query("SELECT 1")

            assert "Unexpected error" in result


class TestDescribeUcTable:
    """Test cases for describe_uc_table tool."""

    @pytest.mark.asyncio
    async def test_describe_table_without_lineage(self, setup_env_vars):
        """Test describing table without lineage."""
        with patch("databricks_mcp.main.get_uc_table_details") as mock_get_details:
            mock_get_details.return_value = "# Table: test_table\nDescription: Test"

            result = await describe_uc_table(
                "catalog.schema.table", include_lineage=False
            )

            assert "test_table" in result
            mock_get_details.assert_called_once_with(
                full_table_name="catalog.schema.table", include_lineage=False
            )

    @pytest.mark.asyncio
    async def test_describe_table_with_lineage(self, setup_env_vars):
        """Test describing table with lineage."""
        with patch("databricks_mcp.main.get_uc_table_details") as mock_get_details:
            mock_get_details.return_value = "# Table: test_table\nLineage: upstream"

            result = await describe_uc_table(
                "catalog.schema.table", include_lineage=True
            )

            assert "test_table" in result
            mock_get_details.assert_called_once_with(
                full_table_name="catalog.schema.table", include_lineage=True
            )

    @pytest.mark.asyncio
    async def test_describe_table_config_error(self, setup_env_vars):
        """Test describing table with config error."""
        with patch("databricks_mcp.main.get_uc_table_details") as mock_get_details:
            mock_get_details.side_effect = DatabricksConfigError("Missing credentials")

            result = await describe_uc_table("catalog.schema.table")

            assert "Databricks not configured" in result
            assert "Missing credentials" in result

    @pytest.mark.asyncio
    async def test_describe_table_unexpected_error(self, setup_env_vars):
        """Test describing table with unexpected error."""
        with patch("databricks_mcp.main.get_uc_table_details") as mock_get_details:
            mock_get_details.side_effect = Exception("Table not found")

            result = await describe_uc_table("catalog.schema.table")

            assert "Unexpected error" in result
            assert "Table not found" in result


class TestDescribeUcCatalog:
    """Test cases for describe_uc_catalog tool."""

    @pytest.mark.asyncio
    async def test_describe_catalog_success(self, setup_env_vars):
        """Test successful catalog description."""
        with patch("databricks_mcp.main.get_uc_catalog_details") as mock_get_details:
            mock_get_details.return_value = "# Catalog: test_catalog\nSchemas: schema1"

            result = await describe_uc_catalog("test_catalog")

            assert "test_catalog" in result
            mock_get_details.assert_called_once_with(catalog_name="test_catalog")

    @pytest.mark.asyncio
    async def test_describe_catalog_error(self, setup_env_vars):
        """Test catalog description with error."""
        with patch("databricks_mcp.main.get_uc_catalog_details") as mock_get_details:
            mock_get_details.side_effect = Exception("Catalog not accessible")

            result = await describe_uc_catalog("invalid_catalog")

            assert "Unexpected error" in result
            assert "Catalog not accessible" in result


class TestDescribeUcSchema:
    """Test cases for describe_uc_schema tool."""

    @pytest.mark.asyncio
    async def test_describe_schema_without_columns(self, setup_env_vars):
        """Test describing schema without columns."""
        with patch("databricks_mcp.main.get_uc_schema_details") as mock_get_details:
            mock_get_details.return_value = "# Schema: test_schema\nTables: table1"

            result = await describe_uc_schema(
                "catalog", "schema", include_columns=False
            )

            assert "test_schema" in result
            mock_get_details.assert_called_once_with(
                catalog_name="catalog", schema_name="schema", include_columns=False
            )

    @pytest.mark.asyncio
    async def test_describe_schema_with_columns(self, setup_env_vars):
        """Test describing schema with columns."""
        with patch("databricks_mcp.main.get_uc_schema_details") as mock_get_details:
            mock_get_details.return_value = "# Schema: test_schema\nColumns: id, name"

            result = await describe_uc_schema("catalog", "schema", include_columns=True)

            assert "test_schema" in result
            assert "Columns" in result
            mock_get_details.assert_called_once_with(
                catalog_name="catalog", schema_name="schema", include_columns=True
            )

    @pytest.mark.asyncio
    async def test_describe_schema_error(self, setup_env_vars):
        """Test describing schema with error."""
        with patch("databricks_mcp.main.get_uc_schema_details") as mock_get_details:
            mock_get_details.side_effect = Exception("Schema error")

            result = await describe_uc_schema("catalog", "invalid")

            assert "Unexpected error" in result


class TestListUcCatalogs:
    """Test cases for list_uc_catalogs tool."""

    @pytest.mark.asyncio
    async def test_list_catalogs_success(self, setup_env_vars):
        """Test successful catalog listing."""
        with patch(
            "databricks_mcp.main.get_uc_all_catalogs_summary"
        ) as mock_get_summary:
            mock_get_summary.return_value = "# Catalogs\n- catalog1\n- catalog2"

            result = await list_uc_catalogs()

            assert "catalog1" in result
            assert "catalog2" in result
            mock_get_summary.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_catalogs_empty(self, setup_env_vars):
        """Test listing catalogs when none exist."""
        with patch(
            "databricks_mcp.main.get_uc_all_catalogs_summary"
        ) as mock_get_summary:
            mock_get_summary.return_value = "# Catalogs\nNo catalogs found"

            result = await list_uc_catalogs()

            assert "No catalogs found" in result

    @pytest.mark.asyncio
    async def test_list_catalogs_config_error(self, setup_env_vars):
        """Test listing catalogs with config error."""
        with patch(
            "databricks_mcp.main.get_uc_all_catalogs_summary"
        ) as mock_get_summary:
            mock_get_summary.side_effect = DatabricksConfigError("Auth failed")

            result = await list_uc_catalogs()

            assert "Databricks not configured" in result
            assert "Auth failed" in result

    @pytest.mark.asyncio
    async def test_list_catalogs_unexpected_error(self, setup_env_vars):
        """Test listing catalogs with unexpected error."""
        with patch(
            "databricks_mcp.main.get_uc_all_catalogs_summary"
        ) as mock_get_summary:
            mock_get_summary.side_effect = Exception("Connection timeout")

            result = await list_uc_catalogs()

            assert "Unexpected error" in result
            assert "Connection timeout" in result


class TestIntegration:
    """Integration tests for multiple tools working together."""

    @pytest.mark.asyncio
    async def test_catalog_schema_table_flow(self, setup_env_vars):
        """Test the flow of listing catalog -> schema -> table."""
        with (
            patch("databricks_mcp.main.get_uc_all_catalogs_summary") as mock_catalogs,
            patch("databricks_mcp.main.get_uc_catalog_details") as mock_catalog,
            patch("databricks_mcp.main.get_uc_schema_details") as mock_schema,
            patch("databricks_mcp.main.get_uc_table_details") as mock_table,
        ):
            mock_catalogs.return_value = "# Catalogs\n- test_catalog"
            mock_catalog.return_value = "# Catalog: test_catalog\n- test_schema"
            mock_schema.return_value = "# Schema: test_schema\n- test_table"
            mock_table.return_value = "# Table: test_table"

            # List catalogs
            catalogs = await list_uc_catalogs()
            assert "test_catalog" in catalogs

            # Describe catalog
            catalog_details = await describe_uc_catalog("test_catalog")
            assert "test_schema" in catalog_details

            # Describe schema
            schema_details = await describe_uc_schema("test_catalog", "test_schema")
            assert "test_table" in schema_details

            # Describe table
            table_details = await describe_uc_table(
                "test_catalog.test_schema.test_table"
            )
            assert "test_table" in table_details

    @pytest.mark.asyncio
    async def test_concurrent_queries(self, setup_env_vars, mock_sql_success_result):
        """Test multiple concurrent SQL queries."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.return_value = mock_sql_success_result

            # Execute multiple queries concurrently
            queries = [
                execute_sql_query("SELECT 1"),
                execute_sql_query("SELECT 2"),
                execute_sql_query("SELECT 3"),
            ]

            results = await asyncio.gather(*queries)

            assert len(results) == 3
            assert mock_execute.call_count == 3
