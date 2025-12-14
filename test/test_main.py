"""Tests for main MCP server module."""

import pytest
from unittest.mock import patch
import asyncio
from mcp.server.fastmcp.exceptions import ToolError
from databricks_mcp.main import (
    execute_sql_query,
    describe_uc_table,
    describe_uc_catalog,
    describe_uc_schema,
    get_uc_table_history,
    list_uc_catalogs,
    format_exception_md,
    get_databricks_job,
    list_databricks_jobs,
    get_databricks_job_run,
    get_databricks_job_run_output,
    list_databricks_job_runs,
    list_databricks_workspaces,
    get_databricks_active_workspace,
    set_databricks_active_workspace,
    DatabricksSessionContext,
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

            with pytest.raises(ToolError) as exc_info:
                await execute_sql_query("INVALID SQL")

            assert "SQL Query Failed" in str(exc_info.value)
            assert "Syntax error" in str(exc_info.value)
            assert "Invalid SQL syntax" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_sql_error(self, setup_env_vars):
        """Test SQL query execution with error status."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.return_value = {
                "status": "error",
                "error": "Connection failed",
                "details": "Network timeout",
            }

            with pytest.raises(ToolError) as exc_info:
                await execute_sql_query("SELECT 1")

            assert "Error during SQL Execution" in str(exc_info.value)
            assert "Connection failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_sql_unexpected_status(self, setup_env_vars):
        """Test SQL query execution with unexpected status."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.return_value = {"status": "unknown"}

            with pytest.raises(ToolError) as exc_info:
                await execute_sql_query("SELECT 1")

            assert "unexpected status" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_sql_config_error(self, setup_env_vars):
        """Test SQL query execution with config error."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.side_effect = DatabricksConfigError("Config missing")

            with pytest.raises(ToolError) as exc_info:
                await execute_sql_query("SELECT 1")

            assert "Databricks not configured" in str(exc_info.value)
            assert "Config missing" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_sql_unexpected_exception(self, setup_env_vars):
        """Test SQL query execution with unexpected exception."""
        with patch("databricks_mcp.main.execute_databricks_sql") as mock_execute:
            mock_execute.side_effect = Exception("Unexpected error")

            with pytest.raises(ToolError) as exc_info:
                await execute_sql_query("SELECT 1")

            assert "Unexpected error" in str(exc_info.value)


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
                full_table_name="catalog.schema.table",
                include_lineage=False,
                workspace=None,
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
                full_table_name="catalog.schema.table",
                include_lineage=True,
                workspace=None,
            )

    @pytest.mark.asyncio
    async def test_describe_table_config_error(self, setup_env_vars):
        """Test describing table with config error."""
        with patch("databricks_mcp.main.get_uc_table_details") as mock_get_details:
            mock_get_details.side_effect = DatabricksConfigError("Missing credentials")

            with pytest.raises(ToolError) as exc_info:
                await describe_uc_table("catalog.schema.table")

            assert "Databricks not configured" in str(exc_info.value)
            assert "Missing credentials" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_describe_table_unexpected_error(self, setup_env_vars):
        """Test describing table with unexpected error."""
        with patch("databricks_mcp.main.get_uc_table_details") as mock_get_details:
            mock_get_details.side_effect = Exception("Table not found")

            with pytest.raises(ToolError) as exc_info:
                await describe_uc_table("catalog.schema.table")

            assert "Unexpected error" in str(exc_info.value)
            assert "Table not found" in str(exc_info.value)


class TestGetUcTableHistory:
    """Test cases for get_uc_table_history tool."""

    @pytest.mark.asyncio
    async def test_get_history_default_params(self, setup_env_vars):
        """Test getting table history with default parameters."""
        with patch("databricks_mcp.main.get_table_history") as mock_get_history:
            mock_get_history.return_value = "# Table History\n| Version | Timestamp |"

            result = await get_uc_table_history("catalog.schema.table")

            assert "Table History" in result
            mock_get_history.assert_called_once_with(
                table_full_name="catalog.schema.table",
                limit=10,
                start_timestamp=None,
                end_timestamp=None,
                workspace=None,
            )

    @pytest.mark.asyncio
    async def test_get_history_with_filters(self, setup_env_vars):
        """Test getting table history with date filters."""
        with patch("databricks_mcp.main.get_table_history") as mock_get_history:
            mock_get_history.return_value = "# Table History\n| Version | Timestamp |"

            result = await get_uc_table_history(
                "catalog.schema.table",
                limit=5,
                start_timestamp="2024-01-01",
                end_timestamp="2024-12-31",
            )

            assert "Table History" in result
            mock_get_history.assert_called_once_with(
                table_full_name="catalog.schema.table",
                limit=5,
                start_timestamp="2024-01-01",
                end_timestamp="2024-12-31",
                workspace=None,
            )

    @pytest.mark.asyncio
    async def test_get_history_config_error(self, setup_env_vars):
        """Test getting history with config error."""
        with patch("databricks_mcp.main.get_table_history") as mock_get_history:
            mock_get_history.side_effect = DatabricksConfigError("Missing credentials")

            with pytest.raises(ToolError) as exc_info:
                await get_uc_table_history("catalog.schema.table")

            assert "Databricks not configured" in str(exc_info.value)


class TestDescribeUcCatalog:
    """Test cases for describe_uc_catalog tool."""

    @pytest.mark.asyncio
    async def test_describe_catalog_success(self, setup_env_vars):
        """Test successful catalog description."""
        with patch("databricks_mcp.main.get_uc_catalog_details") as mock_get_details:
            mock_get_details.return_value = "# Catalog: test_catalog\nSchemas: schema1"

            result = await describe_uc_catalog("test_catalog")

            assert "test_catalog" in result
            mock_get_details.assert_called_once_with(catalog_name="test_catalog", workspace=None)

    @pytest.mark.asyncio
    async def test_describe_catalog_error(self, setup_env_vars):
        """Test catalog description with error."""
        with patch("databricks_mcp.main.get_uc_catalog_details") as mock_get_details:
            mock_get_details.side_effect = Exception("Catalog not accessible")

            with pytest.raises(ToolError) as exc_info:
                await describe_uc_catalog("invalid_catalog")

            assert "Unexpected error" in str(exc_info.value)


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
                catalog_name="catalog", schema_name="schema", include_columns=False, workspace=None
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
                catalog_name="catalog", schema_name="schema", include_columns=True, workspace=None
            )

    @pytest.mark.asyncio
    async def test_describe_schema_error(self, setup_env_vars):
        """Test describing schema with error."""
        with patch("databricks_mcp.main.get_uc_schema_details") as mock_get_details:
            mock_get_details.side_effect = Exception("Schema error")

            with pytest.raises(ToolError) as exc_info:
                await describe_uc_schema("catalog", "invalid")

            assert "Unexpected error" in str(exc_info.value)


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
            mock_get_summary.assert_called_once_with(workspace=None)

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

            with pytest.raises(ToolError) as exc_info:
                await list_uc_catalogs()

            assert "Databricks not configured" in str(exc_info.value)
            assert "Auth failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_list_catalogs_unexpected_error(self, setup_env_vars):
        """Test listing catalogs with unexpected error."""
        with patch(
            "databricks_mcp.main.get_uc_all_catalogs_summary"
        ) as mock_get_summary:
            mock_get_summary.side_effect = Exception("Connection timeout")

            with pytest.raises(ToolError) as exc_info:
                await list_uc_catalogs()

            assert "Unexpected error" in str(exc_info.value)
            assert "Connection timeout" in str(exc_info.value)


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


# ============================================================================
# Job-related MCP tool tests
# ============================================================================


class TestGetDatabricksJob:
    """Test cases for get_databricks_job tool."""

    @pytest.mark.asyncio
    async def test_get_job_success(self, setup_env_vars):
        """Test successful job retrieval."""
        with patch("databricks_mcp.main.get_job") as mock_get_job:
            mock_get_job.return_value = "# Job: **Test Job**\n**Job ID**: `12345`"

            result = await get_databricks_job(12345)

            assert "Test Job" in result
            assert "12345" in result
            mock_get_job.assert_called_once_with(job_id=12345, workspace=None)

    @pytest.mark.asyncio
    async def test_get_job_config_error(self, setup_env_vars):
        """Test job retrieval with config error."""
        with patch("databricks_mcp.main.get_job") as mock_get_job:
            mock_get_job.side_effect = DatabricksConfigError("Token missing")

            with pytest.raises(ToolError) as exc_info:
                await get_databricks_job(12345)

            assert "Databricks not configured" in str(exc_info.value)
            assert "Token missing" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_job_unexpected_error(self, setup_env_vars):
        """Test job retrieval with unexpected error."""
        with patch("databricks_mcp.main.get_job") as mock_get_job:
            mock_get_job.side_effect = Exception("API timeout")

            with pytest.raises(ToolError) as exc_info:
                await get_databricks_job(12345)

            assert "Unexpected error" in str(exc_info.value)
            assert "API timeout" in str(exc_info.value)


class TestListDatabricksJobs:
    """Test cases for list_databricks_jobs tool."""

    @pytest.mark.asyncio
    async def test_list_jobs_success(self, setup_env_vars):
        """Test successful job listing."""
        with patch("databricks_mcp.main.list_jobs") as mock_list:
            mock_list.return_value = "# Jobs List\n## `Job1` (ID: 1)\n## `Job2` (ID: 2)"

            result = await list_databricks_jobs()

            assert "Job1" in result
            assert "Job2" in result
            mock_list.assert_called_once_with(name=None, expand_tasks=False, workspace=None)

    @pytest.mark.asyncio
    async def test_list_jobs_with_filter(self, setup_env_vars):
        """Test job listing with name filter."""
        with patch("databricks_mcp.main.list_jobs") as mock_list:
            mock_list.return_value = "# Jobs List\n**Filter**: name contains `ETL`"

            result = await list_databricks_jobs(name="ETL", expand_tasks=True)

            assert "ETL" in result
            mock_list.assert_called_once_with(name="ETL", expand_tasks=True, workspace=None)

    @pytest.mark.asyncio
    async def test_list_jobs_empty(self, setup_env_vars):
        """Test job listing with no results."""
        with patch("databricks_mcp.main.list_jobs") as mock_list:
            mock_list.return_value = "# Jobs List\n*No jobs found.*"

            result = await list_databricks_jobs()

            assert "No jobs found" in result

    @pytest.mark.asyncio
    async def test_list_jobs_error(self, setup_env_vars):
        """Test job listing with error."""
        with patch("databricks_mcp.main.list_jobs") as mock_list:
            mock_list.side_effect = Exception("Connection refused")

            with pytest.raises(ToolError) as exc_info:
                await list_databricks_jobs()

            assert "Unexpected error" in str(exc_info.value)
            assert "Connection refused" in str(exc_info.value)


class TestGetDatabricksJobRun:
    """Test cases for get_databricks_job_run tool."""

    @pytest.mark.asyncio
    async def test_get_run_success(self, setup_env_vars):
        """Test successful run retrieval."""
        with patch("databricks_mcp.main.get_job_run") as mock_get_run:
            mock_get_run.return_value = (
                "# Run: **Test Run**\n**Run ID**: `54321`\n**Job ID**: `12345`"
            )

            result = await get_databricks_job_run(54321)

            assert "Test Run" in result
            assert "54321" in result
            mock_get_run.assert_called_once_with(run_id=54321, workspace=None)

    @pytest.mark.asyncio
    async def test_get_run_config_error(self, setup_env_vars):
        """Test run retrieval with config error."""
        with patch("databricks_mcp.main.get_job_run") as mock_get_run:
            mock_get_run.side_effect = DatabricksConfigError("Auth expired")

            with pytest.raises(ToolError) as exc_info:
                await get_databricks_job_run(54321)

            assert "Databricks not configured" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_run_unexpected_error(self, setup_env_vars):
        """Test run retrieval with unexpected error."""
        with patch("databricks_mcp.main.get_job_run") as mock_get_run:
            mock_get_run.side_effect = Exception("Run not found")

            with pytest.raises(ToolError) as exc_info:
                await get_databricks_job_run(99999)

            assert "Unexpected error" in str(exc_info.value)
            assert "Run not found" in str(exc_info.value)


class TestGetDatabricksJobRunOutput:
    """Test cases for get_databricks_job_run_output tool."""

    @pytest.mark.asyncio
    async def test_get_run_output_success(self, setup_env_vars):
        """Test successful run output retrieval."""
        with patch("databricks_mcp.main.get_job_run_output") as mock_get_output:
            mock_get_output.return_value = (
                "# Run Output (Run ID: 54321)\n## Notebook Output\n```\nResult: OK\n```"
            )

            result = await get_databricks_job_run_output(54321)

            assert "54321" in result
            assert "Notebook Output" in result
            mock_get_output.assert_called_once_with(run_id=54321, workspace=None)

    @pytest.mark.asyncio
    async def test_get_run_output_with_logs(self, setup_env_vars):
        """Test run output with logs."""
        with patch("databricks_mcp.main.get_job_run_output") as mock_get_output:
            mock_get_output.return_value = "# Run Output\n## Logs\n```\nLog line 1\n```"

            result = await get_databricks_job_run_output(54321)

            assert "Logs" in result

    @pytest.mark.asyncio
    async def test_get_run_output_error(self, setup_env_vars):
        """Test run output with error."""
        with patch("databricks_mcp.main.get_job_run_output") as mock_get_output:
            mock_get_output.side_effect = Exception("Output unavailable")

            with pytest.raises(ToolError) as exc_info:
                await get_databricks_job_run_output(54321)

            assert "Unexpected error" in str(exc_info.value)
            assert "Output unavailable" in str(exc_info.value)


class TestListDatabricksJobRuns:
    """Test cases for list_databricks_job_runs tool."""

    @pytest.mark.asyncio
    async def test_list_runs_success(self, setup_env_vars):
        """Test successful run listing."""
        with patch("databricks_mcp.main.list_job_runs") as mock_list:
            mock_list.return_value = "# Job Runs\n## Run 1\n- **Run ID**: `1`"

            result = await list_databricks_job_runs(job_id=12345)

            assert "Job Runs" in result
            mock_list.assert_called_once_with(
                job_id=12345,
                active_only=False,
                completed_only=False,
                expand_tasks=False,
                start_time_from=None,
                start_time_to=None,
                max_results=25,
                workspace=None,
            )

    @pytest.mark.asyncio
    async def test_list_runs_active_only(self, setup_env_vars):
        """Test listing active runs only."""
        with patch("databricks_mcp.main.list_job_runs") as mock_list:
            mock_list.return_value = "# Job Runs\n**Filters**: active_only=true"

            result = await list_databricks_job_runs(active_only=True)

            assert "active_only" in result
            mock_list.assert_called_once()
            call_kwargs = mock_list.call_args[1]
            assert call_kwargs["active_only"] is True

    @pytest.mark.asyncio
    async def test_list_runs_with_time_filter(self, setup_env_vars):
        """Test listing runs with time filters."""
        with patch("databricks_mcp.main.list_job_runs") as mock_list:
            mock_list.return_value = "# Job Runs\n**Filters**: start_time_from=..."

            await list_databricks_job_runs(
                start_time_from=1704067200000, start_time_to=1704153600000
            )

            mock_list.assert_called_once()
            call_kwargs = mock_list.call_args[1]
            assert call_kwargs["start_time_from"] == 1704067200000
            assert call_kwargs["start_time_to"] == 1704153600000

    @pytest.mark.asyncio
    async def test_list_runs_with_limit(self, setup_env_vars):
        """Test listing runs with custom limit."""
        with patch("databricks_mcp.main.list_job_runs") as mock_list:
            mock_list.return_value = "# Job Runs\n## Run 1"

            await list_databricks_job_runs(job_id=12345, limit=10)

            mock_list.assert_called_once()
            call_kwargs = mock_list.call_args[1]
            assert call_kwargs["max_results"] == 10

    @pytest.mark.asyncio
    async def test_list_runs_empty(self, setup_env_vars):
        """Test listing runs with no results."""
        with patch("databricks_mcp.main.list_job_runs") as mock_list:
            mock_list.return_value = "# Job Runs\n*No runs found.*"

            result = await list_databricks_job_runs(job_id=99999)

            assert "No runs found" in result

    @pytest.mark.asyncio
    async def test_list_runs_error(self, setup_env_vars):
        """Test listing runs with error."""
        with patch("databricks_mcp.main.list_job_runs") as mock_list:
            mock_list.side_effect = Exception("Database error")

            with pytest.raises(ToolError) as exc_info:
                await list_databricks_job_runs()

            assert "Unexpected error" in str(exc_info.value)
            assert "Database error" in str(exc_info.value)


class TestJobToolsIntegration:
    """Integration tests for job-related tools working together."""

    @pytest.mark.asyncio
    async def test_job_to_runs_flow(self, setup_env_vars):
        """Test the flow of listing jobs -> getting job -> listing runs."""
        with (
            patch("databricks_mcp.main.list_jobs") as mock_list_jobs,
            patch("databricks_mcp.main.get_job") as mock_get_job,
            patch("databricks_mcp.main.list_job_runs") as mock_list_runs,
            patch("databricks_mcp.main.get_job_run") as mock_get_run,
        ):
            mock_list_jobs.return_value = "# Jobs\n## `ETL Job` (ID: 12345)"
            mock_get_job.return_value = (
                "# Job: **ETL Job**\n## Tasks\n### Task: `task1`"
            )
            mock_list_runs.return_value = (
                "# Job Runs\n## Run 54321\n- **Status**: TERMINATED"
            )
            mock_get_run.return_value = (
                "# Run: **Run 54321**\n## State\n**Status**: SUCCESS"
            )

            # List jobs
            jobs = await list_databricks_jobs()
            assert "ETL Job" in jobs

            # Get specific job
            job_details = await get_databricks_job(12345)
            assert "task1" in job_details

            # List runs for job
            runs = await list_databricks_job_runs(job_id=12345)
            assert "54321" in runs

            # Get specific run
            run_details = await get_databricks_job_run(54321)
            assert "SUCCESS" in run_details

    @pytest.mark.asyncio
    async def test_concurrent_job_queries(self, setup_env_vars):
        """Test multiple concurrent job queries."""
        with patch("databricks_mcp.main.get_job") as mock_get_job:
            mock_get_job.return_value = "# Job: **Test**"

            queries = [
                get_databricks_job(1),
                get_databricks_job(2),
                get_databricks_job(3),
            ]

            results = await asyncio.gather(*queries)

            assert len(results) == 3
            assert mock_get_job.call_count == 3


class TestListDatabricksWorkspaces:
    """Test cases for list_databricks_workspaces tool."""

    @pytest.mark.asyncio
    async def test_list_workspaces_success(self, setup_env_vars):
        """Test listing workspaces with configured workspaces."""
        from databricks_mcp.databricks_sdk_utils import WorkspaceConfig

        with patch("databricks_mcp.main.get_workspace_configs") as mock_get:
            mock_get.return_value = {
                "default": WorkspaceConfig(
                    name="default",
                    host="https://default.databricks.com",
                    token="token1",
                    sql_warehouse_id="wh1",
                ),
                "prod": WorkspaceConfig(
                    name="prod",
                    host="https://prod.databricks.com",
                    token="token2",
                    sql_warehouse_id=None,
                ),
            }

            result = await list_databricks_workspaces()

            assert "# Configured Workspaces" in result
            assert "`default`" in result
            assert "`prod`" in result
            assert "https://default.databricks.com" in result
            assert "configured" in result

    @pytest.mark.asyncio
    async def test_list_workspaces_empty(self, setup_env_vars):
        """Test listing workspaces when none configured."""
        with patch("databricks_mcp.main.get_workspace_configs") as mock_get:
            mock_get.return_value = {}

            result = await list_databricks_workspaces()

            assert "No workspaces configured" in result


class TestGetDatabricksActiveWorkspace:
    """Test cases for get_databricks_active_workspace tool."""

    @pytest.mark.asyncio
    async def test_get_active_workspace_set(self, setup_env_vars):
        """Test getting active workspace when set."""
        mock_ctx = type("MockContext", (), {})()
        mock_ctx.request_context = type("MockRequestContext", (), {})()
        mock_ctx.request_context.lifespan_context = DatabricksSessionContext(
            active_workspace="prod"
        )

        result = await get_databricks_active_workspace(ctx=mock_ctx)

        assert "# Active Workspace" in result
        assert "`prod`" in result

    @pytest.mark.asyncio
    async def test_get_active_workspace_not_set(self, setup_env_vars):
        """Test getting active workspace when not set."""
        mock_ctx = type("MockContext", (), {})()
        mock_ctx.request_context = type("MockRequestContext", (), {})()
        mock_ctx.request_context.lifespan_context = DatabricksSessionContext()

        result = await get_databricks_active_workspace(ctx=mock_ctx)

        assert "No active workspace set" in result

    @pytest.mark.asyncio
    async def test_get_active_workspace_no_context(self, setup_env_vars):
        """Test getting active workspace with no context."""
        result = await get_databricks_active_workspace(ctx=None)

        assert "No active workspace set" in result


class TestSetDatabricksActiveWorkspace:
    """Test cases for set_databricks_active_workspace tool."""

    @pytest.mark.asyncio
    async def test_set_active_workspace_success(self, setup_env_vars):
        """Test setting active workspace successfully."""
        from databricks_mcp.databricks_sdk_utils import WorkspaceConfig

        mock_ctx = type("MockContext", (), {})()
        mock_ctx.request_context = type("MockRequestContext", (), {})()
        session_ctx = DatabricksSessionContext()
        mock_ctx.request_context.lifespan_context = session_ctx

        with patch("databricks_mcp.main.get_workspace_configs") as mock_get:
            mock_get.return_value = {
                "prod": WorkspaceConfig(
                    name="prod",
                    host="https://prod.databricks.com",
                    token="token",
                ),
            }

            result = await set_databricks_active_workspace("prod", ctx=mock_ctx)

            assert "# Active Workspace Updated" in result
            assert "`prod`" in result
            assert session_ctx.active_workspace == "prod"

    @pytest.mark.asyncio
    async def test_set_active_workspace_not_found(self, setup_env_vars):
        """Test setting workspace that doesn't exist."""
        from databricks_mcp.databricks_sdk_utils import WorkspaceConfig

        mock_ctx = type("MockContext", (), {})()
        mock_ctx.request_context = type("MockRequestContext", (), {})()
        mock_ctx.request_context.lifespan_context = DatabricksSessionContext()

        with patch("databricks_mcp.main.get_workspace_configs") as mock_get:
            mock_get.return_value = {
                "prod": WorkspaceConfig(
                    name="prod",
                    host="https://prod.databricks.com",
                    token="token",
                ),
            }

            with pytest.raises(ToolError) as exc_info:
                await set_databricks_active_workspace("nonexistent", ctx=mock_ctx)

            assert "not found" in str(exc_info.value)
            assert "prod" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_set_active_workspace_no_context(self, setup_env_vars):
        """Test setting workspace with no context."""
        from databricks_mcp.databricks_sdk_utils import WorkspaceConfig

        with patch("databricks_mcp.main.get_workspace_configs") as mock_get:
            mock_get.return_value = {
                "prod": WorkspaceConfig(
                    name="prod",
                    host="https://prod.databricks.com",
                    token="token",
                ),
            }

            with pytest.raises(ToolError) as exc_info:
                await set_databricks_active_workspace("prod", ctx=None)

            assert "context not available" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_set_active_workspace_case_insensitive(self, setup_env_vars):
        """Test setting workspace with different case."""
        from databricks_mcp.databricks_sdk_utils import WorkspaceConfig

        mock_ctx = type("MockContext", (), {})()
        mock_ctx.request_context = type("MockRequestContext", (), {})()
        session_ctx = DatabricksSessionContext()
        mock_ctx.request_context.lifespan_context = session_ctx

        with patch("databricks_mcp.main.get_workspace_configs") as mock_get:
            mock_get.return_value = {
                "prod": WorkspaceConfig(
                    name="prod",
                    host="https://prod.databricks.com",
                    token="token",
                ),
            }

            result = await set_databricks_active_workspace("PROD", ctx=mock_ctx)

            assert "`prod`" in result
            assert session_ctx.active_workspace == "prod"


class TestDatabricksSessionContextIsolation:
    """Test cases for per-session workspace state isolation."""

    def test_session_context_isolation(self):
        """Test that separate DatabricksSessionContext instances are isolated."""
        session1 = DatabricksSessionContext()
        session2 = DatabricksSessionContext()

        session1.active_workspace = "dev"
        session2.active_workspace = "prod"

        assert session1.active_workspace == "dev"
        assert session2.active_workspace == "prod"

    def test_session_context_default_is_none(self):
        """Test that new session context has no active workspace set."""
        session = DatabricksSessionContext()
        assert session.active_workspace is None

    @pytest.mark.asyncio
    async def test_set_workspace_only_affects_calling_session(self, setup_env_vars):
        """Test that set_active_databricks_workspace only affects the calling session."""
        from databricks_mcp.databricks_sdk_utils import WorkspaceConfig

        session1 = DatabricksSessionContext()
        session2 = DatabricksSessionContext()

        mock_ctx1 = type("MockContext", (), {})()
        mock_ctx1.request_context = type("MockRequestContext", (), {})()
        mock_ctx1.request_context.lifespan_context = session1

        mock_ctx2 = type("MockContext", (), {})()
        mock_ctx2.request_context = type("MockRequestContext", (), {})()
        mock_ctx2.request_context.lifespan_context = session2

        with patch("databricks_mcp.main.get_workspace_configs") as mock_get:
            mock_get.return_value = {
                "dev": WorkspaceConfig(name="dev", host="https://dev.databricks.com", token="token"),
                "prod": WorkspaceConfig(name="prod", host="https://prod.databricks.com", token="token"),
            }

            await set_databricks_active_workspace("dev", ctx=mock_ctx1)
            await set_databricks_active_workspace("prod", ctx=mock_ctx2)

            assert session1.active_workspace == "dev"
            assert session2.active_workspace == "prod"
