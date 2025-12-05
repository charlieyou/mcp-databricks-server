"""Tests for databricks_sdk_utils module."""

from unittest.mock import Mock, patch

import pytest
from databricks.sdk.service.catalog import ColumnInfo as CatalogColumnInfo
from databricks.sdk.service.sql import StatementState

from databricks_mcp import databricks_sdk_utils
from databricks_mcp.databricks_sdk_utils import (
    DatabricksConfigError,
    _format_column_details_md,
    _format_run_state_md,
    _format_timestamp,
    _process_lineage_results,
    clear_lineage_cache,
    execute_databricks_sql,
    get_job,
    get_job_run,
    get_job_run_output,
    get_sdk_client,
    get_uc_all_catalogs_summary,
    get_uc_catalog_details,
    get_uc_schema_details,
    get_uc_table_details,
    list_job_runs,
    list_jobs,
)


class TestGetSdkClient:
    """Test cases for get_sdk_client function."""

    def test_get_sdk_client_success(self, setup_env_vars, monkeypatch):
        """Test successful SDK client initialization."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.WorkspaceClient"
        ) as mock_client:
            mock_instance = Mock()
            mock_client.return_value = mock_instance

            client = get_sdk_client()
            assert client is not None
            mock_client.assert_called_once()

    def test_get_sdk_client_caching(self, setup_env_vars, monkeypatch):
        """Test that SDK client is cached after first initialization."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.WorkspaceClient"
        ) as mock_client:
            mock_instance = Mock()
            mock_client.return_value = mock_instance

            client1 = get_sdk_client()
            client2 = get_sdk_client()

            # Should only be called once due to caching
            assert mock_client.call_count == 1
            assert client1 is client2

    def test_get_sdk_client_missing_host(self, monkeypatch):
        """Test that missing DATABRICKS_HOST raises error."""
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")

        with pytest.raises(DatabricksConfigError) as exc_info:
            get_sdk_client()

        assert "DATABRICKS_HOST" in str(exc_info.value)

    def test_get_sdk_client_missing_token(self, monkeypatch):
        """Test that missing DATABRICKS_TOKEN raises error."""
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

        with pytest.raises(DatabricksConfigError) as exc_info:
            get_sdk_client()

        assert "DATABRICKS_TOKEN" in str(exc_info.value)


class TestExecuteDatabricksSql:
    """Test cases for execute_databricks_sql function."""

    def test_execute_sql_success(self, setup_env_vars, mock_statement_response):
        """Test successful SQL execution."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.statement_execution.execute_statement.return_value = (
                mock_statement_response
            )
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT * FROM table")

            assert result["status"] == "success"
            assert result["row_count"] == 2
            assert len(result["data"]) == 2
            assert result["data"][0]["id"] == "1"

    def test_execute_sql_no_warehouse_id(self):
        """Test SQL execution without warehouse ID."""
        # Temporarily save and clear the warehouse ID
        original_warehouse_id = databricks_sdk_utils.DATABRICKS_SQL_WAREHOUSE_ID
        databricks_sdk_utils.DATABRICKS_SQL_WAREHOUSE_ID = None

        try:
            result = databricks_sdk_utils.execute_databricks_sql("SELECT 1")

            assert result["status"] == "error"
            assert "DATABRICKS_SQL_WAREHOUSE_ID" in result["error"]
        finally:
            # Restore original value
            databricks_sdk_utils.DATABRICKS_SQL_WAREHOUSE_ID = original_warehouse_id

    def test_execute_sql_failed_state(self, setup_env_vars):
        """Test SQL execution with failed state."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.FAILED
            mock_error = Mock()
            mock_error.message = "Syntax error in SQL"
            mock_status.error = mock_error
            mock_response.status = mock_status
            mock_client.statement_execution.execute_statement.return_value = (
                mock_response
            )
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("INVALID SQL")

            assert result["status"] == "failed"
            assert "Syntax error" in result["details"]

    def test_execute_sql_no_data(self, setup_env_vars):
        """Test SQL execution that returns no data."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.SUCCEEDED
            mock_status.error = None
            mock_response.status = mock_status
            mock_response.result = None
            mock_client.statement_execution.execute_statement.return_value = (
                mock_response
            )
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("CREATE TABLE test (id INT)")

            assert result["status"] == "success"
            assert result["row_count"] == 0
            assert "no data" in result["message"]

    def test_execute_sql_exception(self, setup_env_vars):
        """Test SQL execution with exception."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.statement_execution.execute_statement.side_effect = Exception(
                "Connection error"
            )
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT 1")

            assert result["status"] == "error"
            assert "Connection error" in result["error"]


class TestGetUcAllCatalogsSummary:
    """Test cases for get_uc_all_catalogs_summary function."""

    def test_list_catalogs_success(self, setup_env_vars, mock_catalog_info):
        """Test successful listing of catalogs."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.catalogs.list.return_value = [mock_catalog_info]
            mock_get_client.return_value = mock_client

            result = get_uc_all_catalogs_summary()

            assert "test_catalog" in result
            assert "Test catalog description" in result
            assert "MANAGED_CATALOG" in result

    def test_list_catalogs_empty(self, setup_env_vars):
        """Test listing catalogs when none exist."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.catalogs.list.return_value = []
            mock_get_client.return_value = mock_client

            result = get_uc_all_catalogs_summary()

            assert "No catalogs found" in result

    def test_list_catalogs_error(self, setup_env_vars):
        """Test listing catalogs with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.catalogs.list.side_effect = Exception("Permission denied")
            mock_get_client.return_value = mock_client

            result = get_uc_all_catalogs_summary()

            assert "Error" in result
            assert "Permission denied" in result


class TestGetUcCatalogDetails:
    """Test cases for get_uc_catalog_details function."""

    def test_get_catalog_details_success(self, setup_env_vars, mock_schema_info):
        """Test successful retrieval of catalog details."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.schemas.list.return_value = [mock_schema_info]
            mock_get_client.return_value = mock_client

            result = get_uc_catalog_details("test_catalog")

            assert "test_catalog" in result
            assert "test_schema" in result
            assert "Test schema description" in result

    def test_get_catalog_details_no_schemas(self, setup_env_vars):
        """Test getting catalog details with no schemas."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.schemas.list.return_value = []
            mock_get_client.return_value = mock_client

            result = get_uc_catalog_details("empty_catalog")

            assert "No schemas found" in result

    def test_get_catalog_details_error(self, setup_env_vars):
        """Test getting catalog details with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.schemas.list.side_effect = Exception("Catalog not found")
            mock_get_client.return_value = mock_client

            result = get_uc_catalog_details("invalid_catalog")

            assert "Error" in result
            assert "Catalog not found" in result


class TestGetUcSchemaDetails:
    """Test cases for get_uc_schema_details function."""

    def test_get_schema_details_without_columns(
        self, setup_env_vars, mock_schema_info, mock_table_info
    ):
        """Test getting schema details without column information."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.schemas.get.return_value = mock_schema_info
            mock_client.tables.list.return_value = [mock_table_info]
            mock_get_client.return_value = mock_client

            result = get_uc_schema_details(
                "test_catalog", "test_schema", include_columns=False
            )

            assert "test_catalog.test_schema" in result
            assert "test_table" in result
            # Columns should not be detailed when include_columns=False

    def test_get_schema_details_with_columns(
        self, setup_env_vars, mock_schema_info, mock_table_info
    ):
        """Test getting schema details with column information."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.schemas.get.return_value = mock_schema_info
            mock_client.tables.list.return_value = [mock_table_info]
            mock_get_client.return_value = mock_client

            result = get_uc_schema_details(
                "test_catalog", "test_schema", include_columns=True
            )

            assert "test_catalog.test_schema" in result
            assert "test_table" in result
            assert "id" in result  # Column name should appear
            assert "INT" in result  # Column type should appear

    def test_get_schema_details_no_tables(self, setup_env_vars, mock_schema_info):
        """Test getting schema details with no tables."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.schemas.get.return_value = mock_schema_info
            mock_client.tables.list.return_value = []
            mock_get_client.return_value = mock_client

            result = get_uc_schema_details("test_catalog", "test_schema")

            assert "No tables found" in result

    def test_get_schema_details_error(self, setup_env_vars):
        """Test getting schema details with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.schemas.get.side_effect = Exception("Schema not found")
            mock_get_client.return_value = mock_client

            result = get_uc_schema_details("test_catalog", "invalid_schema")

            assert "Error" in result
            assert "Schema not found" in result


class TestGetUcTableDetails:
    """Test cases for get_uc_table_details function."""

    def test_get_table_details_without_lineage(self, setup_env_vars, mock_table_info):
        """Test getting table details without lineage."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.return_value = mock_table_info
            mock_get_client.return_value = mock_client

            result = get_uc_table_details(
                "test_catalog.test_schema.test_table", include_lineage=False
            )

            assert "test_table" in result
            assert "Test table description" in result
            assert "Lineage fetching skipped" in result

    def test_get_table_details_with_lineage(self, setup_env_vars, mock_table_info):
        """Test getting table details with lineage."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.return_value = mock_table_info
            mock_get_client.return_value = mock_client

            with patch(
                "databricks_mcp.databricks_sdk_utils._get_table_lineage"
            ) as mock_lineage:
                mock_lineage.return_value = {
                    "upstream_tables": ["catalog.schema.upstream"],
                    "downstream_tables": ["catalog.schema.downstream"],
                    "notebooks_reading": [],
                    "notebooks_writing": [],
                }

                result = get_uc_table_details(
                    "test_catalog.test_schema.test_table", include_lineage=True
                )

                assert "test_table" in result
                assert "Lineage Information" in result
                assert "upstream" in result
                assert "downstream" in result

    def test_get_table_details_error(self, setup_env_vars):
        """Test getting table details with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.side_effect = Exception("Table not found")
            mock_get_client.return_value = mock_client

            result = get_uc_table_details("invalid.table.name")

            assert "Error" in result
            assert "Table not found" in result


class TestFormatColumnDetailsMd:
    """Test cases for _format_column_details_md function."""

    def test_format_columns(self, mock_column_info):
        """Test formatting column details."""
        result = _format_column_details_md([mock_column_info])

        assert len(result) == 1
        assert "id" in result[0]
        assert "INT" in result[0]
        assert "not nullable" in result[0]
        assert "Primary key" in result[0]

    def test_format_empty_columns(self):
        """Test formatting empty column list."""
        result = _format_column_details_md([])

        assert len(result) == 1
        assert "No column information" in result[0]

    def test_format_nullable_column(self):
        """Test formatting nullable column."""
        column = Mock(spec=CatalogColumnInfo)
        column.name = "optional_field"
        column.type_text = "STRING"
        column.nullable = True
        column.comment = None
        column.partition_index = None

        result = _format_column_details_md([column])

        assert "nullable" in result[0]
        assert "optional_field" in result[0]


class TestProcessLineageResults:
    """Test cases for _process_lineage_results function."""

    def test_process_lineage_success(self, mock_lineage_data):
        """Test processing lineage results successfully."""
        with patch(
            "databricks_mcp.databricks_sdk_utils._get_job_info_cached"
        ) as mock_job:
            mock_job.return_value = {"name": "Test Job", "tasks": []}

            result = _process_lineage_results(
                mock_lineage_data, "catalog.schema.test_table"
            )

            assert "upstream_tables" in result
            assert "downstream_tables" in result
            assert len(result["upstream_tables"]) > 0
            assert len(result["downstream_tables"]) > 0

    def test_process_lineage_empty(self):
        """Test processing empty lineage results."""
        empty_data = {"status": "success", "data": []}

        result = _process_lineage_results(empty_data, "catalog.schema.test_table")

        assert result["upstream_tables"] == []
        assert result["downstream_tables"] == []

    def test_process_lineage_invalid(self):
        """Test processing invalid lineage results."""
        invalid_data = {"status": "failed"}

        result = _process_lineage_results(invalid_data, "catalog.schema.test_table")

        assert result["upstream_tables"] == []
        assert result["downstream_tables"] == []


class TestLineageCache:
    """Test cases for lineage cache functionality."""

    def test_clear_lineage_cache(self):
        """Test clearing lineage cache."""
        # Add some data to cache
        databricks_sdk_utils._job_cache["test"] = {"name": "Test"}
        databricks_sdk_utils._notebook_cache["test"] = "123"

        clear_lineage_cache()

        assert len(databricks_sdk_utils._job_cache) == 0
        assert len(databricks_sdk_utils._notebook_cache) == 0


# ============================================================================
# Job-related tests
# ============================================================================


class TestFormatHelpers:
    """Test cases for job formatting helper functions."""

    def test_format_run_state_md_with_all_fields(self):
        """Test formatting run state with all fields."""
        from databricks.sdk.service.jobs import RunState, RunLifeCycleState, RunResultState

        state = Mock(spec=RunState)
        state.life_cycle_state = RunLifeCycleState.TERMINATED
        state.result_state = RunResultState.SUCCESS
        state.state_message = "Run completed"

        result = _format_run_state_md(state)

        assert "TERMINATED" in result
        assert "SUCCESS" in result
        assert "Run completed" in result

    def test_format_run_state_md_none(self):
        """Test formatting None run state."""
        result = _format_run_state_md(None)
        assert result == "Unknown"

    def test_format_timestamp_valid(self):
        """Test formatting a valid timestamp."""
        # Jan 1, 2024 00:00:00 UTC - test returns a valid datetime string
        ts_ms = 1704067200000
        result = _format_timestamp(ts_ms)
        # Should return a date string in YYYY-MM-DD HH:MM:SS format
        assert len(result) == 19  # "YYYY-MM-DD HH:MM:SS"
        assert "-" in result
        assert ":" in result

    def test_format_timestamp_none(self):
        """Test formatting None timestamp."""
        result = _format_timestamp(None)
        assert result == "N/A"


class TestGetJob:
    """Test cases for get_job function."""

    def test_get_job_success(self, setup_env_vars):
        """Test getting job details successfully."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_job = Mock()
            mock_job.job_id = 12345
            mock_job.creator_user_name = "user@example.com"
            mock_job.created_time = 1704067200000

            mock_settings = Mock()
            mock_settings.name = "Test Job"
            mock_settings.description = "A test job"
            mock_settings.schedule = None
            mock_settings.max_concurrent_runs = 1
            mock_settings.tasks = []
            mock_settings.job_clusters = []
            mock_job.settings = mock_settings

            mock_client.jobs.get.return_value = mock_job
            mock_get_client.return_value = mock_client

            result = get_job(12345)

            assert "Test Job" in result
            assert "12345" in result
            assert "user@example.com" in result

    def test_get_job_with_tasks(self, setup_env_vars):
        """Test getting job with tasks."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_job = Mock()
            mock_job.job_id = 12345
            mock_job.creator_user_name = None
            mock_job.created_time = None

            mock_task = Mock()
            mock_task.task_key = "task1"
            mock_task.description = "Test task"
            mock_task.notebook_task = Mock()
            mock_task.notebook_task.notebook_path = "/Shared/my_notebook"
            mock_task.spark_python_task = None
            mock_task.spark_jar_task = None
            mock_task.sql_task = None
            mock_task.dbt_task = None
            mock_task.python_wheel_task = None
            mock_task.depends_on = None
            mock_task.timeout_seconds = 3600

            mock_settings = Mock()
            mock_settings.name = "Job With Tasks"
            mock_settings.description = None
            mock_settings.schedule = None
            mock_settings.max_concurrent_runs = None
            mock_settings.tasks = [mock_task]
            mock_settings.job_clusters = []
            mock_job.settings = mock_settings

            mock_client.jobs.get.return_value = mock_job
            mock_get_client.return_value = mock_client

            result = get_job(12345)

            assert "Job With Tasks" in result
            assert "task1" in result
            assert "Notebook" in result
            assert "/Shared/my_notebook" in result

    def test_get_job_error(self, setup_env_vars):
        """Test getting job with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.get.side_effect = Exception("Job not found")
            mock_get_client.return_value = mock_client

            result = get_job(99999)

            assert "Error" in result
            assert "Job not found" in result


class TestListJobs:
    """Test cases for list_jobs function."""

    def test_list_jobs_success(self, setup_env_vars):
        """Test listing jobs successfully."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            from databricks.sdk.service.jobs import BaseJob

            mock_client = Mock()

            mock_job = Mock(spec=BaseJob)
            mock_job.job_id = 12345
            mock_job.creator_user_name = "user@example.com"
            mock_job.created_time = 1704067200000
            mock_settings = Mock()
            mock_settings.name = "Test Job"
            mock_settings.tasks = []
            mock_job.settings = mock_settings

            mock_client.jobs.list.return_value = [mock_job]
            mock_get_client.return_value = mock_client

            result = list_jobs()

            assert "Test Job" in result
            assert "12345" in result

    def test_list_jobs_empty(self, setup_env_vars):
        """Test listing jobs with no results."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.list.return_value = []
            mock_get_client.return_value = mock_client

            result = list_jobs()

            assert "No jobs found" in result

    def test_list_jobs_error(self, setup_env_vars):
        """Test listing jobs with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.list.side_effect = Exception("API error")
            mock_get_client.return_value = mock_client

            result = list_jobs()

            assert "Error" in result
            assert "API error" in result


class TestGetJobRun:
    """Test cases for get_job_run function."""

    def test_get_job_run_success(self, setup_env_vars):
        """Test getting job run details successfully."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            from databricks.sdk.service.jobs import Run, RunLifeCycleState

            mock_client = Mock()
            mock_run = Mock(spec=Run)
            mock_run.run_id = 54321
            mock_run.job_id = 12345
            mock_run.run_name = "Test Run"
            mock_run.number_in_job = 1
            mock_run.start_time = 1704067200000
            mock_run.end_time = 1704070800000
            mock_run.execution_duration = 3600000

            mock_state = Mock()
            mock_state.life_cycle_state = RunLifeCycleState.TERMINATED
            mock_state.result_state = None
            mock_state.state_message = None
            mock_run.state = mock_state

            mock_run.trigger = None
            mock_run.run_page_url = "https://databricks.com/run/54321"
            mock_run.tasks = []
            mock_run.cluster_instance = None

            mock_client.jobs.get_run.return_value = mock_run
            mock_get_client.return_value = mock_client

            result = get_job_run(54321)

            assert "Test Run" in result
            assert "54321" in result
            assert "12345" in result

    def test_get_job_run_error(self, setup_env_vars):
        """Test getting job run with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.get_run.side_effect = Exception("Run not found")
            mock_get_client.return_value = mock_client

            result = get_job_run(99999)

            assert "Error" in result
            assert "Run not found" in result


class TestGetJobRunOutput:
    """Test cases for get_job_run_output function."""

    def test_get_job_run_output_success(self, setup_env_vars):
        """Test getting job run output successfully."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_output = Mock()

            mock_metadata = Mock()
            mock_metadata.job_id = 12345
            mock_metadata.run_id = 54321
            mock_metadata.state = None
            mock_metadata.start_time = 1704067200000
            mock_metadata.end_time = 1704070800000
            mock_output.metadata = mock_metadata

            mock_notebook_output = Mock()
            mock_notebook_output.result = "Output result"
            mock_notebook_output.truncated = False
            mock_output.notebook_output = mock_notebook_output

            mock_output.sql_output = None
            mock_output.dbt_output = None
            mock_output.logs = "Log output"
            mock_output.logs_truncated = False
            mock_output.error = None
            mock_output.error_trace = None

            mock_client.jobs.get_run_output.return_value = mock_output
            mock_get_client.return_value = mock_client

            result = get_job_run_output(54321)

            assert "54321" in result
            assert "Output result" in result
            assert "Log output" in result

    def test_get_job_run_output_error(self, setup_env_vars):
        """Test getting job run output with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.get_run_output.side_effect = Exception("Output not found")
            mock_get_client.return_value = mock_client

            result = get_job_run_output(99999)

            assert "Error" in result
            assert "Output not found" in result


class TestListJobRuns:
    """Test cases for list_job_runs function."""

    def test_list_job_runs_success(self, setup_env_vars):
        """Test listing job runs successfully."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            from databricks.sdk.service.jobs import BaseRun, RunLifeCycleState

            mock_client = Mock()

            mock_run = Mock(spec=BaseRun)
            mock_run.run_id = 54321
            mock_run.job_id = 12345
            mock_run.run_name = "Test Run"
            mock_run.start_time = 1704067200000
            mock_run.end_time = 1704070800000
            mock_run.run_page_url = "https://databricks.com/run/54321"
            mock_run.tasks = []

            mock_state = Mock()
            mock_state.life_cycle_state = RunLifeCycleState.TERMINATED
            mock_state.result_state = None
            mock_state.state_message = None
            mock_run.state = mock_state

            mock_client.jobs.list_runs.return_value = [mock_run]
            mock_get_client.return_value = mock_client

            result = list_job_runs(job_id=12345)

            assert "Test Run" in result
            assert "54321" in result
            assert "12345" in result
            assert "Total" in result

    def test_list_job_runs_empty(self, setup_env_vars):
        """Test listing job runs with no results."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.list_runs.return_value = []
            mock_get_client.return_value = mock_client

            result = list_job_runs(job_id=12345)

            assert "No runs found" in result

    def test_list_job_runs_error(self, setup_env_vars):
        """Test listing job runs with error."""
        with patch(
            "databricks_mcp.databricks_sdk_utils.get_sdk_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.list_runs.side_effect = Exception("API error")
            mock_get_client.return_value = mock_client

            result = list_job_runs()

            assert "Error" in result
            assert "API error" in result
