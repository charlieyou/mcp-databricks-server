"""Comprehensive test coverage for untested critical paths.

Addresses bd issue: mcp-databricks-server-e6e
"""

from unittest.mock import Mock, patch

from databricks.sdk.service.sql import StatementState

from databricks_mcp import config
from databricks_mcp.config import execute_databricks_sql
from databricks_mcp.jobs import (
    _format_notebook_as_markdown,
    _parse_notebook_html,
    export_task_run,
)
from databricks_mcp.lineage import _get_table_lineage, _process_lineage_results
from databricks_mcp.unity_catalog import get_uc_table_details


class TestGetUcTableDetailsWithLineageE2E:
    """E2E tests for get_uc_table_details(include_lineage=True).

    These tests exercise the full pipeline from table fetch through lineage processing.
    """

    def test_full_pipeline_with_upstream_and_downstream(self, setup_env_vars):
        """Test full lineage pipeline with both upstream and downstream tables."""
        mock_table = Mock()
        mock_table.name = "target_table"
        mock_table.full_name = "prod.analytics.target_table"
        mock_table.comment = "Analytics target table"
        mock_table.columns = []

        with patch(
            "databricks_mcp.unity_catalog.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.return_value = mock_table
            mock_get_client.return_value = mock_client

            with patch(
                "databricks_mcp.lineage.execute_databricks_sql"
            ) as mock_sql:
                mock_sql.return_value = {
                    "status": "success",
                    "data": [
                        {
                            "source_table_full_name": "prod.raw.source_table",
                            "target_table_full_name": "prod.analytics.target_table",
                            "entity_type": "NOTEBOOK",
                            "entity_id": "nb123",
                            "entity_run_id": None,
                            "entity_metadata": '{"notebook_id": "nb123"}',
                            "created_by": "user@example.com",
                            "event_time": "2024-01-01T00:00:00Z",
                        },
                        {
                            "source_table_full_name": "prod.analytics.target_table",
                            "target_table_full_name": "prod.reports.downstream_table",
                            "entity_type": "JOB",
                            "entity_id": "job456",
                            "entity_run_id": "run789",
                            "entity_metadata": '{"job_id": "job456"}',
                            "created_by": "scheduler@example.com",
                            "event_time": "2024-01-01T01:00:00Z",
                        },
                    ],
                }

                with patch(
                    "databricks_mcp.lineage._get_job_info_cached"
                ) as mock_job:
                    mock_job.return_value = {"name": "ETL Pipeline", "tasks": []}

                    result = get_uc_table_details(
                        "prod.analytics.target_table", include_lineage=True
                    )

                    assert "target_table" in result
                    assert "## Lineage Information" in result
                    assert "### Upstream Tables" in result
                    assert "prod.raw.source_table" in result
                    assert "### Downstream Tables" in result
                    assert "prod.reports.downstream_table" in result

    def test_full_pipeline_with_notebooks_reading_and_writing(self, setup_env_vars):
        """Test lineage with notebook information."""
        mock_table = Mock()
        mock_table.name = "data_table"
        mock_table.full_name = "main.default.data_table"
        mock_table.comment = None
        mock_table.columns = []

        with patch(
            "databricks_mcp.unity_catalog.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.return_value = mock_table
            mock_get_client.return_value = mock_client

            with patch(
                "databricks_mcp.lineage.execute_databricks_sql"
            ) as mock_sql:
                mock_sql.return_value = {
                    "status": "success",
                    "data": [
                        {
                            "source_table_full_name": "main.default.data_table",
                            "target_table_full_name": "main.default.report_table",
                            "entity_type": "NOTEBOOK",
                            "entity_id": "reader_notebook",
                            "entity_run_id": None,
                            "entity_metadata": '{"notebook_id": "reader_notebook", "job_info": {"job_id": "123"}}',
                            "created_by": "analyst@example.com",
                            "event_time": "2024-01-01T00:00:00Z",
                        },
                        {
                            "source_table_full_name": "main.raw.input_table",
                            "target_table_full_name": "main.default.data_table",
                            "entity_type": "NOTEBOOK",
                            "entity_id": "writer_notebook",
                            "entity_run_id": None,
                            "entity_metadata": '{"notebook_id": "writer_notebook", "job_info": {"job_id": "456"}}',
                            "created_by": "etl@example.com",
                            "event_time": "2024-01-01T00:00:00Z",
                        },
                    ],
                }

                with patch(
                    "databricks_mcp.lineage._get_job_info_cached"
                ) as mock_job:
                    mock_job.return_value = {"name": "Notebook Job", "tasks": []}

                    result = get_uc_table_details(
                        "main.default.data_table", include_lineage=True
                    )

                    assert "data_table" in result
                    assert "Lineage Information" in result

    def test_full_pipeline_lineage_error_handled_gracefully(self, setup_env_vars):
        """Test that lineage errors don't crash the pipeline."""
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.full_name = "cat.schema.test_table"
        mock_table.comment = "Test"
        mock_table.columns = []

        with patch(
            "databricks_mcp.unity_catalog.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.return_value = mock_table
            mock_get_client.return_value = mock_client

            with patch(
                "databricks_mcp.lineage.execute_databricks_sql"
            ) as mock_sql:
                mock_sql.return_value = {
                    "status": "error",
                    "error": "Permission denied to lineage table",
                }

                result = get_uc_table_details(
                    "cat.schema.test_table", include_lineage=True
                )

                assert "test_table" in result
                assert "Lineage Information" in result
                assert "No table, notebook, or job dependencies found" in result or "lineage fetch" in result.lower()

    def test_full_pipeline_table_without_comment_or_columns(self, setup_env_vars):
        """Test table without comment or columns shows appropriate defaults."""
        mock_table = Mock()
        mock_table.name = "sparse_table"
        mock_table.full_name = "cat.schema.sparse_table"
        mock_table.comment = None
        mock_table.columns = None

        with patch(
            "databricks_mcp.unity_catalog.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.return_value = mock_table
            mock_get_client.return_value = mock_client

            with patch(
                "databricks_mcp.lineage.execute_databricks_sql"
            ) as mock_sql:
                mock_sql.return_value = {"status": "success", "data": []}

                result = get_uc_table_details(
                    "cat.schema.sparse_table", include_lineage=True
                )

                assert "sparse_table" in result
                assert "No description provided" in result
                assert "not partitioned" in result or "Partition" in result
                assert "No column information" in result


class TestGetTableLineageIntegration:
    """Integration tests for _get_table_lineage + _process_lineage_results."""

    def test_get_table_lineage_no_warehouse_configured(self, monkeypatch, tmp_path):
        """Test _get_table_lineage when no warehouse is configured."""
        cfg_file = tmp_path / ".databrickscfg"
        cfg_file.write_text("""[DEFAULT]
host = https://test.databricks.com
token = test_token
""")
        monkeypatch.setattr(
            "databricks_mcp.config._get_databrickscfg_path", lambda: cfg_file
        )
        config.reload_workspace_configs()

        result = _get_table_lineage("cat.schema.table")

        assert result["status"] == "error"
        assert "SQL warehouse ID is not configured" in result["error"]

    def test_get_table_lineage_success(self, setup_env_vars):
        """Test _get_table_lineage with successful SQL execution."""
        with patch(
            "databricks_mcp.lineage.execute_databricks_sql"
        ) as mock_sql:
            mock_sql.return_value = {
                "status": "success",
                "data": [
                    {
                        "source_table_full_name": "upstream.table",
                        "target_table_full_name": "target.table",
                        "entity_type": "JOB",
                        "entity_id": "job123",
                        "entity_run_id": "run456",
                        "entity_metadata": "{}",
                        "created_by": "user",
                        "event_time": "2024-01-01",
                    }
                ],
            }

            with patch(
                "databricks_mcp.lineage._get_job_info_cached"
            ) as mock_job:
                mock_job.return_value = None

                result = _get_table_lineage("target.table")

                assert "upstream_tables" in result
                assert "downstream_tables" in result
                mock_sql.assert_called_once()

    def test_process_lineage_with_notebook_and_job_info(self):
        """Test _process_lineage_results with rich notebook/job metadata."""
        raw_data = {
            "status": "success",
            "data": [
                {
                    "source_table_full_name": "source.table",
                    "target_table_full_name": "target.table",
                    "entity_type": "NOTEBOOK",
                    "entity_id": "notebook_id",
                    "entity_run_id": None,
                    "entity_metadata": '{"notebook_id": "notebook_id", "job_info": {"job_id": "job123", "run_id": "run456"}}',
                    "created_by": "user@example.com",
                    "event_time": "2024-01-01T00:00:00Z",
                },
            ],
        }

        with patch(
            "databricks_mcp.lineage._get_job_info_cached"
        ) as mock_job:
            mock_job.return_value = {
                "name": "ETL Job",
                "tasks": [{"task_key": "main", "notebook_path": "/Shared/etl_notebook"}],
            }

            result = _process_lineage_results(raw_data, "target.table")

            assert "upstream_tables" in result
            assert "source.table" in result["upstream_tables"]

    def test_process_lineage_deduplicates_tables(self):
        """Test that duplicate table entries are deduplicated."""
        raw_data = {
            "status": "success",
            "data": [
                {
                    "source_table_full_name": "source.table",
                    "target_table_full_name": "target.table",
                    "entity_type": "JOB",
                    "entity_id": "job1",
                    "entity_run_id": None,
                    "entity_metadata": "{}",
                    "created_by": "user1",
                    "event_time": "2024-01-01",
                },
                {
                    "source_table_full_name": "source.table",
                    "target_table_full_name": "target.table",
                    "entity_type": "JOB",
                    "entity_id": "job2",
                    "entity_run_id": None,
                    "entity_metadata": "{}",
                    "created_by": "user2",
                    "event_time": "2024-01-02",
                },
            ],
        }

        with patch(
            "databricks_mcp.lineage._get_job_info_cached"
        ) as mock_job:
            mock_job.return_value = None

            result = _process_lineage_results(raw_data, "target.table")

            assert result["upstream_tables"].count("source.table") == 1


class TestMissingSqlWarehouse:
    """Negative tests for missing SQL warehouse configuration."""

    def test_execute_sql_missing_warehouse(self, monkeypatch, tmp_path):
        """Test execute_databricks_sql without warehouse ID returns error."""
        cfg_file = tmp_path / ".databrickscfg"
        cfg_file.write_text("""[DEFAULT]
host = https://test.databricks.com
token = test_token
""")
        monkeypatch.setattr("databricks_mcp.config._get_databrickscfg_path", lambda: cfg_file)
        config.reload_workspace_configs()

        result = execute_databricks_sql("SELECT 1")

        assert result["status"] == "error"
        assert "SQL warehouse ID is not configured" in result["error"]

    def test_lineage_skipped_when_no_warehouse(self, setup_env_vars, monkeypatch, tmp_path):
        """Test that lineage is skipped with proper message when no warehouse."""
        cfg_file = tmp_path / ".databrickscfg"
        cfg_file.write_text("""[DEFAULT]
host = https://test.databricks.com
token = test_token
""")
        monkeypatch.setattr("databricks_mcp.config._get_databrickscfg_path", lambda: cfg_file)
        config.reload_workspace_configs()

        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.full_name = "cat.schema.test_table"
        mock_table.comment = "Test"
        mock_table.columns = []

        with patch(
            "databricks_mcp.unity_catalog.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.tables.get.return_value = mock_table
            mock_get_client.return_value = mock_client

            result = get_uc_table_details(
                "cat.schema.test_table", include_lineage=True
            )

            assert "Lineage fetching skipped" in result
            assert "SQL warehouse ID is not configured" in result


class TestSqlTimeoutAndFailureStates:
    """Tests for SQL timeout and failure states."""

    def test_sql_timeout_pending_state(self, setup_env_vars):
        """Test SQL query that times out in PENDING state."""
        with patch(
            "databricks_mcp.config.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.PENDING
            mock_status.error = None
            mock_response.status = mock_status
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT * FROM large_table", wait_timeout="1s")

            assert result["status"] == "error"
            assert "timed out" in result["error"]
            assert "still running" in result["error"]

    def test_sql_timeout_running_state(self, setup_env_vars):
        """Test SQL query that times out in RUNNING state."""
        with patch(
            "databricks_mcp.config.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.RUNNING
            mock_status.error = None
            mock_response.status = mock_status
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT * FROM large_table")

            assert result["status"] == "error"
            assert "timed out" in result["error"]

    def test_sql_failed_state_with_error_details(self, setup_env_vars):
        """Test SQL query that fails with error details."""
        with patch(
            "databricks_mcp.config.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.FAILED
            mock_error = Mock()
            mock_error.message = "PARSE_SYNTAX_ERROR: Syntax error at line 1"
            mock_status.error = mock_error
            mock_response.status = mock_status
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELEC * FORM table")

            assert result["status"] == "failed"
            assert "failed with state" in result["error"]
            assert "PARSE_SYNTAX_ERROR" in result["details"]

    def test_sql_failed_state_without_error_details(self, setup_env_vars):
        """Test SQL query that fails without specific error details."""
        with patch(
            "databricks_mcp.config.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.FAILED
            mock_status.error = None
            mock_response.status = mock_status
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT 1")

            assert result["status"] == "failed"
            assert "No error details provided" in result["details"]

    def test_sql_canceled_state(self, setup_env_vars):
        """Test SQL query that is canceled."""
        with patch(
            "databricks_mcp.config.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.CANCELED
            mock_error = Mock()
            mock_error.message = "Query was canceled by user"
            mock_status.error = mock_error
            mock_response.status = mock_status
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT 1")

            assert result["status"] == "failed"
            assert "CANCELED" in result["error"]

    def test_sql_closed_state(self, setup_env_vars):
        """Test SQL query in CLOSED state."""
        with patch(
            "databricks_mcp.config.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_status = Mock()
            mock_status.state = StatementState.CLOSED
            mock_status.error = None
            mock_response.status = mock_status
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT 1")

            assert result["status"] == "failed"
            assert "CLOSED" in result["error"]

    def test_sql_no_status(self, setup_env_vars):
        """Test SQL query with no status returned."""
        with patch(
            "databricks_mcp.config.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_response = Mock()
            mock_response.status = None
            mock_client.statement_execution.execute_statement.return_value = mock_response
            mock_get_client.return_value = mock_client

            result = execute_databricks_sql("SELECT 1")

            assert result["status"] == "failed"
            assert "unknown" in result["error"].lower()


class TestNotebookExportParsing:
    """Tests for notebook export parsing functions."""

    def test_parse_notebook_html_valid(self):
        """Test parsing valid notebook HTML with base64 encoded content."""
        import base64
        import json
        from urllib.parse import quote

        notebook_data = {
            "name": "Test Notebook",
            "language": "python",
            "commands": [
                {"command": "print('hello')", "state": "finished", "results": None}
            ],
        }
        encoded = base64.b64encode(
            quote(json.dumps(notebook_data)).encode()
        ).decode()
        html = f"<html>something= '{encoded}'</html>"

        result = _parse_notebook_html(html)

        assert result is not None
        assert result["name"] == "Test Notebook"
        assert result["language"] == "python"

    def test_parse_notebook_html_no_match(self):
        """Test parsing HTML without embedded notebook data."""
        html = "<html><body>Regular HTML content</body></html>"

        result = _parse_notebook_html(html)

        assert result is None

    def test_parse_notebook_html_invalid_base64(self):
        """Test parsing HTML with invalid base64 content."""
        html = "something= 'not-valid-base64-but-long-enough-to-match-regex!!!'"

        result = _parse_notebook_html(html)

        assert result is None

    def test_parse_notebook_html_invalid_json(self):
        """Test parsing HTML with valid base64 but invalid JSON."""
        import base64
        from urllib.parse import quote

        invalid_json = "not valid json at all"
        encoded = base64.b64encode(quote(invalid_json).encode()).decode()
        html = f"= '{encoded}'"

        result = _parse_notebook_html(html)

        assert result is None

    def test_format_notebook_as_markdown_basic(self):
        """Test formatting a basic notebook as markdown."""
        notebook = {
            "name": "My Notebook",
            "language": "python",
            "commands": [
                {"command": "x = 1 + 1", "state": "finished", "results": None},
                {"command": "print(x)", "state": "finished", "results": None},
            ],
        }

        result = _format_notebook_as_markdown(notebook)

        assert "# Notebook: My Notebook" in result
        assert "**Language**: python" in result
        assert "## Cell 1" in result
        assert "## Cell 2" in result
        assert "x = 1 + 1" in result
        assert "print(x)" in result

    def test_format_notebook_with_output(self):
        """Test formatting notebook with cell output."""
        notebook = {
            "name": "Output Notebook",
            "language": "python",
            "commands": [
                {
                    "command": "print('hello world')",
                    "state": "finished",
                    "results": {
                        "data": [{"type": "ansi", "data": "hello world"}]
                    },
                }
            ],
        }

        result = _format_notebook_as_markdown(notebook)

        assert "**Output:**" in result
        assert "hello world" in result

    def test_format_notebook_with_error_state(self):
        """Test formatting notebook cell with error state."""
        notebook = {
            "name": "Error Notebook",
            "language": "python",
            "commands": [
                {
                    "command": "1/0",
                    "state": "error",
                    "results": None,
                    "errorSummary": "ZeroDivisionError",
                    "error": "division by zero",
                }
            ],
        }

        result = _format_notebook_as_markdown(notebook)

        assert "❌ Error" in result
        assert "**Error:**" in result
        assert "ZeroDivisionError" in result
        assert "division by zero" in result

    def test_format_notebook_truncates_long_output(self):
        """Test that long output is truncated."""
        long_output = "x" * 3000
        notebook = {
            "name": "Long Output",
            "language": "python",
            "commands": [
                {
                    "command": "print(long_string)",
                    "state": "finished",
                    "results": {"data": [{"type": "ansi", "data": long_output}]},
                }
            ],
        }

        result = _format_notebook_as_markdown(notebook)

        assert "... (truncated)" in result
        assert len(result) < len(long_output) + 500

    def test_format_notebook_skips_empty_commands(self):
        """Test that empty commands are skipped."""
        notebook = {
            "name": "Sparse Notebook",
            "language": "python",
            "commands": [
                {"command": "", "state": "finished", "results": None},
                {"command": "real_code()", "state": "finished", "results": None},
                {"command": "   ", "state": "finished", "results": None},
            ],
        }

        result = _format_notebook_as_markdown(notebook)

        assert "real_code()" in result
        assert result.count("## Cell") == 1

    def test_format_notebook_with_only_error_summary(self):
        """Test formatting with only error summary (no detailed error)."""
        notebook = {
            "name": "Error Summary Only",
            "language": "python",
            "commands": [
                {
                    "command": "import nonexistent",
                    "state": "error",
                    "results": None,
                    "errorSummary": "ModuleNotFoundError: No module named 'nonexistent'",
                    "error": None,
                }
            ],
        }

        result = _format_notebook_as_markdown(notebook)

        assert "ModuleNotFoundError" in result


class TestExportTaskRun:
    """Tests for export_task_run function."""

    def test_export_task_run_success(self, setup_env_vars):
        """Test successful task run export."""
        import base64
        import json
        from urllib.parse import quote

        notebook_data = {
            "name": "ETL Notebook",
            "language": "python",
            "commands": [{"command": "df.write.save()", "state": "finished"}],
        }
        encoded = base64.b64encode(quote(json.dumps(notebook_data)).encode()).decode()
        html_content = f"= '{encoded}'"

        with patch(
            "databricks_mcp.jobs.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_view = Mock()
            mock_view.content = html_content
            mock_view.name = "notebook_view"
            mock_export = Mock()
            mock_export.views = [mock_view]
            mock_client.jobs.export_run.return_value = mock_export
            mock_get_client.return_value = mock_client

            result = export_task_run(12345)

            assert "ETL Notebook" in result
            assert "df.write.save()" in result

    def test_export_task_run_no_views(self, setup_env_vars):
        """Test export when no views are available."""
        with patch(
            "databricks_mcp.jobs.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_export = Mock()
            mock_export.views = None
            mock_client.jobs.export_run.return_value = mock_export
            mock_get_client.return_value = mock_client

            result = export_task_run(12345)

            assert "No views available" in result

    def test_export_task_run_empty_views(self, setup_env_vars):
        """Test export when views list is empty."""
        with patch(
            "databricks_mcp.jobs.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_export = Mock()
            mock_export.views = []
            mock_client.jobs.export_run.return_value = mock_export
            mock_get_client.return_value = mock_client

            result = export_task_run(12345)

            assert "No views available" in result

    def test_export_task_run_unparseable_content(self, setup_env_vars):
        """Test export when notebook content can't be parsed."""
        with patch(
            "databricks_mcp.jobs.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_view = Mock()
            mock_view.content = "<html>not a notebook</html>"
            mock_view.name = "Bad View"
            mock_export = Mock()
            mock_export.views = [mock_view]
            mock_client.jobs.export_run.return_value = mock_export
            mock_get_client.return_value = mock_client

            result = export_task_run(12345)

            assert "Bad View" in result
            assert "Could not parse" in result

    def test_export_task_run_no_content_in_view(self, setup_env_vars):
        """Test export when view has no content."""
        with patch(
            "databricks_mcp.jobs.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_view = Mock()
            mock_view.content = None
            mock_export = Mock()
            mock_export.views = [mock_view]
            mock_client.jobs.export_run.return_value = mock_export
            mock_get_client.return_value = mock_client

            result = export_task_run(12345)

            assert "No parseable content" in result

    def test_export_task_run_error(self, setup_env_vars):
        """Test export when API call fails."""
        with patch(
            "databricks_mcp.jobs.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_client.jobs.export_run.side_effect = Exception("Run not found")
            mock_get_client.return_value = mock_client

            result = export_task_run(99999)

            assert "Error" in result
            assert "Run not found" in result

    def test_export_task_run_with_dashboards(self, setup_env_vars):
        """Test export with include_dashboards=True."""
        import base64
        import json
        from urllib.parse import quote

        notebook_data = {"name": "Dashboard Notebook", "language": "sql", "commands": []}
        encoded = base64.b64encode(quote(json.dumps(notebook_data)).encode()).decode()
        html_content = f"= '{encoded}'"

        with patch(
            "databricks_mcp.jobs.get_workspace_client"
        ) as mock_get_client:
            mock_client = Mock()
            mock_view = Mock()
            mock_view.content = html_content
            mock_view.name = "notebook"
            mock_export = Mock()
            mock_export.views = [mock_view]
            mock_client.jobs.export_run.return_value = mock_export
            mock_get_client.return_value = mock_client

            from databricks.sdk.service.jobs import ViewsToExport

            export_task_run(12345, include_dashboards=True)

            mock_client.jobs.export_run.assert_called_once()
            call_kwargs = mock_client.jobs.export_run.call_args.kwargs
            assert call_kwargs["views_to_export"] == ViewsToExport.ALL


class TestLineageEdgeCases:
    """Additional edge case tests for lineage processing."""

    def test_lineage_with_malformed_metadata_json(self):
        """Test processing lineage with invalid JSON in metadata."""
        raw_data = {
            "status": "success",
            "data": [
                {
                    "source_table_full_name": "source.table",
                    "target_table_full_name": "target.table",
                    "entity_type": "NOTEBOOK",
                    "entity_id": "nb1",
                    "entity_run_id": None,
                    "entity_metadata": "not valid json {{{",
                    "created_by": "user",
                    "event_time": "2024-01-01",
                },
            ],
        }

        result = _process_lineage_results(raw_data, "target.table")

        assert "upstream_tables" in result
        assert "source.table" in result["upstream_tables"]

    def test_lineage_with_null_entity_metadata(self):
        """Test processing lineage with null metadata."""
        raw_data = {
            "status": "success",
            "data": [
                {
                    "source_table_full_name": "source.table",
                    "target_table_full_name": "target.table",
                    "entity_type": "JOB",
                    "entity_id": "job1",
                    "entity_run_id": None,
                    "entity_metadata": None,
                    "created_by": "user",
                    "event_time": "2024-01-01",
                },
            ],
        }

        with patch(
            "databricks_mcp.lineage._get_job_info_cached"
        ) as mock_job:
            mock_job.return_value = None

            result = _process_lineage_results(raw_data, "target.table")

            assert "upstream_tables" in result
            assert "source.table" in result["upstream_tables"]
