"""Tests for databricks_formatter module."""

from databricks_mcp.databricks_formatter import format_query_results


class TestFormatQueryResults:
    """Test cases for format_query_results function."""

    def test_format_sdk_success_result(self, mock_sql_success_result):
        """Test formatting successful SDK-based query results."""
        output = format_query_results(mock_sql_success_result)

        # Check that column headers are present
        assert "id | name | age" in output

        # Check that data rows are present
        assert "1 | Alice | 30" in output
        assert "2 | Bob | 25" in output

        # Check for separator line
        assert "---" in output

    def test_format_empty_result(self):
        """Test formatting when query returns no data."""
        result = {
            "status": "success",
            "data": [],
            "message": "Query succeeded but returned no data.",
        }
        output = format_query_results(result)
        assert "Query succeeded but returned no data" in output

    def test_format_error_result(self, mock_sql_error_result):
        """Test formatting error results."""
        output = format_query_results(mock_sql_error_result)
        assert "Error from query execution" in output
        assert "Table not found" in output

    def test_format_null_values(self):
        """Test formatting results with NULL values."""
        result = {
            "status": "success",
            "row_count": 1,
            "data": [{"id": "1", "name": None, "age": "30"}],
        }
        output = format_query_results(result)
        assert "NULL" in output
        assert "1 | NULL | 30" in output

    def test_format_old_api_style(self):
        """Test formatting old direct API style output."""
        result = {
            "manifest": {"schema": {"columns": [{"name": "id"}, {"name": "value"}]}},
            "result": {"data_array": [["1", "test"], ["2", "data"]]},
        }
        output = format_query_results(result)
        assert "id | value" in output
        assert "1 | test" in output
        assert "2 | data" in output

    def test_format_empty_input(self):
        """Test formatting with empty/None input."""
        output = format_query_results(None)
        assert "No results or invalid result format" in output

        output = format_query_results({})
        assert (
            "Invalid or unrecognized result format" in output
            or "No results or invalid result format" in output
        )

    def test_format_no_columns(self):
        """Test formatting with missing column names."""
        result = {"status": "success", "data": []}
        output = format_query_results(result)
        assert "Query succeeded but returned no data" in output

    def test_format_complex_data_types(self):
        """Test formatting results with complex data types."""
        result = {
            "status": "success",
            "row_count": 2,
            "data": [
                {"id": "1", "data": '{"key": "value"}', "count": "100"},
                {"id": "2", "data": "[1, 2, 3]", "count": "200"},
            ],
        }
        output = format_query_results(result)
        assert '{"key": "value"}' in output
        assert "[1, 2, 3]" in output
        assert "100" in output
        assert "200" in output

    def test_format_single_row(self):
        """Test formatting results with single row."""
        result = {
            "status": "success",
            "row_count": 1,
            "data": [{"id": "1", "value": "single"}],
        }
        output = format_query_results(result)
        assert "id | value" in output
        assert "1 | single" in output

    def test_format_many_columns(self):
        """Test formatting results with many columns."""
        result = {
            "status": "success",
            "row_count": 1,
            "data": [
                {
                    "col1": "a",
                    "col2": "b",
                    "col3": "c",
                    "col4": "d",
                    "col5": "e",
                    "col6": "f",
                }
            ],
        }
        output = format_query_results(result)
        assert "col1" in output and "col6" in output
        assert "a" in output and "f" in output
