"""Tests for unity_catalog module."""

import pytest

from databricks_mcp.unity_catalog import _validate_table_name


class TestValidateTableName:
    """Test cases for _validate_table_name function."""

    def test_valid_simple_name(self):
        """Test basic valid table name."""
        result = _validate_table_name("catalog.schema.table")
        assert result == "`catalog`.`schema`.`table`"

    def test_valid_with_underscores(self):
        """Test valid table name with underscores."""
        result = _validate_table_name("my_catalog.my_schema.my_table")
        assert result == "`my_catalog`.`my_schema`.`my_table`"

    def test_valid_with_numbers(self):
        """Test valid table name with numbers."""
        result = _validate_table_name("catalog1.schema2.table3")
        assert result == "`catalog1`.`schema2`.`table3`"

    def test_valid_with_hyphens(self):
        """Test valid table name with hyphens (UC allows this)."""
        result = _validate_table_name("my-catalog.my-schema.my-table")
        assert result == "`my-catalog`.`my-schema`.`my-table`"

    def test_valid_mixed_characters(self):
        """Test valid table name with mixed allowed characters."""
        result = _validate_table_name("cat_1.schema-2.table_3-final")
        assert result == "`cat_1`.`schema-2`.`table_3-final`"

    def test_invalid_too_few_parts(self):
        """Test error when table name has too few parts."""
        with pytest.raises(ValueError) as exc_info:
            _validate_table_name("schema.table")
        assert "Expected format: catalog.schema.table" in str(exc_info.value)

    def test_invalid_too_many_parts(self):
        """Test error when table name has too many parts."""
        with pytest.raises(ValueError) as exc_info:
            _validate_table_name("a.b.c.d")
        assert "Expected format: catalog.schema.table" in str(exc_info.value)

    def test_invalid_empty_part(self):
        """Test error when table name has empty part."""
        with pytest.raises(ValueError) as exc_info:
            _validate_table_name("catalog..table")
        assert "Expected format: catalog.schema.table" in str(exc_info.value)

    def test_invalid_special_characters(self):
        """Test error when table name has disallowed special characters."""
        with pytest.raises(ValueError) as exc_info:
            _validate_table_name("catalog.schema.table@name")
        assert "Only letters, numbers, underscores, and hyphens are allowed" in str(
            exc_info.value
        )

    def test_invalid_spaces(self):
        """Test error when table name has spaces."""
        with pytest.raises(ValueError) as exc_info:
            _validate_table_name("catalog.schema.table name")
        assert "Only letters, numbers, underscores, and hyphens are allowed" in str(
            exc_info.value
        )

    def test_invalid_backticks_rejected(self):
        """Test that backticks are rejected to prevent SQL injection."""
        with pytest.raises(ValueError) as exc_info:
            _validate_table_name("catalog.schema.table`; DROP TABLE--")
        assert "Only letters, numbers, underscores, and hyphens are allowed" in str(
            exc_info.value
        )
