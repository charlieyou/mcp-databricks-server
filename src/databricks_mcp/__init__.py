"""Databricks MCP Server - A Model Context Protocol server for Databricks Unity Catalog and SQL."""

__version__ = "0.1.0"

from .databricks_formatter import format_query_results
from .databricks_sdk_utils import (
    DatabricksConfigError,
    execute_databricks_sql,
    get_sdk_client,
    get_uc_all_catalogs_summary,
    get_uc_catalog_details,
    get_uc_schema_details,
    get_uc_table_details,
)

__all__ = [
    "format_query_results",
    "DatabricksConfigError",
    "execute_databricks_sql",
    "get_sdk_client",
    "get_uc_all_catalogs_summary",
    "get_uc_catalog_details",
    "get_uc_schema_details",
    "get_uc_table_details",
]
