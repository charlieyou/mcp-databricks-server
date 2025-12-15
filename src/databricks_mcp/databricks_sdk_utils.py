"""
Facade module for backward compatibility.

This module re-exports all public symbols from the refactored modules:
- config: Configuration, workspace management, SQL execution
- lineage: Table lineage processing and caching
- unity_catalog: Unity Catalog operations
- jobs: Job and run operations
"""

from .config import (
    DatabricksConfigError,
    WorkspaceConfig,
    _resolve_workspace_name,
    execute_databricks_sql,
    get_sdk_client,
    get_sql_warehouse_id,
    get_workspace_client,
    get_workspace_configs,
    reload_workspace_configs,
)
from .jobs import (
    _format_notebook_as_markdown,
    _format_run_state_md,
    _format_timestamp,
    _parse_notebook_html,
    export_task_run,
    get_job,
    get_job_run,
    get_job_run_output,
    list_job_runs,
    list_jobs,
)
from .lineage import (
    _get_table_lineage,
    _process_lineage_results,
    clear_lineage_cache,
)
from .unity_catalog import (
    _format_column_details_md,
    get_table_history,
    get_uc_all_catalogs_summary,
    get_uc_catalog_details,
    get_uc_schema_details,
    get_uc_table_details,
)

__all__ = [
    # config
    "DatabricksConfigError",
    "WorkspaceConfig",
    "_resolve_workspace_name",
    "execute_databricks_sql",
    "get_sdk_client",
    "get_sql_warehouse_id",
    "get_workspace_client",
    "get_workspace_configs",
    "reload_workspace_configs",
    # unity_catalog
    "_format_column_details_md",
    "get_table_history",
    "get_uc_all_catalogs_summary",
    "get_uc_catalog_details",
    "get_uc_schema_details",
    "get_uc_table_details",
    # jobs
    "_format_notebook_as_markdown",
    "_format_run_state_md",
    "_format_timestamp",
    "_parse_notebook_html",
    "export_task_run",
    "get_job",
    "get_job_run",
    "get_job_run_output",
    "list_job_runs",
    "list_jobs",
    # lineage
    "_get_table_lineage",
    "_process_lineage_results",
    "clear_lineage_cache",
]
