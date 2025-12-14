import asyncio
import functools
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.fastmcp.server import Context

from .databricks_formatter import format_query_results
from .databricks_sdk_utils import (
    DatabricksConfigError,
    execute_databricks_sql,
    export_task_run,
    get_job,
    get_job_run,
    get_job_run_output,
    get_table_history,
    get_uc_all_catalogs_summary,
    get_uc_catalog_details,
    get_uc_schema_details,
    get_uc_table_details,
    get_workspace_configs,
    list_job_runs,
    list_jobs,
)


@dataclass
class DatabricksSessionContext:
    """Server-global Databricks state shared across MCP client sessions."""
    active_workspace: Optional[str] = field(default=None)


@asynccontextmanager
async def databricks_session_lifespan(server: FastMCP) -> AsyncIterator[DatabricksSessionContext]:
    """
    Initialize DatabricksSessionContext for the FastMCP server lifespan.

    Note: FastMCP lifespan is server-global; this context is shared across
    MCP client sessions. For true per-client workspace state, use the
    workspaces dict keyed by client_id.
    """
    yield DatabricksSessionContext()


mcp = FastMCP("databricks", lifespan=databricks_session_lifespan)


def _get_session_workspace(
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> Optional[str]:
    """
    Resolve workspace from priority order:
    1) Explicit workspace param
    2) Session's active_workspace from lifespan context
    3) None (let SDK decide based on default config)
    """
    if workspace is not None:
        return workspace
    if ctx is not None:
        try:
            lifespan_ctx = ctx.request_context.lifespan_context
            if isinstance(lifespan_ctx, DatabricksSessionContext) and lifespan_ctx.active_workspace:
                return lifespan_ctx.active_workspace
        except (AttributeError, TypeError):
            pass
    return None


def format_exception_md(title: str, details: str) -> str:
    return f"""# {title}
**Details:**
```
{details}
```"""


def handle_tool_errors(tool_name):
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                return await fn(*args, **kwargs)
            except ToolError:
                raise
            except DatabricksConfigError as e:
                raise ToolError(f"{tool_name}: Databricks not configured - {e}") from e
            except Exception as e:
                raise ToolError(f"{tool_name}: Unexpected error - {e}") from e

        return wrapper

    return decorator


@mcp.tool()
@handle_tool_errors("execute_sql_query")
async def execute_sql_query(
    sql: str,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Executes a given SQL query against the Databricks SQL warehouse and returns the formatted results.

    Use this tool when you need to run specific SQL queries, such as SELECT, SHOW, or other DQL statements.
    This is ideal for targeted data retrieval or for queries that are too complex for the structured description tools.
    The results are returned in a human-readable, Markdown-like table format.

    **Important:** Always use `LIMIT` in your queries to avoid retrieving excessive amounts of data.

    Args:
        sql: The complete SQL query string to execute.
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    sdk_result = await asyncio.to_thread(
        execute_databricks_sql, sql_query=sql, workspace=resolved_workspace
    )

    status = sdk_result.get("status")
    if status == "failed":
        error_message = sdk_result.get("error", "Unknown query execution error.")
        details = sdk_result.get("details", "No additional details provided.")
        raise ToolError(f"SQL Query Failed: {error_message}\nDetails: {details}")
    elif status == "error":
        error_message = sdk_result.get("error", "Unknown error during SQL execution.")
        details = sdk_result.get("details", "No additional details provided.")
        raise ToolError(f"Error during SQL Execution: {error_message}\nDetails: {details}")
    elif status == "success":
        return format_query_results(sdk_result)
    else:
        raise ToolError(f"Received an unexpected status from query execution: {status}. Result: {sdk_result}")


@mcp.tool()
@handle_tool_errors("describe_uc_table")
async def describe_uc_table(
    full_table_name: str,
    include_lineage: bool = False,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Provides a detailed description of a specific Unity Catalog table.

    Use this tool to understand the structure (columns, data types, partitioning) of a single table.
    This is essential before constructing SQL queries against the table.

    Optionally, it can include comprehensive lineage information that goes beyond traditional
    table-to-table dependencies:

    **Table Lineage:**
    - Upstream tables (tables this table reads from)
    - Downstream tables (tables that read from this table)

    **Notebook & Job Lineage:**
    - Notebooks that read from this table, including:
      * Notebook name and workspace path
      * Associated Databricks job information (job name, ID, task details)
    - Notebooks that write to this table with the same detailed context

    **Use Cases:**
    - Data impact analysis: understand what breaks if you modify this table
    - Code discovery: find notebooks that process this data for further analysis
    - Debugging: trace data flow issues by examining both table dependencies and processing code
    - Documentation: understand the complete data ecosystem around a table

    The lineage information allows LLMs and tools to subsequently fetch the actual notebook
    code content for deeper analysis of data transformations and business logic.

    The output is formatted in Markdown.

    Args:
        full_table_name: The fully qualified three-part name of the table (e.g., `catalog.schema.table`).
        include_lineage: Set to True to fetch and include comprehensive lineage (tables, notebooks, jobs).
                         Defaults to False. May take longer to retrieve but provides rich context for
                         understanding data dependencies and enabling code exploration.
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    details_markdown = await asyncio.to_thread(
        get_uc_table_details,
        full_table_name=full_table_name,
        include_lineage=include_lineage,
        workspace=resolved_workspace,
    )
    return details_markdown


@mcp.tool()
@handle_tool_errors("get_uc_table_history")
async def get_uc_table_history(
    full_table_name: str,
    limit: int = 10,
    start_timestamp: str | None = None,
    end_timestamp: str | None = None,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Retrieves the version history of a Delta table.

    Use this tool to understand recent changes to a table, including what operations
    were performed, when, and by whom. Useful for auditing and debugging data issues.

    The output is formatted in Markdown.

    Args:
        full_table_name: The fully qualified three-part name of the table (e.g., `catalog.schema.table`).
        limit: Maximum number of history records to return. Default is 10.
        start_timestamp: Optional. Filter to show only history after this timestamp (ISO format, e.g., '2024-01-01' or '2024-01-01T00:00:00').
        end_timestamp: Optional. Filter to show only history before this timestamp (ISO format).
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    history_markdown = await asyncio.to_thread(
        get_table_history,
        table_full_name=full_table_name,
        limit=limit,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        workspace=resolved_workspace,
    )
    return history_markdown


@mcp.tool()
@handle_tool_errors("describe_uc_catalog")
async def describe_uc_catalog(
    catalog_name: str,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Provides a summary of a specific Unity Catalog, listing all its schemas with their names and descriptions.

    Use this tool when you know the catalog name and need to discover the schemas within it.
    This is often a precursor to describing a specific schema or table.
    The output is formatted in Markdown.

    Args:
        catalog_name: The name of the Unity Catalog to describe (e.g., `prod`, `dev`, `system`).
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    summary_markdown = await asyncio.to_thread(
        get_uc_catalog_details, catalog_name=catalog_name, workspace=resolved_workspace
    )
    return summary_markdown


@mcp.tool()
@handle_tool_errors("describe_uc_schema")
async def describe_uc_schema(
    catalog_name: str,
    schema_name: str,
    include_columns: bool = False,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Provides detailed information about a specific schema within a Unity Catalog.

    Use this tool to understand the contents of a schema, primarily its tables.
    Optionally, it can list all tables within the schema and their column details.
    Set `include_columns=True` to get column information, which is crucial for query construction but makes the output longer.
    If `include_columns=False`, only table names and descriptions are shown, useful for a quicker overview.
    The output is formatted in Markdown.

    Args:
        catalog_name: The name of the catalog containing the schema.
        schema_name: The name of the schema to describe.
        include_columns: If True, lists tables with their columns. Defaults to False for a briefer summary.
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    details_markdown = await asyncio.to_thread(
        get_uc_schema_details,
        catalog_name=catalog_name,
        schema_name=schema_name,
        include_columns=include_columns,
        workspace=resolved_workspace,
    )
    return details_markdown


@mcp.tool()
@handle_tool_errors("list_uc_catalogs")
async def list_uc_catalogs(
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Lists all available Unity Catalogs with their names, descriptions, and types.

    Use this tool as a starting point to discover available data sources when you don't know specific catalog names.
    It provides a high-level overview of all accessible catalogs in the workspace.
    The output is formatted in Markdown.

    Args:
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    summary_markdown = await asyncio.to_thread(
        get_uc_all_catalogs_summary, workspace=resolved_workspace
    )
    return summary_markdown


# ============================================================================
# Job-related MCP tools
# ============================================================================


@mcp.tool()
@handle_tool_errors("get_databricks_job")
async def get_databricks_job(
    job_id: int,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Retrieves details for a specific Databricks job by its job ID.

    Use this tool to get comprehensive information about a job including:
    - Job name, description, and creator
    - Schedule configuration (cron expression, timezone, pause status)
    - Task definitions (notebook, spark python, spark jar, SQL, dbt tasks)
    - Task dependencies and timeout settings
    - Job cluster configurations

    The output is formatted in Markdown.

    Args:
        job_id: The unique identifier of the job to retrieve.
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    return await asyncio.to_thread(get_job, job_id=job_id, workspace=resolved_workspace)


@mcp.tool()
@handle_tool_errors("list_databricks_jobs")
async def list_databricks_jobs(
    name: str | None = None,
    expand_tasks: bool = False,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Lists all Databricks jobs in the workspace with optional filtering.

    Use this tool to discover available jobs or search for specific jobs by name.
    Results include job IDs, names, creators, and creation timestamps.

    The output is formatted in Markdown.

    Args:
        name: Optional filter to find jobs whose names contain this string.
        expand_tasks: If True, includes task keys in the output (default: False).
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    return await asyncio.to_thread(
        list_jobs,
        name=name,
        expand_tasks=expand_tasks,
        workspace=resolved_workspace,
    )


@mcp.tool()
@handle_tool_errors("get_databricks_job_run")
async def get_databricks_job_run(
    run_id: int,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Retrieves details for a specific job run by its run ID.

    Use this tool to get comprehensive information about a job run including:
    - Run state (lifecycle state, result state, state message)
    - Timing information (start time, end time, duration)
    - Individual task run details with their states
    - Cluster information
    - Link to the Databricks UI

    The output is formatted in Markdown.

    Args:
        run_id: The unique identifier of the run to retrieve.
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    return await asyncio.to_thread(get_job_run, run_id=run_id, workspace=resolved_workspace)


@mcp.tool()
@handle_tool_errors("get_databricks_job_run_output")
async def get_databricks_job_run_output(
    run_id: int,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Retrieves the output of a specific job run by its run ID.

    Use this tool to get the results and logs from a completed job run including:
    - Run metadata (job ID, status, timing)
    - Notebook output (result value)
    - SQL output (link to results)
    - dbt output (artifacts link)
    - Execution logs
    - Error messages and stack traces (if the run failed)

    The output is formatted in Markdown.

    Args:
        run_id: The unique identifier of the run whose output to retrieve.
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    return await asyncio.to_thread(
        get_job_run_output, run_id=run_id, workspace=resolved_workspace
    )


@mcp.tool()
@handle_tool_errors("list_databricks_job_runs")
async def list_databricks_job_runs(
    job_id: int | None = None,
    active_only: bool = False,
    completed_only: bool = False,
    expand_tasks: bool = False,
    start_time_from: int | None = None,
    start_time_to: int | None = None,
    limit: int = 25,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Lists all job runs with optional filtering by job ID, status, and time range.

    Use this tool to find job runs based on various criteria:
    - Filter by specific job ID
    - Filter by run status (active or completed)
    - Filter by start time range (milliseconds since epoch)

    Results include run IDs, states, timing, and links to the Databricks UI.

    The output is formatted in Markdown.

    Args:
        job_id: Optional job ID to filter runs for a specific job.
        active_only: If True, only returns currently running jobs (default: False).
        completed_only: If True, only returns completed jobs (default: False).
        expand_tasks: If True, includes task-level details in the output (default: False).
        start_time_from: Optional filter for runs started after this time (milliseconds since epoch).
        start_time_to: Optional filter for runs started before this time (milliseconds since epoch).
        limit: Maximum number of runs to return (default: 25). Auto-paginates if needed.
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    return await asyncio.to_thread(
        list_job_runs,
        job_id=job_id,
        active_only=active_only,
        completed_only=completed_only,
        expand_tasks=expand_tasks,
        start_time_from=start_time_from,
        start_time_to=start_time_to,
        max_results=limit,
        workspace=resolved_workspace,
    )


@mcp.tool()
@handle_tool_errors("export_databricks_task_run")
async def export_databricks_task_run(
    run_id: int,
    include_dashboards: bool = False,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Exports a task run as HTML, including notebook code and rendered outputs.

    Use this tool to get the full notebook content (code cells and their outputs) from
    a completed task run. This is useful for:
    - Viewing the actual code that was executed
    - Seeing the output/results of each cell
    - Debugging failed notebook runs
    - Auditing what a job actually did

    **Important**: For multi-task jobs, use the individual task's run_id (found in
    get_databricks_job_run under "Task Runs"), not the parent job run ID.

    The output is formatted in Markdown containing the HTML export.

    Args:
        run_id: The task run ID to export.
        include_dashboards: If True, also exports any dashboards (default: False).
        workspace: Optional workspace name. Uses session's active workspace if not specified.
    """
    resolved_workspace = _get_session_workspace(workspace=workspace, ctx=ctx)
    return await asyncio.to_thread(
        export_task_run,
        run_id=run_id,
        include_dashboards=include_dashboards,
        workspace=resolved_workspace,
    )


# ============================================================================
# Workspace management MCP tools
# ============================================================================


@mcp.tool()
@handle_tool_errors("list_databricks_workspaces")
async def list_databricks_workspaces() -> str:
    """
    Lists all configured Databricks workspaces.

    Use this tool to discover available workspaces for multi-workspace operations.
    Returns workspace names along with their host URLs and whether a SQL warehouse
    is configured for each.

    The output is formatted in Markdown.
    """
    configs = get_workspace_configs()
    if not configs:
        return "# Configured Workspaces\n\n*No workspaces configured.*"

    lines = ["# Configured Workspaces", ""]
    for name, config in sorted(configs.items()):
        warehouse_status = "configured" if config.sql_warehouse_id else "not configured"
        lines.append(f"## `{name}`")
        lines.append(f"- **Host**: `{config.host}`")
        lines.append(f"- **SQL Warehouse**: {warehouse_status}")
        lines.append("")
    return "\n".join(lines)


@mcp.tool()
@handle_tool_errors("get_databricks_active_workspace")
async def get_databricks_active_workspace(
    ctx: Optional[Context] = None,
) -> str:
    """
    Returns the currently active Databricks workspace for this MCP server instance.

    The active workspace is used as the default when no explicit workspace
    parameter is provided to other tools.

    Note: The active workspace is stored in the FastMCP lifespan context and
    is shared across all MCP client sessions connected to this server.

    The output is formatted in Markdown.
    """
    active = None
    if ctx is not None:
        try:
            lifespan_ctx = ctx.request_context.lifespan_context
            if isinstance(lifespan_ctx, DatabricksSessionContext):
                active = lifespan_ctx.active_workspace
        except (AttributeError, TypeError):
            pass

    if active:
        return f"# Active Workspace\n\n**Current**: `{active}`"
    return (
        "# Active Workspace\n\n"
        "*No active workspace set. Tools will fall back to the Databricks SDK's "
        "default workspace resolution (e.g., a 'default' workspace if configured).*"
    )


@mcp.tool()
@handle_tool_errors("set_databricks_active_workspace")
async def set_databricks_active_workspace(
    workspace: str,
    ctx: Optional[Context] = None,
) -> str:
    """
    Sets the active Databricks workspace for this MCP server instance.

    Once set, all subsequent tool calls that don't specify an explicit workspace
    parameter will use this workspace by default.

    Note: This changes the active workspace for all MCP client sessions
    connected to this server process.

    Use list_databricks_workspaces to see available workspaces.

    The output is formatted in Markdown.

    Args:
        workspace: The name of the workspace to set as active.
    """
    configs = get_workspace_configs()
    normalized = workspace.strip().lower()

    if not configs:
        raise ToolError(
            "No Databricks workspaces are configured. "
            "Set DATABRICKS_HOST/DATABRICKS_TOKEN or DATABRICKS_<NAME>_HOST/TOKEN "
            "environment variables before setting an active workspace."
        )

    if normalized not in configs:
        available = ", ".join(sorted(configs.keys()))
        raise ToolError(
            f"Workspace '{workspace}' not found. Available workspaces: {available}"
        )

    if ctx is None:
        raise ToolError("Session context not available. Cannot set active workspace.")

    try:
        lifespan_ctx = ctx.request_context.lifespan_context
        if not isinstance(lifespan_ctx, DatabricksSessionContext):
            raise ToolError("Session context is not a DatabricksSessionContext.")
        lifespan_ctx.active_workspace = normalized
    except (AttributeError, TypeError) as e:
        raise ToolError(f"Failed to access session context: {e}") from e

    return f"# Active Workspace Updated\n\n**Set to**: `{normalized}`"


def main():
    """Entry point for the Databricks MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
