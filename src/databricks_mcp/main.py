import asyncio
import functools
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.fastmcp.server import Context

from .config import (
    DatabricksConfigError,
    execute_databricks_sql,
    get_workspace_configs,
)
from .databricks_formatter import format_query_results
from .jobs import (
    export_task_run,
    get_job,
    get_job_run,
    get_job_run_output,
    list_job_runs,
    list_jobs,
)
from .unity_catalog import (
    get_table_history,
    get_uc_all_catalogs_summary,
    get_uc_catalog_details,
    get_uc_schema_details,
    get_uc_table_details,
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


def _is_error_markdown(result: str) -> bool:
    """Check if a result string is an error markdown response.
    
    Error markdown starts with '# Error:' (ignoring leading whitespace).
    This convention allows SDK utilities to return structured error information
    while the MCP layer converts them to proper ToolError exceptions.
    """
    first_line = result.lstrip().split("\n", 1)[0].strip()
    return first_line.startswith("# Error:")


def _extract_error_from_markdown(result: str) -> tuple[str, str]:
    """Extract error title and details from error markdown."""
    stripped = result.lstrip()
    first_line, *rest = stripped.split("\n", 1)
    error_title = first_line.replace("# Error:", "").strip()
    error_details = rest[0].strip() if rest else ""
    return error_title, error_details


def handle_tool_errors(tool_name):
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            try:
                result = await fn(*args, **kwargs)
                # Detect "# Error:" prefix in Markdown responses and convert to ToolError
                # This ensures MCP clients can distinguish success vs failure
                if isinstance(result, str) and _is_error_markdown(result):
                    error_title, error_details = _extract_error_from_markdown(result)
                    raise ToolError(f"{tool_name}: {error_title}\n{error_details}")
                return result
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
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Provides detailed metadata about a specific Unity Catalog table.

    Use this tool when you know the exact three-part table name and need comprehensive information
    about its structure. The tool retrieves schema details (columns, types, nullability), table properties,
    storage location, and table statistics.

    Optionally, include lineage information to understand data dependencies—upstream and downstream tables,
    as well as notebooks and jobs that read from or write to this table. Lineage is useful for impact
    analysis and understanding how data flows through the data pipeline.

    The output is formatted in Markdown.

    Args:
        full_table_name: The fully qualified three-part name of the table (e.g., `catalog.schema.table`).
        include_lineage: Set to True to fetch and include comprehensive lineage (tables, notebooks, jobs).
                         Defaults to False. May take longer to retrieve but provides rich context for
                         understanding data dependencies and enabling code exploration.
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    workspace: Optional[str] = None,
    ctx: Optional[Context] = None,
) -> str:
    """
    Retrieves version history for a Delta table in Unity Catalog.

    Use this tool to see the change history of a table, including when changes occurred,
    who made them, and what operations were performed. Useful for auditing and debugging.

    Args:
        full_table_name: The fully qualified three-part name of the table (e.g., `catalog.schema.table`).
        limit: Maximum number of history records to return. Default is 10.
        start_timestamp: Optional. Filter to show only history after this timestamp (ISO format, e.g., '2024-01-01' or '2024-01-01T00:00:00').
        end_timestamp: Optional. Filter to show only history before this timestamp (ISO format).
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Retrieves a summary of all schemas within a specific Unity Catalog.

    Use this tool when you know the catalog name and need to discover the schemas within it.
    This is often a precursor to describing a specific schema or table.
    The output is formatted in Markdown.

    Args:
        catalog_name: The name of the Unity Catalog to describe (e.g., `prod`, `dev`, `system`).
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Retrieves detailed information about a specific schema within a Unity Catalog.

    This includes listing all tables in the schema with their names, types, and descriptions.
    If `include_columns=True`, column-level details are also provided for each table.
    If `include_columns=False`, only table names and descriptions are shown, useful for a quicker overview.
    The output is formatted in Markdown.

    Args:
        catalog_name: The name of the catalog containing the schema.
        schema_name: The name of the schema to describe.
        include_columns: If True, lists tables with their columns. Defaults to False for a briefer summary.
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Retrieves comprehensive details about a specific Databricks job by its ID.

    This includes:
    - Job name and creator
    - Schedule configuration (cron expression, timezone, pause status)
    - Task definitions (notebook, spark python, spark jar, SQL, dbt tasks)
    - Task dependencies and timeout settings
    - Job cluster configurations

    The output is formatted in Markdown.

    Args:
        job_id: The unique identifier of the job to retrieve.
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Lists Databricks jobs, optionally filtered by name.

    Returns a summary of all matching jobs including job ID, name, and creator.
    Use expand_tasks=True to see task details for each job.

    Args:
        name: Optional filter to find jobs whose names contain this string.
        expand_tasks: If True, includes task keys in the output (default: False).
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Retrieves details about a specific job run by its run ID.

    This includes:
    - Run state and result
    - Start and end times
    - Task-level execution details
    - Link to the Databricks UI

    The output is formatted in Markdown.

    Args:
        run_id: The unique identifier of the run to retrieve.
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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

    This is useful for debugging failed runs or examining the results of completed runs.
    The output includes notebook results, error messages, and any logged output.

    The output is formatted in Markdown.

    Args:
        run_id: The unique identifier of the run whose output to retrieve.
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Lists job runs with optional filtering.

    Can filter by job ID, status (active/completed), and time range. Results are sorted
    by start time (most recent first). Useful for monitoring and debugging job executions.

    Args:
        job_id: Optional filter to list runs for a specific job only.
        active_only: If True, only returns currently running jobs (default: False).
        completed_only: If True, only returns completed jobs (default: False).
        expand_tasks: If True, includes task-level details in the output (default: False).
        start_time_from: Optional filter for runs started after this time (milliseconds since epoch).
        start_time_to: Optional filter for runs started before this time (milliseconds since epoch).
        limit: Maximum number of runs to return (default: 25). Auto-paginates if needed.
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
    Exports the HTML output of a task run by its task run ID.

    Use this to view rendered notebook output. Note: This requires the task run ID (found via
    get_databricks_job_run under "Task Runs"), not the parent job run ID.

    The output is formatted in Markdown containing the HTML export.

    Args:
        run_id: The task run ID to export.
        include_dashboards: If True, also exports any dashboards (default: False).
        workspace: Target workspace name (e.g., 'prod', 'dev'). Required when working with multiple 
                   workspaces. Use `list_databricks_workspaces` to see available options. Falls back 
                   to the active workspace if not specified.
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
