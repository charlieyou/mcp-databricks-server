import itertools
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    SchemaInfo,
    TableInfo,
)
from databricks.sdk.service.jobs import (
    BaseJob,
    BaseRun,
    Run,
    RunState,
    ViewsToExport,
)
from databricks.sdk.service.sql import (
    StatementParameterListItem,
    StatementResponse,
    StatementState,
)
from dotenv import load_dotenv

# Load environment variables from .env file when the module is imported
load_dotenv()

logger = logging.getLogger(__name__)


class DatabricksConfigError(RuntimeError):
    """Raised when Databricks configuration is missing or invalid."""

    pass


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
DATABRICKS_SQL_WAREHOUSE_ID = os.environ.get("DATABRICKS_SQL_WAREHOUSE_ID")

_sdk_client: Optional[WorkspaceClient] = None


def get_sdk_client() -> WorkspaceClient:
    """
    Lazily initializes and returns the Databricks WorkspaceClient.
    Raises DatabricksConfigError if configuration is missing.
    """
    global _sdk_client
    if _sdk_client is not None:
        return _sdk_client

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    if not host or not token:
        raise DatabricksConfigError(
            "DATABRICKS_HOST and DATABRICKS_TOKEN must be set in environment variables or .env file "
            "for databricks_sdk_utils to initialize."
        )

    # Configure and initialize the global SDK client
    # Using short timeouts as previously determined to be effective
    sdk_config = Config(
        host=host, token=token, http_timeout_seconds=30, retry_timeout_seconds=60
    )
    _sdk_client = WorkspaceClient(config=sdk_config)
    return _sdk_client


# Cache for job information to avoid redundant API calls
_job_cache = {}
_notebook_cache = {}


def _format_column_details_md(columns: List[ColumnInfo]) -> List[str]:
    """
    Formats a list of ColumnInfo objects into a list of Markdown strings.
    """
    markdown_lines = []
    if not columns:
        markdown_lines.append("  - *No column information available.*")
        return markdown_lines

    for col in columns:
        if not isinstance(col, ColumnInfo):
            logger.warning(
                f"Warning: Encountered an unexpected item in columns list: {type(col)}. Skipping."
            )
            continue
        col_type = col.type_text or (
            col.type_name.value
            if col.type_name and hasattr(col.type_name, "value")
            else "N/A"
        )
        nullable_status = "nullable" if col.nullable else "not nullable"
        col_description = f": {col.comment}" if col.comment else ""
        markdown_lines.append(
            f"  - **{col.name}** (`{col_type}`, {nullable_status}){col_description}"
        )
    return markdown_lines


def _get_job_info_cached(job_id: str) -> Dict[str, Any]:
    """Get job information with caching to avoid redundant API calls"""
    if job_id not in _job_cache:
        try:
            client = get_sdk_client()
            job_info = client.jobs.get(job_id=job_id)
            _job_cache[job_id] = {
                "name": job_info.settings.name
                if job_info.settings.name
                else f"Job {job_id}",
                "tasks": [],
            }

            # Pre-process all tasks to build notebook mapping
            if job_info.settings.tasks:
                for task in job_info.settings.tasks:
                    if hasattr(task, "notebook_task") and task.notebook_task:
                        task_info = {
                            "task_key": task.task_key,
                            "notebook_path": task.notebook_task.notebook_path,
                        }
                        _job_cache[job_id]["tasks"].append(task_info)

        except Exception as e:
            logger.error(f"Error fetching job {job_id}: {e}")
            _job_cache[job_id] = {"name": f"Job {job_id}", "tasks": [], "error": str(e)}

    return _job_cache[job_id]


def _get_notebook_id_cached(notebook_path: str) -> str:
    """Get notebook ID with caching to avoid redundant API calls"""
    if notebook_path not in _notebook_cache:
        try:
            client = get_sdk_client()
            notebook_details = client.workspace.get_status(notebook_path)
            _notebook_cache[notebook_path] = str(notebook_details.object_id)
        except Exception as e:
            logger.error(f"Error fetching notebook {notebook_path}: {e}")
            _notebook_cache[notebook_path] = None

    return _notebook_cache[notebook_path]


def _resolve_notebook_info_optimized(notebook_id: str, job_id: str) -> Dict[str, Any]:
    """
    Optimized version that resolves notebook info using cached job data.
    Returns dict with notebook_path, notebook_name, job_name, and task_key.
    """
    result = {
        "notebook_id": notebook_id,
        "notebook_path": f"notebook_id:{notebook_id}",
        "notebook_name": f"notebook_id:{notebook_id}",
        "job_id": job_id,
        "job_name": f"Job {job_id}",
        "task_key": None,
    }

    # Get cached job info
    job_info = _get_job_info_cached(job_id)
    result["job_name"] = job_info["name"]

    # Look for notebook in job tasks
    for task_info in job_info["tasks"]:
        notebook_path = task_info["notebook_path"]
        cached_notebook_id = _get_notebook_id_cached(notebook_path)

        if cached_notebook_id == notebook_id:
            result["notebook_path"] = notebook_path
            result["notebook_name"] = notebook_path.split("/")[-1]
            result["task_key"] = task_info["task_key"]
            break

    return result


def _format_notebook_info_optimized(notebook_info: Dict[str, Any]) -> str:
    """
    Formats notebook information using pre-resolved data.
    """
    lines = []

    if notebook_info["notebook_path"].startswith("/"):
        lines.append(f"**`{notebook_info['notebook_name']}`**")
        lines.append(f"  - **Path**: `{notebook_info['notebook_path']}`")
    else:
        lines.append(f"**{notebook_info['notebook_name']}**")

    lines.append(
        f"  - **Job**: {notebook_info['job_name']} (ID: {notebook_info['job_id']})"
    )
    if notebook_info["task_key"]:
        lines.append(f"  - **Task**: {notebook_info['task_key']}")

    return "\n".join(lines)


def _process_lineage_results(
    lineage_query_output: Dict[str, Any], main_table_full_name: str
) -> Dict[str, Any]:
    """
    Optimized version of lineage processing that batches API calls and uses caching.
    """
    logger.info("Processing lineage results with optimization...")
    start_time = time.time()

    processed_data: Dict[str, Any] = {
        "upstream_tables": [],
        "downstream_tables": [],
        "notebooks_reading": [],
        "notebooks_writing": [],
    }

    if (
        not lineage_query_output
        or lineage_query_output.get("status") != "success"
        or not isinstance(lineage_query_output.get("data"), list)
    ):
        logger.warning(
            "Warning: Lineage query output is invalid or not successful. Returning empty lineage."
        )
        return processed_data

    upstream_set = set()
    downstream_set = set()
    notebooks_reading_dict = {}
    notebooks_writing_dict = {}

    # Collect all unique job IDs first for batch processing
    unique_job_ids = set()
    notebook_job_pairs = []

    for row in lineage_query_output["data"]:
        source_table = row.get("source_table_full_name")
        target_table = row.get("target_table_full_name")
        entity_metadata = row.get("entity_metadata")

        # Parse entity metadata
        notebook_id = None
        job_id = None

        if entity_metadata:
            try:
                if isinstance(entity_metadata, str):
                    metadata_dict = json.loads(entity_metadata)
                else:
                    metadata_dict = entity_metadata

                notebook_id = metadata_dict.get("notebook_id")
                job_info = metadata_dict.get("job_info")
                if job_info:
                    job_id = job_info.get("job_id")
            except (json.JSONDecodeError, AttributeError):
                pass

        # Process table-to-table lineage
        if (
            source_table == main_table_full_name
            and target_table
            and target_table != main_table_full_name
        ):
            downstream_set.add(target_table)
        elif (
            target_table == main_table_full_name
            and source_table
            and source_table != main_table_full_name
        ):
            upstream_set.add(source_table)

        # Collect notebook-job pairs for batch processing
        if notebook_id and job_id:
            unique_job_ids.add(job_id)
            notebook_job_pairs.append(
                {
                    "notebook_id": notebook_id,
                    "job_id": job_id,
                    "source_table": source_table,
                    "target_table": target_table,
                }
            )

    # Pre-load all job information in parallel (this is where the optimization happens)
    logger.info(f"Pre-loading {len(unique_job_ids)} unique jobs...")
    batch_start = time.time()

    for job_id in unique_job_ids:
        _get_job_info_cached(job_id)  # This will cache the job info

    batch_time = time.time() - batch_start
    logger.info(f"Job batch loading took {batch_time:.2f} seconds")

    # Now process all notebook-job pairs using cached data
    logger.info(f"Processing {len(notebook_job_pairs)} notebook entries...")
    for pair in notebook_job_pairs:
        notebook_info = _resolve_notebook_info_optimized(
            pair["notebook_id"], pair["job_id"]
        )
        formatted_info = _format_notebook_info_optimized(notebook_info)

        if pair["source_table"] == main_table_full_name:
            notebooks_reading_dict[pair["notebook_id"]] = formatted_info
        elif pair["target_table"] == main_table_full_name:
            notebooks_writing_dict[pair["notebook_id"]] = formatted_info

    processed_data["upstream_tables"] = sorted(list(upstream_set))
    processed_data["downstream_tables"] = sorted(list(downstream_set))
    processed_data["notebooks_reading"] = sorted(list(notebooks_reading_dict.values()))
    processed_data["notebooks_writing"] = sorted(list(notebooks_writing_dict.values()))

    total_time = time.time() - start_time
    logger.info(f"Total lineage processing took {total_time:.2f} seconds")

    return processed_data


def clear_lineage_cache():
    """Clear the job and notebook caches to free memory"""
    global _job_cache, _notebook_cache
    _job_cache = {}
    _notebook_cache = {}
    logger.info("Cleared lineage caches")


def _validate_table_name(table_full_name: str) -> str:
    """
    Validates and quotes a fully qualified table name to prevent SQL injection.
    Returns the quoted table name (e.g., `catalog`.`schema`.`table`).
    Raises ValueError if the table name is invalid.
    """
    parts = table_full_name.split(".")
    if len(parts) != 3 or any(not p for p in parts):
        raise ValueError(
            f"Invalid table name '{table_full_name}'. Expected format: catalog.schema.table"
        )

    safe_parts = []
    for p in parts:
        if not re.match(r"^[A-Za-z0-9_]+$", p):
            raise ValueError(
                f"Invalid identifier '{p}' in '{table_full_name}'. "
                "Only letters, numbers, and underscores are allowed."
            )
        safe_parts.append(f"`{p}`")

    return ".".join(safe_parts)


def get_table_history(
    table_full_name: str,
    limit: int = 10,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
) -> str:
    """
    Retrieves Delta table history using DESCRIBE HISTORY SQL command.
    Returns formatted Markdown with version history.

    Args:
        table_full_name: Fully qualified table name (catalog.schema.table)
        limit: Maximum number of history records to return (default 10, max 1000)
        start_timestamp: Filter history after this timestamp (ISO format)
        end_timestamp: Filter history before this timestamp (ISO format)
    """
    if not DATABRICKS_SQL_WAREHOUSE_ID:
        return "# Error: Table History\n\n`DATABRICKS_SQL_WAREHOUSE_ID` is not set. Cannot fetch history."

    # Validate table name to prevent SQL injection
    try:
        quoted_table_name = _validate_table_name(table_full_name)
    except ValueError as e:
        return f"# Error: Table History\n\n{e}"

    # Validate and clamp limit
    if limit is None or limit <= 0:
        limit = 10
    if limit > 1000:
        limit = 1000

    # Build parameterized query for timestamp filters
    parameters: List[StatementParameterListItem] = []
    where_clauses: List[str] = []

    if start_timestamp:
        where_clauses.append("timestamp >= :start_ts")
        parameters.append(
            StatementParameterListItem(
                name="start_ts", value=start_timestamp, type="STRING"
            )
        )
    if end_timestamp:
        where_clauses.append("timestamp <= :end_ts")
        parameters.append(
            StatementParameterListItem(
                name="end_ts", value=end_timestamp, type="STRING"
            )
        )

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    history_sql_query = f"""
    SELECT version, timestamp, operation, userName, operationParameters, operationMetrics
    FROM (DESCRIBE HISTORY {quoted_table_name})
    {where_sql}
    ORDER BY version DESC
    LIMIT {limit}
    """
    logger.info(f"Fetching history for table: {table_full_name}")

    result = execute_databricks_sql(
        history_sql_query,
        wait_timeout="30s",
        parameters=parameters if parameters else None,
    )

    markdown_parts = [f"# Table History: `{table_full_name}`", ""]

    # Handle error and failed states
    status = result.get("status")
    if status in ("error", "failed"):
        error_msg = result.get("error", "Unknown error")
        details = result.get("details")
        markdown_parts.append(f"**Error**: {error_msg}")
        if details:
            markdown_parts.extend(["", "```", str(details), "```"])
        return "\n".join(markdown_parts)

    if status != "success":
        markdown_parts.append(
            f"**Error**: Unexpected status `{status}` from history query."
        )
        return "\n".join(markdown_parts)

    data = result.get("data") or []
    if not data:
        markdown_parts.append("*No history records found.*")
        return "\n".join(markdown_parts)

    filters = []
    if start_timestamp:
        filters.append(f"after {start_timestamp}")
    if end_timestamp:
        filters.append(f"before {end_timestamp}")
    if filters:
        markdown_parts.append(f"**Filters**: {', '.join(filters)}")
        markdown_parts.append("")

    markdown_parts.append(f"**Showing**: {len(data)} records (limit: {limit})")
    markdown_parts.append("")

    markdown_parts.append("| Version | Timestamp | Operation | User |")
    markdown_parts.append("|---------|-----------|-----------|------|")

    for row in data:
        version = row.get("version", "N/A")
        timestamp = row.get("timestamp", "N/A")
        operation = row.get("operation", "N/A")
        user_name = row.get("userName", "N/A")
        markdown_parts.append(
            f"| {version} | {timestamp} | {operation} | {user_name} |"
        )

    return "\n".join(markdown_parts)


def _get_table_lineage(table_full_name: str) -> Dict[str, Any]:
    """
    Retrieves table lineage information for a given table using the global SDK client
    and global SQL warehouse ID. Now includes notebook and job information with enhanced details.
    """
    if not DATABRICKS_SQL_WAREHOUSE_ID:  # Check before attempting query
        return {
            "status": "error",
            "error": "DATABRICKS_SQL_WAREHOUSE_ID is not set. Cannot fetch lineage.",
        }

    lineage_sql_query = """
    SELECT source_table_full_name, target_table_full_name, entity_type, entity_id, 
           entity_run_id, entity_metadata, created_by, event_time
    FROM system.access.table_lineage
    WHERE source_table_full_name = :table_name OR target_table_full_name = :table_name
    ORDER BY event_time DESC LIMIT 100;
    """
    logger.info(f"Fetching and processing lineage for table: {table_full_name}")

    parameters = [
        StatementParameterListItem(
            name="table_name", value=table_full_name, type="STRING"
        )
    ]

    # execute_databricks_sql will now use the global warehouse_id
    raw_lineage_output = execute_databricks_sql(
        lineage_sql_query, wait_timeout="50s", parameters=parameters
    )
    return _process_lineage_results(raw_lineage_output, table_full_name)


def _format_single_table_md(
    table_info: TableInfo, base_heading_level: int, display_columns: bool
) -> List[str]:
    """
    Formats the details for a single TableInfo object into a list of Markdown strings.
    Uses a base_heading_level to control Markdown header depth for hierarchical display.
    """
    table_markdown_parts = []
    table_header_prefix = "#" * base_heading_level
    sub_header_prefix = "#" * (base_heading_level + 1)

    table_markdown_parts.append(
        f"{table_header_prefix} Table: **{table_info.full_name}**"
    )

    if table_info.comment:
        table_markdown_parts.extend(["", f"**Description**: {table_info.comment}"])
    elif base_heading_level == 1:
        table_markdown_parts.extend(["", "**Description**: No description provided."])

    # Process and add partition columns
    partition_column_names: List[str] = []
    if table_info.columns:
        temp_partition_cols: List[tuple[str, int]] = []
        for col in table_info.columns:
            if col.partition_index is not None:
                temp_partition_cols.append((col.name, col.partition_index))
        if temp_partition_cols:
            temp_partition_cols.sort(key=lambda x: x[1])
            partition_column_names = [name for name, index in temp_partition_cols]

    if partition_column_names:
        table_markdown_parts.extend(["", f"{sub_header_prefix} Partition Columns"])
        table_markdown_parts.extend(
            [f"- `{col_name}`" for col_name in partition_column_names]
        )
    elif base_heading_level == 1:
        table_markdown_parts.extend(
            [
                "",
                f"{sub_header_prefix} Partition Columns",
                "- *This table is not partitioned or partition key information is unavailable.*",
            ]
        )

    if display_columns:
        table_markdown_parts.extend(["", f"{sub_header_prefix} Table Columns"])
        if table_info.columns:
            table_markdown_parts.extend(_format_column_details_md(table_info.columns))
        else:
            table_markdown_parts.append("  - *No column information available.*")

    return table_markdown_parts


def execute_databricks_sql(
    sql_query: str,
    wait_timeout: str = "50s",
    parameters: Optional[List[StatementParameterListItem]] = None,
) -> Dict[str, Any]:
    """
    Executes a SQL query on Databricks using the global SDK client and global SQL warehouse ID.
    """
    if not DATABRICKS_SQL_WAREHOUSE_ID:
        return {
            "status": "error",
            "error": "DATABRICKS_SQL_WAREHOUSE_ID is not set. Cannot execute SQL query.",
        }

    try:
        logger.info(
            f"Executing SQL on warehouse {DATABRICKS_SQL_WAREHOUSE_ID} (timeout: {wait_timeout}), length={len(sql_query)}"
        )
        client = get_sdk_client()
        response: StatementResponse = client.statement_execution.execute_statement(
            statement=sql_query,
            warehouse_id=DATABRICKS_SQL_WAREHOUSE_ID,  # Use global warehouse ID
            wait_timeout=wait_timeout,
            parameters=parameters,
        )

        if response.status and response.status.state == StatementState.SUCCEEDED:
            if response.result and response.result.data_array:
                column_names = (
                    [col.name for col in response.manifest.schema.columns]
                    if response.manifest
                    and response.manifest.schema
                    and response.manifest.schema.columns
                    else []
                )
                results = [
                    dict(zip(column_names, row)) for row in response.result.data_array
                ]
                return {"status": "success", "row_count": len(results), "data": results}
            else:
                return {
                    "status": "success",
                    "row_count": 0,
                    "data": [],
                    "message": "Query succeeded but returned no data.",
                }
        elif response.status and response.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            return {
                "status": "error",
                "error": f"Query timed out after {wait_timeout}. The query is still running on the server.",
            }
        elif response.status:
            error_message = (
                response.status.error.message
                if response.status.error
                else "No error details provided."
            )
            return {
                "status": "failed",
                "error": f"Query execution failed with state: {response.status.state.value}",
                "details": error_message,
            }
        else:
            return {"status": "failed", "error": "Query execution status unknown."}
    except Exception as e:
        return {
            "status": "error",
            "error": f"An error occurred during SQL execution: {str(e)}",
        }


def get_uc_table_details(full_table_name: str, include_lineage: bool = False) -> str:
    """
    Fetches table metadata and optionally lineage, then formats it into a Markdown string.
    Uses the _format_single_table_md helper for core table structure.
    """
    logger.info(f"Fetching metadata for {full_table_name}...")

    try:
        client = get_sdk_client()
        table_info: TableInfo = client.tables.get(full_name=full_table_name)
    except Exception as e:
        error_details = str(e)
        return f"""# Error: Could Not Retrieve Table Details
**Table:** `{full_table_name}`
**Problem:** Failed to fetch the complete metadata for this table.
**Details:**
```
{error_details}
```"""

    markdown_parts = _format_single_table_md(
        table_info, base_heading_level=1, display_columns=True
    )

    if include_lineage:
        markdown_parts.extend(["", "## Lineage Information"])
        if not DATABRICKS_SQL_WAREHOUSE_ID:
            markdown_parts.append(
                "- *Lineage fetching skipped: `DATABRICKS_SQL_WAREHOUSE_ID` environment variable is not set.*"
            )
        else:
            logger.info(f"Fetching lineage for {full_table_name}...")
            lineage_info = _get_table_lineage(full_table_name)

            has_upstream = (
                lineage_info
                and isinstance(lineage_info.get("upstream_tables"), list)
                and lineage_info["upstream_tables"]
            )
            has_downstream = (
                lineage_info
                and isinstance(lineage_info.get("downstream_tables"), list)
                and lineage_info["downstream_tables"]
            )
            has_notebooks_reading = (
                lineage_info
                and isinstance(lineage_info.get("notebooks_reading"), list)
                and lineage_info["notebooks_reading"]
            )
            has_notebooks_writing = (
                lineage_info
                and isinstance(lineage_info.get("notebooks_writing"), list)
                and lineage_info["notebooks_writing"]
            )

            if has_upstream:
                markdown_parts.extend(
                    ["", "### Upstream Tables (tables this table reads from):"]
                )
                markdown_parts.extend(
                    [f"- `{table}`" for table in lineage_info["upstream_tables"]]
                )

            if has_downstream:
                markdown_parts.extend(
                    ["", "### Downstream Tables (tables that read from this table):"]
                )
                markdown_parts.extend(
                    [f"- `{table}`" for table in lineage_info["downstream_tables"]]
                )

            if has_notebooks_reading:
                markdown_parts.extend(["", "### Notebooks Reading from this Table:"])
                for notebook in lineage_info["notebooks_reading"]:
                    markdown_parts.extend([f"- {notebook}", ""])

            if has_notebooks_writing:
                markdown_parts.extend(["", "### Notebooks Writing to this Table:"])
                for notebook in lineage_info["notebooks_writing"]:
                    markdown_parts.extend([f"- {notebook}", ""])

            if not any(
                [
                    has_upstream,
                    has_downstream,
                    has_notebooks_reading,
                    has_notebooks_writing,
                ]
            ):
                if (
                    lineage_info
                    and lineage_info.get("status") == "error"
                    and lineage_info.get("error")
                ):
                    markdown_parts.extend(
                        [
                            "",
                            "*Note: Could not retrieve complete lineage information.*",
                            f"> *Lineage fetch error: {lineage_info.get('error')}*",
                        ]
                    )
                elif (
                    lineage_info
                    and lineage_info.get("status") != "success"
                    and lineage_info.get("error")
                ):
                    markdown_parts.extend(
                        [
                            "",
                            "*Note: Could not retrieve complete lineage information.*",
                            f"> *Lineage fetch error: {lineage_info.get('error')}*",
                        ]
                    )
                else:
                    markdown_parts.append(
                        "- *No table, notebook, or job dependencies found or lineage fetch was not fully successful.*"
                    )
    else:
        markdown_parts.extend(
            [
                "",
                "## Lineage Information",
                "- *Lineage fetching skipped as per request.*",
            ]
        )

    return "\n".join(markdown_parts)


def get_uc_schema_details(
    catalog_name: str, schema_name: str, include_columns: bool = False
) -> str:
    """
    Fetches detailed information for a specific schema, optionally including its tables and their columns.
    Uses the global SDK client and the _format_single_table_md helper with appropriate heading levels.
    """
    full_schema_name = f"{catalog_name}.{schema_name}"
    markdown_parts = [f"# Schema Details: **{full_schema_name}**"]

    try:
        logger.info(f"Fetching details for schema: {full_schema_name}...")
        client = get_sdk_client()
        schema_info: SchemaInfo = client.schemas.get(full_name=full_schema_name)

        description = (
            schema_info.comment if schema_info.comment else "No description provided."
        )
        markdown_parts.append(f"**Description**: {description}")
        markdown_parts.append("")

        markdown_parts.append(f"## Tables in Schema `{schema_name}`")

        tables_iterable = client.tables.list(
            catalog_name=catalog_name, schema_name=schema_name
        )
        tables_list = list(tables_iterable)

        if not tables_list:
            markdown_parts.append("- *No tables found in this schema.*")
        else:
            for i, table_info in enumerate(tables_list):
                if not isinstance(table_info, TableInfo):
                    logger.warning(
                        f"Warning: Encountered an unexpected item in tables list: {type(table_info)}"
                    )
                    continue

                markdown_parts.extend(
                    _format_single_table_md(
                        table_info,
                        base_heading_level=3,
                        display_columns=include_columns,
                    )
                )
                if i < len(tables_list) - 1:
                    markdown_parts.append("\n=============\n")
                else:
                    markdown_parts.append("")

    except Exception as e:
        error_message = (
            f"Failed to retrieve details for schema '{full_schema_name}': {str(e)}"
        )
        logger.error(f"Error in get_uc_schema_details: {error_message}")
        return f"""# Error: Could Not Retrieve Schema Details
**Schema:** `{full_schema_name}`
**Problem:** An error occurred while attempting to fetch schema information.
**Details:**
```
{error_message}
```"""

    return "\n".join(markdown_parts)


def get_uc_catalog_details(catalog_name: str) -> str:
    """
    Fetches and formats a summary of all schemas within a given catalog
    using the global SDK client.
    """
    markdown_parts = [f"# Catalog Summary: **{catalog_name}**", ""]
    schemas_found_count = 0

    try:
        logger.info(
            f"Fetching schemas for catalog: {catalog_name} using global sdk_client..."
        )
        # The sdk_client is globally defined in this module
        client = get_sdk_client()
        schemas_iterable = client.schemas.list(catalog_name=catalog_name)

        # Convert iterator to list to easily check if empty and get a count
        schemas_list = list(schemas_iterable)

        if not schemas_list:
            markdown_parts.append(f"No schemas found in catalog `{catalog_name}`.")
            return "\n".join(markdown_parts)

        schemas_found_count = len(schemas_list)
        markdown_parts.append(
            f"Showing top {schemas_found_count} schemas found in catalog `{catalog_name}`:"
        )
        markdown_parts.append("")

        for i, schema_info in enumerate(schemas_list):
            if not isinstance(schema_info, SchemaInfo):
                logger.warning(
                    f"Warning: Encountered an unexpected item in schemas list: {type(schema_info)}"
                )
                continue

            # Start of a schema item in the list
            schema_name_display = (
                schema_info.full_name if schema_info.full_name else "Unnamed Schema"
            )
            markdown_parts.append(
                f"## {schema_name_display}"
            )  # Main bullet point for schema name

            description = (
                f"**Description**: {schema_info.comment}" if schema_info.comment else ""
            )
            markdown_parts.append(description)

            markdown_parts.append(
                ""
            )  # Add a blank line for separation between schemas, or remove if too much space

    except Exception as e:
        error_message = (
            f"Failed to retrieve schemas for catalog '{catalog_name}': {str(e)}"
        )
        logger.error(f"Error in get_catalog_summary: {error_message}")
        # Return a structured error message in Markdown
        return f"""# Error: Could Not Retrieve Catalog Summary
**Catalog:** `{catalog_name}`
**Problem:** An error occurred while attempting to fetch schema information.
**Details:**
```
{error_message}
```"""

    markdown_parts.append(
        f"**Total Schemas Found in `{catalog_name}`**: {schemas_found_count}"
    )
    return "\n".join(markdown_parts)


def get_uc_all_catalogs_summary() -> str:
    """
    Fetches a summary of all available Unity Catalogs, including their names, comments, and types.
    Uses the global SDK client.
    """
    markdown_parts = ["# Available Unity Catalogs", ""]
    catalogs_found_count = 0

    try:
        logger.info("Fetching all catalogs using global sdk_client...")
        client = get_sdk_client()
        catalogs_iterable = client.catalogs.list()
        catalogs_list = list(catalogs_iterable)

        if not catalogs_list:
            markdown_parts.append("- *No catalogs found or accessible.*")
            return "\n".join(markdown_parts)

        catalogs_found_count = len(catalogs_list)
        markdown_parts.append(f"Found {catalogs_found_count} catalog(s):")
        markdown_parts.append("")

        for catalog_info in catalogs_list:
            if not isinstance(catalog_info, CatalogInfo):
                logger.warning(
                    f"Warning: Encountered an unexpected item in catalogs list: {type(catalog_info)}"
                )
                continue

            markdown_parts.append(f"- **`{catalog_info.name}`**")
            description = (
                catalog_info.comment
                if catalog_info.comment
                else "No description provided."
            )
            markdown_parts.append(f"  - **Description**: {description}")

            catalog_type_str = "N/A"
            if catalog_info.catalog_type and hasattr(
                catalog_info.catalog_type, "value"
            ):
                catalog_type_str = catalog_info.catalog_type.value
            elif (
                catalog_info.catalog_type
            ):  # Fallback if it's not an Enum but has a direct string representation
                catalog_type_str = str(catalog_info.catalog_type)
            markdown_parts.append(f"  - **Type**: `{catalog_type_str}`")

            markdown_parts.append("")  # Add a blank line for separation

    except Exception as e:
        error_message = f"Failed to retrieve catalog list: {str(e)}"
        logger.error(f"Error in get_uc_all_catalogs_summary: {error_message}")
        return f"""# Error: Could Not Retrieve Catalog List
**Problem:** An error occurred while attempting to fetch the list of catalogs.
**Details:**
```
{error_message}
```"""

    return "\n".join(markdown_parts)


# ============================================================================
# Job-related utility functions
# ============================================================================


def _format_run_state_md(state: Optional[RunState], max_message_len: int = 200) -> str:
    """Format a run state object into a readable string."""
    if not state:
        return "Unknown"
    parts = []
    if state.life_cycle_state:
        parts.append(f"**{state.life_cycle_state.value}**")
    if state.result_state:
        parts.append(f"Result: {state.result_state.value}")
    if state.state_message:
        msg = state.state_message
        if len(msg) > max_message_len:
            msg = msg[:max_message_len] + "..."
        parts.append(f"({msg})")
    return " - ".join(parts) if parts else "Unknown"


def _format_timestamp(ts_ms: Optional[int]) -> str:
    """Format a millisecond timestamp into a readable date string."""
    if not ts_ms:
        return "N/A"
    from datetime import datetime

    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def get_job(job_id: int) -> str:
    """
    Fetches details for a specific job by job_id.
    Returns formatted Markdown.
    """
    try:
        client = get_sdk_client()
        job = client.jobs.get(job_id=job_id)

        markdown_parts = [
            f"# Job: **{job.settings.name if job.settings else 'Unnamed'}**",
            "",
        ]
        markdown_parts.append(f"**Job ID**: `{job.job_id}`")

        if job.creator_user_name:
            markdown_parts.append(f"**Created by**: {job.creator_user_name}")
        if job.created_time:
            markdown_parts.append(
                f"**Created at**: {_format_timestamp(job.created_time)}"
            )

        if job.settings:
            settings = job.settings
            if settings.description:
                markdown_parts.extend(["", f"**Description**: {settings.description}"])

            if settings.schedule:
                schedule = settings.schedule
                markdown_parts.extend(["", "## Schedule"])
                if schedule.quartz_cron_expression:
                    markdown_parts.append(
                        f"- **Cron**: `{schedule.quartz_cron_expression}`"
                    )
                if schedule.timezone_id:
                    markdown_parts.append(f"- **Timezone**: {schedule.timezone_id}")
                if schedule.pause_status:
                    markdown_parts.append(
                        f"- **Status**: {schedule.pause_status.value}"
                    )

            if settings.max_concurrent_runs:
                markdown_parts.append(
                    f"**Max concurrent runs**: {settings.max_concurrent_runs}"
                )

            if settings.tasks:
                markdown_parts.extend(["", "## Tasks"])
                for task in settings.tasks:
                    markdown_parts.append(f"### Task: `{task.task_key}`")
                    if task.description:
                        markdown_parts.append(f"- **Description**: {task.description}")
                    if task.notebook_task:
                        markdown_parts.append("- **Type**: Notebook")
                        markdown_parts.append(
                            f"- **Path**: `{task.notebook_task.notebook_path}`"
                        )
                    elif task.spark_python_task:
                        markdown_parts.append("- **Type**: Spark Python")
                        markdown_parts.append(
                            f"- **File**: `{task.spark_python_task.python_file}`"
                        )
                    elif task.spark_jar_task:
                        markdown_parts.append("- **Type**: Spark JAR")
                        if task.spark_jar_task.main_class_name:
                            markdown_parts.append(
                                f"- **Main class**: `{task.spark_jar_task.main_class_name}`"
                            )
                    elif task.sql_task:
                        markdown_parts.append("- **Type**: SQL")
                    elif task.dbt_task:
                        markdown_parts.append("- **Type**: dbt")
                    elif task.python_wheel_task:
                        markdown_parts.append("- **Type**: Python Wheel")
                    if task.depends_on:
                        deps = [d.task_key for d in task.depends_on]
                        markdown_parts.append(f"- **Depends on**: {', '.join(deps)}")
                    if task.timeout_seconds:
                        markdown_parts.append(f"- **Timeout**: {task.timeout_seconds}s")
                    markdown_parts.append("")

            if settings.job_clusters:
                markdown_parts.extend(["## Job Clusters"])
                for cluster in settings.job_clusters:
                    markdown_parts.append(f"- **{cluster.job_cluster_key}**")

        return "\n".join(markdown_parts)

    except Exception as e:
        return f"""# Error: Could Not Retrieve Job Details
**Job ID:** `{job_id}`
**Details:**
```
{str(e)}
```"""


def list_jobs(
    name: Optional[str] = None,
    expand_tasks: bool = False,
    max_results: int = 100,
) -> str:
    """
    Lists jobs in the workspace.
    Returns formatted Markdown. Limited to max_results to prevent excessive output.
    """
    try:
        client = get_sdk_client()
        jobs_iterator = client.jobs.list(
            name=name,
            expand_tasks=expand_tasks,
            limit=min(max_results, 25),  # Databricks API max is 25
        )
        # Use islice to prevent auto-pagination beyond limit
        jobs_list = list(itertools.islice(jobs_iterator, max_results))
        has_more = len(jobs_list) >= max_results

        markdown_parts = ["# Jobs List", ""]

        if name:
            markdown_parts.append(f"**Filter**: name contains `{name}`")
        if has_more:
            markdown_parts.append(
                f"**Showing**: first {len(jobs_list)} jobs (more available)"
            )
        else:
            markdown_parts.append(f"**Total**: {len(jobs_list)} jobs")
        markdown_parts.append("")

        if not jobs_list:
            markdown_parts.append("*No jobs found.*")
            return "\n".join(markdown_parts)

        for job in jobs_list:
            if not isinstance(job, BaseJob):
                continue
            job_name = (
                job.settings.name if job.settings and job.settings.name else "Unnamed"
            )
            markdown_parts.append(f"## `{job_name}` (ID: {job.job_id})")
            if job.creator_user_name:
                markdown_parts.append(f"- **Created by**: {job.creator_user_name}")
            if job.created_time:
                markdown_parts.append(
                    f"- **Created**: {_format_timestamp(job.created_time)}"
                )
            if expand_tasks and job.settings and job.settings.tasks:
                task_keys = [t.task_key for t in job.settings.tasks]
                markdown_parts.append(f"- **Tasks**: {', '.join(task_keys)}")
            markdown_parts.append("")

        return "\n".join(markdown_parts)

    except Exception as e:
        return f"""# Error: Could Not List Jobs
**Details:**
```
{str(e)}
```"""


def get_job_run(run_id: int) -> str:
    """
    Fetches details for a specific job run by run_id.
    Returns formatted Markdown.
    """
    try:
        client = get_sdk_client()
        run: Run = client.jobs.get_run(run_id=run_id)

        run_name = run.run_name if run.run_name else f"Run {run.run_id}"
        markdown_parts = [f"# Run: **{run_name}**", ""]

        markdown_parts.append(f"**Run ID**: `{run.run_id}`")
        markdown_parts.append(f"**Job ID**: `{run.job_id}`")
        if run.number_in_job:
            markdown_parts.append(f"**Run number**: #{run.number_in_job}")

        markdown_parts.extend(["", "## State"])
        markdown_parts.append(f"**Status**: {_format_run_state_md(run.state)}")

        markdown_parts.extend(["", "## Timing"])
        markdown_parts.append(f"- **Started**: {_format_timestamp(run.start_time)}")
        markdown_parts.append(f"- **Ended**: {_format_timestamp(run.end_time)}")
        if run.execution_duration:
            duration_sec = run.execution_duration / 1000
            markdown_parts.append(f"- **Duration**: {duration_sec:.1f}s")

        if run.trigger:
            markdown_parts.extend(["", f"**Trigger**: {run.trigger.value}"])

        if run.run_page_url:
            markdown_parts.extend(
                ["", f"**[View in Databricks UI]({run.run_page_url})**"]
            )

        if run.tasks:
            markdown_parts.extend(["", "## Task Runs"])
            for task_run in run.tasks:
                markdown_parts.append(f"### Task: `{task_run.task_key}`")
                markdown_parts.append(f"- **Run ID**: {task_run.run_id}")
                markdown_parts.append(
                    f"- **Status**: {_format_run_state_md(task_run.state)}"
                )
                markdown_parts.append(
                    f"- **Started**: {_format_timestamp(task_run.start_time)}"
                )
                markdown_parts.append(
                    f"- **Ended**: {_format_timestamp(task_run.end_time)}"
                )
                if task_run.attempt_number:
                    markdown_parts.append(f"- **Attempt**: {task_run.attempt_number}")
                markdown_parts.append("")

        if run.cluster_instance:
            markdown_parts.extend(["", "## Cluster"])
            if run.cluster_instance.cluster_id:
                markdown_parts.append(
                    f"- **Cluster ID**: `{run.cluster_instance.cluster_id}`"
                )
            if run.cluster_instance.spark_context_id:
                markdown_parts.append(
                    f"- **Spark Context**: `{run.cluster_instance.spark_context_id}`"
                )

        return "\n".join(markdown_parts)

    except Exception as e:
        return f"""# Error: Could Not Retrieve Run Details
**Run ID:** `{run_id}`
**Details:**
```
{str(e)}
```"""


def get_job_run_output(run_id: int) -> str:
    """
    Fetches the output of a specific job run by run_id.
    Returns formatted Markdown.
    """
    try:
        client = get_sdk_client()
        output = client.jobs.get_run_output(run_id=run_id)

        markdown_parts = [f"# Run Output (Run ID: {run_id})", ""]

        if output.metadata:
            meta = output.metadata
            markdown_parts.append("## Run Metadata")
            markdown_parts.append(f"- **Job ID**: {meta.job_id}")
            markdown_parts.append(f"- **Run ID**: {meta.run_id}")
            markdown_parts.append(f"- **Status**: {_format_run_state_md(meta.state)}")
            markdown_parts.append(
                f"- **Started**: {_format_timestamp(meta.start_time)}"
            )
            markdown_parts.append(f"- **Ended**: {_format_timestamp(meta.end_time)}")
            markdown_parts.append("")

        if output.notebook_output:
            markdown_parts.extend(["## Notebook Output"])
            if output.notebook_output.result:
                markdown_parts.append("```")
                markdown_parts.append(output.notebook_output.result)
                markdown_parts.append("```")
            if output.notebook_output.truncated:
                markdown_parts.append("*Output was truncated.*")
            markdown_parts.append("")

        if output.sql_output:
            markdown_parts.extend(["## SQL Output"])
            sql_out = output.sql_output
            if sql_out.output_link:
                markdown_parts.append(f"**[View Output]({sql_out.output_link})**")
            markdown_parts.append("")

        if output.dbt_output:
            markdown_parts.extend(["## dbt Output"])
            dbt_out = output.dbt_output
            if dbt_out.artifacts_link:
                markdown_parts.append(f"**[View Artifacts]({dbt_out.artifacts_link})**")
            markdown_parts.append("")

        if output.logs:
            markdown_parts.extend(["## Logs"])
            markdown_parts.append("```")
            markdown_parts.append(output.logs)
            markdown_parts.append("```")
            if output.logs_truncated:
                markdown_parts.append("*Logs were truncated.*")

        if output.error:
            markdown_parts.extend(["## Error"])
            markdown_parts.append(f"```\n{output.error}\n```")

        if output.error_trace:
            markdown_parts.extend(["## Error Trace"])
            markdown_parts.append(f"```\n{output.error_trace}\n```")

        return "\n".join(markdown_parts)

    except Exception as e:
        return f"""# Error: Could Not Retrieve Run Output
**Run ID:** `{run_id}`
**Details:**
```
{str(e)}
```"""


def list_job_runs(
    job_id: Optional[int] = None,
    active_only: bool = False,
    completed_only: bool = False,
    expand_tasks: bool = False,
    start_time_from: Optional[int] = None,
    start_time_to: Optional[int] = None,
    max_results: int = 25,
) -> str:
    """
    Lists job runs with optional filtering.
    Returns formatted Markdown. The SDK auto-paginates to fetch up to max_results.
    """
    try:
        if max_results <= 0:
            max_results = 1
        client = get_sdk_client()
        runs_iterator = client.jobs.list_runs(
            job_id=job_id,
            active_only=active_only,
            completed_only=completed_only,
            expand_tasks=expand_tasks,
            start_time_from=start_time_from,
            start_time_to=start_time_to,
        )
        # Fetch one extra to detect if more exist
        raw_runs = list(itertools.islice(runs_iterator, max_results + 1))
        has_more = len(raw_runs) > max_results
        runs_list = raw_runs[:max_results]

        # SDK's active_only filter is unreliable - filter client-side if needed
        if active_only:
            runs_list = [
                r
                for r in runs_list
                if r.state
                and r.state.life_cycle_state
                and r.state.life_cycle_state.value
                in ("PENDING", "RUNNING", "TERMINATING")
            ]

        markdown_parts = ["# Job Runs", ""]

        filters = []
        if job_id:
            filters.append(f"job_id={job_id}")
        if active_only:
            filters.append("active_only=true")
        if completed_only:
            filters.append("completed_only=true")
        if start_time_from:
            filters.append(f"start_time_from={_format_timestamp(start_time_from)}")
        if start_time_to:
            filters.append(f"start_time_to={_format_timestamp(start_time_to)}")

        if filters:
            markdown_parts.append(f"**Filters**: {', '.join(filters)}")
        if has_more:
            markdown_parts.append(
                f"**Showing**: first {len(runs_list)} runs (more available)"
            )
        else:
            markdown_parts.append(f"**Total**: {len(runs_list)} runs")
        markdown_parts.append("")

        if not runs_list:
            markdown_parts.append("*No runs found.*")
            return "\n".join(markdown_parts)

        for run in runs_list:
            if not isinstance(run, BaseRun):
                continue
            run_name = run.run_name if run.run_name else f"Run {run.run_id}"
            markdown_parts.append(f"## {run_name}")
            markdown_parts.append(f"- **Run ID**: `{run.run_id}`")
            markdown_parts.append(f"- **Job ID**: `{run.job_id}`")
            markdown_parts.append(f"- **Status**: {_format_run_state_md(run.state)}")
            markdown_parts.append(f"- **Started**: {_format_timestamp(run.start_time)}")
            markdown_parts.append(f"- **Ended**: {_format_timestamp(run.end_time)}")
            if run.run_page_url:
                markdown_parts.append(f"- **[View in UI]({run.run_page_url})**")

            if expand_tasks and run.tasks:
                markdown_parts.append("- **Tasks**:")
                for task in run.tasks:
                    status = _format_run_state_md(task.state)
                    markdown_parts.append(f"  - `{task.task_key}`: {status}")

            markdown_parts.append("")

        return "\n".join(markdown_parts)

    except Exception as e:
        return f"""# Error: Could Not List Runs
**Details:**
```
{str(e)}
```"""


def _parse_notebook_html(html_content: str) -> Optional[Dict[str, Any]]:
    """
    Parse Databricks notebook HTML export and extract the embedded JSON data.
    Returns the notebook data dict or None if parsing fails.
    """
    import base64
    import re
    from urllib.parse import unquote

    match = re.search(r"= '([A-Za-z0-9+/=]{100,})'", html_content)
    if not match:
        return None

    try:
        data = match.group(1)
        decoded = base64.b64decode(data).decode("utf-8")
        decoded = unquote(decoded)
        return json.loads(decoded)
    except Exception:
        return None


def _format_notebook_as_markdown(notebook: Dict[str, Any]) -> str:
    """
    Format a parsed Databricks notebook as clean Markdown with code and outputs.
    """
    name = notebook.get("name", "Untitled")
    language = notebook.get("language", "python")
    commands = notebook.get("commands", [])

    parts = [f"# Notebook: {name}", f"**Language**: {language}", ""]

    for i, cmd in enumerate(commands, 1):
        code = cmd.get("command", "").strip()
        state = cmd.get("state", "")

        if not code:
            continue

        parts.append(f"## Cell {i}")
        if state == "error":
            parts.append("**Status**: ❌ Error")
        parts.append("")
        parts.append(f"```{language}")
        parts.append(code)
        parts.append("```")

        results = cmd.get("results")
        if results and results.get("data"):
            output_lines = []
            for item in results["data"]:
                item_type = item.get("type", "")
                item_data = item.get("data", "")

                if item_type == "ansi" and item_data:
                    output_lines.append(str(item_data).strip())

            if output_lines:
                combined_output = "\n".join(output_lines)
                if len(combined_output) > 2000:
                    combined_output = combined_output[:2000] + "\n... (truncated)"
                parts.append("")
                parts.append("**Output:**")
                parts.append("```")
                parts.append(combined_output)
                parts.append("```")

        error_summary = cmd.get("errorSummary")
        error_details = cmd.get("error")
        if error_summary or error_details:
            parts.append("")
            parts.append("**Error:**")
            parts.append("```")
            if error_summary:
                parts.append(error_summary)
            if error_details:
                parts.append(error_details)
            parts.append("```")

        parts.append("")

    return "\n".join(parts)


def export_task_run(run_id: int, include_dashboards: bool = False) -> str:
    """
    Exports a task run as Markdown, including notebook code and outputs.
    Returns formatted Markdown with code cells and their results.

    Args:
        run_id: The task run ID (for multi-task jobs, use the individual task's run_id).
        include_dashboards: If True, exports dashboards in addition to notebooks.
    """
    try:
        client = get_sdk_client()
        views_to_export = (
            ViewsToExport.ALL if include_dashboards else ViewsToExport.CODE
        )

        export_result = client.jobs.export_run(
            run_id=run_id,
            views_to_export=views_to_export,
        )

        if not export_result.views:
            return f"# Exported Task Run (Run ID: {run_id})\n\n*No views available for this run.*"

        markdown_parts = []

        for view in export_result.views:
            if not view.content:
                continue

            notebook = _parse_notebook_html(view.content)
            if notebook:
                markdown_parts.append(_format_notebook_as_markdown(notebook))
            else:
                view_name = view.name if view.name else "Unnamed View"
                markdown_parts.append(
                    f"# {view_name}\n\n*Could not parse notebook content.*"
                )

        if not markdown_parts:
            return f"# Exported Task Run (Run ID: {run_id})\n\n*No parseable content found.*"

        return "\n\n---\n\n".join(markdown_parts)

    except Exception as e:
        return f"""# Error: Could Not Export Task Run
**Run ID:** `{run_id}`
**Details:**
```
{str(e)}
```"""
