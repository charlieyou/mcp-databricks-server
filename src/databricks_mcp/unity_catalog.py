"""
Unity Catalog operations and formatting.

NOTE: Do not import from databricks_sdk_utils here to avoid circular imports.
Only import from config and lineage (lower layers).
"""
import logging
import re
from typing import List, Optional

from databricks.sdk.service.catalog import (
    CatalogInfo,
    ColumnInfo,
    SchemaInfo,
    TableInfo,
)
from databricks.sdk.service.sql import StatementParameterListItem

from .config import (
    execute_databricks_sql,
    get_sql_warehouse_id,
    get_workspace_client,
)
from .lineage import _get_table_lineage

logger = logging.getLogger(__name__)


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
    workspace: Optional[str] = None,
) -> str:
    """
    Retrieves Delta table history using DESCRIBE HISTORY SQL command.
    Returns formatted Markdown with version history.

    Args:
        table_full_name: Fully qualified table name (catalog.schema.table)
        limit: Maximum number of history records to return (default 10, max 1000)
        start_timestamp: Filter history after this timestamp (ISO format)
        end_timestamp: Filter history before this timestamp (ISO format)
        workspace: Optional workspace name. Uses default if not specified.
    """
    warehouse_id = get_sql_warehouse_id(workspace)
    if not warehouse_id:
        return "# Error: Table History\n\nSQL warehouse ID is not configured for this workspace. Cannot fetch history."

    try:
        quoted_table_name = _validate_table_name(table_full_name)
    except ValueError as e:
        return f"# Error: Table History\n\n{e}"

    if limit is None or limit <= 0:
        limit = 10
    if limit > 1000:
        limit = 1000

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
    SELECT version, timestamp, operation, userName, job, operationParameters, operationMetrics
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
        workspace=workspace,
    )

    markdown_parts = [f"# Table History: `{table_full_name}`", ""]

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

    markdown_parts.append("| Version | Timestamp | Operation | User | Job | Parameters | Metrics |")
    markdown_parts.append("|---------|-----------|-----------|------|-----|------------|---------|")

    for row in data:
        version = row.get("version", "N/A")
        timestamp = row.get("timestamp", "N/A")
        operation = row.get("operation", "N/A")
        user_name = row.get("userName", "N/A")
        job = row.get("job", "")
        op_params = row.get("operationParameters", "")
        op_metrics = row.get("operationMetrics", "")
        job_str = str(job).replace("|", "\\|") if job else "-"
        op_params_str = str(op_params).replace("|", "\\|") if op_params else "-"
        op_metrics_str = str(op_metrics).replace("|", "\\|") if op_metrics else "-"
        markdown_parts.append(
            f"| {version} | {timestamp} | {operation} | {user_name} | {job_str} | {op_params_str} | {op_metrics_str} |"
        )

    return "\n".join(markdown_parts)


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


def get_uc_table_details(
    full_table_name: str,
    include_lineage: bool = False,
    workspace: Optional[str] = None,
) -> str:
    """
    Fetches table metadata and optionally lineage, then formats it into a Markdown string.
    Uses the _format_single_table_md helper for core table structure.
    
    Args:
        full_table_name: Fully qualified table name (catalog.schema.table).
        include_lineage: Whether to include lineage information.
        workspace: Optional workspace name. Uses default if not specified.
    """
    logger.info(f"Fetching metadata for {full_table_name}...")

    try:
        client = get_workspace_client(workspace)
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
        warehouse_id = get_sql_warehouse_id(workspace)
        if not warehouse_id:
            markdown_parts.append(
                "- *Lineage fetching skipped: SQL warehouse ID is not configured for this workspace.*"
            )
        else:
            logger.info(f"Fetching lineage for {full_table_name}...")
            lineage_info = _get_table_lineage(full_table_name, workspace=workspace)

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
    catalog_name: str,
    schema_name: str,
    include_columns: bool = False,
    workspace: Optional[str] = None,
) -> str:
    """
    Fetches detailed information for a specific schema, optionally including its tables and their columns.
    
    Args:
        catalog_name: The catalog name.
        schema_name: The schema name.
        include_columns: Whether to include column details for tables.
        workspace: Optional workspace name. Uses default if not specified.
    """
    full_schema_name = f"{catalog_name}.{schema_name}"
    markdown_parts = [f"# Schema Details: **{full_schema_name}**"]

    try:
        logger.info(f"Fetching details for schema: {full_schema_name}...")
        client = get_workspace_client(workspace)
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


def get_uc_catalog_details(catalog_name: str, workspace: Optional[str] = None) -> str:
    """
    Fetches and formats a summary of all schemas within a given catalog.
    
    Args:
        catalog_name: The catalog name.
        workspace: Optional workspace name. Uses default if not specified.
    """
    markdown_parts = [f"# Catalog Summary: **{catalog_name}**", ""]
    schemas_found_count = 0

    try:
        logger.info(
            f"Fetching schemas for catalog: {catalog_name}..."
        )
        client = get_workspace_client(workspace)
        schemas_iterable = client.schemas.list(catalog_name=catalog_name)

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

            schema_name_display = (
                schema_info.full_name if schema_info.full_name else "Unnamed Schema"
            )
            markdown_parts.append(
                f"## {schema_name_display}"
            )

            description = (
                f"**Description**: {schema_info.comment}" if schema_info.comment else ""
            )
            markdown_parts.append(description)

            markdown_parts.append(
                ""
            )

    except Exception as e:
        error_message = (
            f"Failed to retrieve details for catalog '{catalog_name}': {str(e)}"
        )
        logger.error(f"Error in get_uc_catalog_details: {error_message}")
        return f"""# Error: Could Not Retrieve Catalog Details
**Catalog:** `{catalog_name}`
**Problem:** An error occurred while attempting to fetch catalog information.
**Details:**
```
{error_message}
```"""

    markdown_parts.append(
        f"**Total Schemas Found in `{catalog_name}`**: {schemas_found_count}"
    )
    return "\n".join(markdown_parts)


def get_uc_all_catalogs_summary(workspace: Optional[str] = None) -> str:
    """
    Fetches a summary of all available Unity Catalogs, including their names, comments, and types.
    
    Args:
        workspace: Optional workspace name. Uses default if not specified.
    """
    markdown_parts = ["# Available Unity Catalogs", ""]
    catalogs_found_count = 0

    try:
        logger.info("Fetching all catalogs...")
        client = get_workspace_client(workspace)
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
            ):
                catalog_type_str = str(catalog_info.catalog_type)
            markdown_parts.append(f"  - **Type**: `{catalog_type_str}`")

            markdown_parts.append("")

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
