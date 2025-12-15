import json
import logging
import threading
import time
from typing import Any, Dict, Optional

from databricks.sdk.service.sql import StatementParameterListItem

from .config import (
    _resolve_workspace_name,
    execute_databricks_sql,
    get_sdk_client,
    get_sql_warehouse_id,
)

logger = logging.getLogger(__name__)

_job_cache: Dict[tuple[str, str], Dict[str, Any]] = {}
_job_cache_lock = threading.Lock()
_notebook_cache: Dict[tuple[str, str], Optional[str]] = {}
_notebook_cache_lock = threading.Lock()


def _get_job_info_cached(job_id: str, workspace: Optional[str] = None) -> Dict[str, Any]:
    """Get job information with caching to avoid redundant API calls.
    
    Uses double-checked locking to prevent overwriting existing cache entries.
    """
    workspace_name = _resolve_workspace_name(workspace)
    cache_key = (workspace_name, job_id)
    
    with _job_cache_lock:
        existing = _job_cache.get(cache_key)
        if existing is not None:
            return existing
    
    try:
        client = get_sdk_client(workspace_name)
        job_info = client.jobs.get(job_id=job_id)
        result: Dict[str, Any] = {
            "name": job_info.settings.name
            if job_info.settings.name
            else f"Job {job_id}",
            "tasks": [],
        }

        if job_info.settings.tasks:
            for task in job_info.settings.tasks:
                if hasattr(task, "notebook_task") and task.notebook_task:
                    task_info = {
                        "task_key": task.task_key,
                        "notebook_path": task.notebook_task.notebook_path,
                    }
                    result["tasks"].append(task_info)

    except Exception as e:
        logger.error(f"Error fetching job {job_id}: {e}")
        result = {"name": f"Job {job_id}", "tasks": [], "error": str(e)}

    with _job_cache_lock:
        existing = _job_cache.get(cache_key)
        if existing is not None:
            return existing
        _job_cache[cache_key] = result
        return result


def _get_notebook_id_cached(notebook_path: str, workspace: Optional[str] = None) -> Optional[str]:
    """Get notebook ID with caching to avoid redundant API calls.
    
    Uses double-checked locking to prevent overwriting existing cache entries.
    """
    workspace_name = _resolve_workspace_name(workspace)
    cache_key = (workspace_name, notebook_path)
    
    with _notebook_cache_lock:
        existing = _notebook_cache.get(cache_key)
        if existing is not None:
            return existing
    
    try:
        client = get_sdk_client(workspace_name)
        notebook_details = client.workspace.get_status(notebook_path)
        result: Optional[str] = str(notebook_details.object_id)
    except Exception as e:
        logger.error(f"Error fetching notebook {notebook_path}: {e}")
        result = None

    with _notebook_cache_lock:
        existing = _notebook_cache.get(cache_key)
        if existing is not None:
            return existing
        _notebook_cache[cache_key] = result
        return result


def _resolve_notebook_info_optimized(notebook_id: str, job_id: str, workspace: Optional[str] = None) -> Dict[str, Any]:
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

    job_info = _get_job_info_cached(job_id, workspace)
    result["job_name"] = job_info["name"]

    for task_info in job_info["tasks"]:
        notebook_path = task_info["notebook_path"]
        cached_notebook_id = _get_notebook_id_cached(notebook_path, workspace)

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
    lineage_query_output: Dict[str, Any],
    main_table_full_name: str,
    workspace: Optional[str] = None,
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

    unique_job_ids = set()
    notebook_job_pairs = []

    for row in lineage_query_output["data"]:
        source_table = row.get("source_table_full_name")
        target_table = row.get("target_table_full_name")
        entity_metadata = row.get("entity_metadata")

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

    logger.info(f"Pre-loading {len(unique_job_ids)} unique jobs...")
    batch_start = time.time()

    for job_id in unique_job_ids:
        _get_job_info_cached(job_id, workspace)

    batch_time = time.time() - batch_start
    logger.info(f"Job batch loading took {batch_time:.2f} seconds")

    logger.info(f"Processing {len(notebook_job_pairs)} notebook entries...")
    for pair in notebook_job_pairs:
        notebook_info = _resolve_notebook_info_optimized(
            pair["notebook_id"], pair["job_id"], workspace
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


def clear_lineage_cache() -> None:
    """Clear the job and notebook caches to free memory"""
    global _job_cache, _notebook_cache
    with _job_cache_lock:
        _job_cache = {}
    with _notebook_cache_lock:
        _notebook_cache = {}
    logger.info("Cleared lineage caches")


def _get_table_lineage(table_full_name: str, workspace: Optional[str] = None) -> Dict[str, Any]:
    """
    Retrieves table lineage information for a given table using the specified workspace.
    Includes notebook and job information with enhanced details.
    """
    warehouse_id = get_sql_warehouse_id(workspace)
    if not warehouse_id:
        return {
            "status": "error",
            "error": "SQL warehouse ID is not configured for this workspace. Cannot fetch lineage.",
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

    raw_lineage_output = execute_databricks_sql(
        lineage_sql_query, wait_timeout="50s", parameters=parameters, workspace=workspace
    )
    return _process_lineage_results(raw_lineage_output, table_full_name, workspace=workspace)
