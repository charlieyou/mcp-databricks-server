"""
Job and run operations and formatting.

NOTE: Do not import from databricks_sdk_utils here to avoid circular imports.
Only import from config (lower layer).
"""
import base64
import itertools
import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional
from urllib.parse import unquote

from databricks.sdk.service.jobs import (
    BaseJob,
    BaseRun,
    Run,
    RunState,
    ViewsToExport,
)

from .config import get_workspace_client

logger = logging.getLogger(__name__)


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
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")


def get_job(job_id: int, workspace: Optional[str] = None) -> str:
    """
    Fetches details for a specific job by job_id.
    Returns formatted Markdown.
    
    Args:
        job_id: The job ID.
        workspace: Optional workspace name. Uses default if not specified.
    """
    try:
        client = get_workspace_client(workspace)
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
    workspace: Optional[str] = None,
) -> str:
    """
    Lists jobs in the workspace.
    Returns formatted Markdown. Limited to max_results to prevent excessive output.
    
    Args:
        name: Optional filter for job name.
        expand_tasks: Whether to expand task details.
        max_results: Maximum number of jobs to return.
        workspace: Optional workspace name. Uses default if not specified.
    """
    try:
        client = get_workspace_client(workspace)
        jobs_iterator = client.jobs.list(
            name=name,
            expand_tasks=expand_tasks,
            limit=min(max_results, 25),
        )
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


def get_job_run(run_id: int, workspace: Optional[str] = None) -> str:
    """
    Fetches details for a specific job run by run_id.
    Returns formatted Markdown.
    
    Args:
        run_id: The run ID.
        workspace: Optional workspace name. Uses default if not specified.
    """
    try:
        client = get_workspace_client(workspace)
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


def get_job_run_output(run_id: int, workspace: Optional[str] = None) -> str:
    """
    Fetches the output of a specific job run by run_id.
    Returns formatted Markdown.
    
    Args:
        run_id: The run ID.
        workspace: Optional workspace name. Uses default if not specified.
    """
    try:
        client = get_workspace_client(workspace)
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
    workspace: Optional[str] = None,
) -> str:
    """
    Lists job runs with optional filtering.
    Returns formatted Markdown. The SDK auto-paginates to fetch up to max_results.
    
    Args:
        job_id: Optional job ID to filter runs.
        active_only: Only show active runs.
        completed_only: Only show completed runs.
        expand_tasks: Include task details.
        start_time_from: Filter runs started after this timestamp (ms).
        start_time_to: Filter runs started before this timestamp (ms).
        max_results: Maximum number of runs to return.
        workspace: Optional workspace name. Uses default if not specified.
    """
    try:
        if max_results <= 0:
            max_results = 1
        client = get_workspace_client(workspace)
        runs_iterator = client.jobs.list_runs(
            job_id=job_id,
            active_only=active_only,
            completed_only=completed_only,
            expand_tasks=expand_tasks,
            start_time_from=start_time_from,
            start_time_to=start_time_to,
        )
        raw_runs = list(itertools.islice(runs_iterator, max_results + 1))
        has_more = len(raw_runs) > max_results
        runs_list = raw_runs[:max_results]

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
                # Handle both dict and string items in data array
                if isinstance(item, str):
                    if item.strip():
                        output_lines.append(item.strip())
                    continue
                if not isinstance(item, dict):
                    continue
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


def export_task_run(
    run_id: int,
    include_dashboards: bool = False,
    workspace: Optional[str] = None,
) -> str:
    """
    Exports a task run as Markdown, including notebook code and outputs.
    Returns formatted Markdown with code cells and their results.

    Args:
        run_id: The task run ID (for multi-task jobs, use the individual task's run_id).
        include_dashboards: If True, exports dashboards in addition to notebooks.
        workspace: Optional workspace name. Uses default if not specified.
    """
    try:
        client = get_workspace_client(workspace)
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
