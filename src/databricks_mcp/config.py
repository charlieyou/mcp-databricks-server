"""
Workspace configuration, client caching, and SQL execution.

NOTE: This is the base module. Other modules depend on it
to avoid circular imports. Other modules may import from this module.
"""
import configparser
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    StatementParameterListItem,
    StatementResponse,
    StatementState,
)

logger = logging.getLogger(__name__)


class DatabricksConfigError(RuntimeError):
    """Raised when Databricks configuration is missing or invalid."""

    pass


@dataclass(frozen=True)
class WorkspaceConfig:
    """Configuration for a Databricks workspace."""
    name: str
    host: Optional[str] = None
    token: Optional[str] = None
    sql_warehouse_id: Optional[str] = None
    profile: Optional[str] = None


def _get_databrickscfg_path() -> Path:
    """Get the path to .databrickscfg file."""
    return Path.home() / ".databrickscfg"


def _load_workspace_configs_from_profiles() -> Dict[str, WorkspaceConfig]:
    """
    Load workspace configurations from ~/.databrickscfg profiles.
    
    Each profile section becomes a workspace with the profile name as the workspace name.
    The DEFAULT section is mapped to 'default' workspace name.
    
    Uses RawConfigParser to avoid interpolation issues with tokens containing %.
    Does not require token - lets SDK handle auth validation for different auth methods.
    
    Returns dict of workspace name -> WorkspaceConfig.
    """
    configs: Dict[str, WorkspaceConfig] = {}
    
    cfg_path = _get_databrickscfg_path()
    if not cfg_path.exists():
        logger.warning(f"Databricks config file not found: {cfg_path}")
        return configs
    
    parser = configparser.RawConfigParser()
    try:
        parser.read(cfg_path)
    except configparser.Error as e:
        logger.error(f"Error parsing {cfg_path}: {e}")
        return configs
    
    defaults = parser.defaults()
    if defaults.get("host"):
        configs["default"] = WorkspaceConfig(
            name="default",
            host=defaults.get("host"),
            token=defaults.get("token"),
            sql_warehouse_id=defaults.get("sql_warehouse_id"),
            profile="DEFAULT",
        )
    
    for section in parser.sections():
        items = dict(parser.items(section, raw=True))
        host = items.get("host")
        if not host:
            continue
        
        workspace_name = section.lower()
        
        configs[workspace_name] = WorkspaceConfig(
            name=workspace_name,
            host=host,
            token=items.get("token"),
            sql_warehouse_id=items.get("sql_warehouse_id"),
            profile=section,
        )
    
    return configs


_workspace_configs: Dict[str, WorkspaceConfig] = _load_workspace_configs_from_profiles()


def reload_workspace_configs() -> None:
    """
    Reload workspace configurations from .databrickscfg.
    
    Useful for testing when the config file changes after module import.
    Also clears cached workspace clients.
    """
    global _workspace_configs, _workspace_clients
    _workspace_configs = _load_workspace_configs_from_profiles()
    with _workspace_clients_lock:
        _workspace_clients = {}


def get_workspace_configs() -> Dict[str, WorkspaceConfig]:
    """
    Returns a copy of the current workspace configurations.
    
    Used by MCP tools to list available workspaces.
    """
    return dict(_workspace_configs)


def _resolve_workspace_name(workspace: Optional[str] = None) -> str:
    """
    Resolve workspace name with priority:
    1. Explicit param if provided (case-insensitive, whitespace ignored)
    2. 'default' if exists
    3. Single workspace if only one configured
    4. Error if ambiguous or none configured
    
    Raises DatabricksConfigError on failure.
    """
    if not _workspace_configs:
        cfg_path = _get_databrickscfg_path()
        raise DatabricksConfigError(
            f"No Databricks profiles configured. Create {cfg_path}."
        )
    
    if workspace is not None:
        normalized = workspace.strip().lower()
        if not normalized:
            raise DatabricksConfigError(
                "Workspace name cannot be empty. Provide a valid workspace name."
            )
        if normalized not in _workspace_configs:
            available = ", ".join(sorted(_workspace_configs.keys()))
            raise DatabricksConfigError(
                f"Workspace '{workspace}' not found. Available workspaces: {available}"
            )
        return normalized
    
    if "default" in _workspace_configs:
        return "default"
    
    if len(_workspace_configs) == 1:
        return next(iter(_workspace_configs.keys()))
    
    available = ", ".join(sorted(_workspace_configs.keys()))
    raise DatabricksConfigError(
        f"Multiple workspaces configured but none specified. "
        f"Available workspaces: {available}"
    )


_workspace_clients: Dict[str, WorkspaceClient] = {}
_workspace_clients_lock = threading.Lock()


def get_workspace_client(workspace: Optional[str] = None) -> WorkspaceClient:
    """
    Get a WorkspaceClient for the specified workspace.
    
    Uses the profile from .databrickscfg for authentication (supports PAT, OAuth, etc.).
    Lazily creates and caches clients per workspace name.
    Uses _resolve_workspace_name to determine which workspace to use.
    Uses double-checked locking to minimize lock hold time.
    """
    resolved_name = _resolve_workspace_name(workspace)
    
    with _workspace_clients_lock:
        existing = _workspace_clients.get(resolved_name)
        if existing is not None:
            return existing
    
    from databricks.sdk.config import Config as SdkConfig
    
    ws_config = _workspace_configs[resolved_name]
    
    if not ws_config.sql_warehouse_id:
        logger.warning(
            f"Workspace '{resolved_name}' has no sql_warehouse_id configured. "
            "SQL queries will fail. Add sql_warehouse_id to your .databrickscfg profile."
        )
    
    logger.debug(
        f"Creating WorkspaceClient for workspace '{resolved_name}' (host={ws_config.host})"
    )
    
    new_client = WorkspaceClient(
        config=SdkConfig(
            host=ws_config.host,
            token=ws_config.token,
            profile=ws_config.profile,
            http_timeout_seconds=30,
            retry_timeout_seconds=60,
        )
    )
    
    with _workspace_clients_lock:
        existing = _workspace_clients.get(resolved_name)
        if existing is not None:
            return existing
        _workspace_clients[resolved_name] = new_client
        return new_client


def get_sql_warehouse_id(workspace: Optional[str] = None) -> Optional[str]:
    """
    Get the SQL warehouse ID for a workspace.
    
    Returns None if no warehouse ID is configured for the workspace.
    """
    resolved_name = _resolve_workspace_name(workspace)
    return _workspace_configs[resolved_name].sql_warehouse_id


def get_sdk_client(workspace: Optional[str] = None) -> WorkspaceClient:
    """
    Lazily initializes and returns the Databricks WorkspaceClient.
    
    Backward compatibility alias for get_workspace_client().
    Delegates to get_workspace_client() which uses the default workspace.
    
    Args:
        workspace: Optional workspace name. If None, uses default workspace.
    """
    return get_workspace_client(workspace)


def execute_databricks_sql(
    sql_query: str,
    wait_timeout: str = "50s",
    parameters: Optional[List[StatementParameterListItem]] = None,
    workspace: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Executes a SQL query on Databricks using the specified workspace.
    
    Args:
        sql_query: The SQL query to execute.
        wait_timeout: Timeout for the query execution.
        parameters: Optional list of query parameters.
        workspace: Optional workspace name. Uses default if not specified.
    """
    warehouse_id = get_sql_warehouse_id(workspace)
    if not warehouse_id:
        return {
            "status": "error",
            "error": "SQL warehouse ID is not configured for this workspace. Cannot execute SQL query.",
        }

    try:
        logger.info(
            f"Executing SQL on warehouse {warehouse_id} (timeout: {wait_timeout}), length={len(sql_query)}"
        )
        client = get_workspace_client(workspace)
        response: StatementResponse = client.statement_execution.execute_statement(
            statement=sql_query,
            warehouse_id=warehouse_id,
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
