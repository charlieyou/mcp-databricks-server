# Databricks MCP Server

MCP server for Databricks Unity Catalog. Enables LLM agents to explore catalogs, schemas, tables, lineage, and execute SQL queries.

## Tools

**Unity Catalog:**
- `list_uc_catalogs()` - List all Unity Catalogs
- `describe_uc_catalog(catalog_name)` - List schemas in a catalog
- `describe_uc_schema(catalog_name, schema_name, include_columns=False)` - Describe tables in a schema
- `describe_uc_table(full_table_name, include_lineage=False)` - Describe table structure and lineage
- `execute_sql_query(sql)` - Execute SQL queries

**Jobs:**
- `list_databricks_jobs(name=None, expand_tasks=False)` - List jobs in the workspace
- `get_databricks_job(job_id)` - Get job details
- `list_databricks_job_runs(job_id=None, active_only=False, completed_only=False, ...)` - List job runs
- `get_databricks_job_run(run_id)` - Get run details
- `get_databricks_job_run_output(run_id)` - Get run output and logs

**Workspace Management:**
- `list_databricks_workspaces()` - List all configured workspaces
- `get_databricks_active_workspace()` - Get the active workspace for the session
- `set_databricks_active_workspace(workspace)` - Set the active workspace for the session

## Setup

```bash
uv pip install -e .
```

### Configuration

Configure workspaces using `~/.databrickscfg` (or set `DATABRICKS_CONFIG_FILE` to use an alternative path):

```ini
[DEFAULT]
host = https://prod.cloud.databricks.com
token = your-prod-token
sql_warehouse_id = your-warehouse-id

[DEV]
host = https://dev.cloud.databricks.com
token = your-dev-token
sql_warehouse_id = dev-warehouse-id
```

The `DEFAULT` section maps to the `default` workspace name. Named sections (e.g., `[DEV]`) become lowercase workspace names (e.g., `dev`).

**Note:** This server uses `.databrickscfg` for all authentication. Pure environment variable setups (`DATABRICKS_HOST`/`DATABRICKS_TOKEN` without a config file) are not supported. The SDK's profile-based auth supports PAT tokens, OAuth, Azure CLI, and other authentication methods.

## Running

```bash
databricks-mcp-server
```

### Cursor Configuration

Add to `~/.cursor/mcp.json`:

```json
{
    "mcpServers": {
        "databricks": {
            "command": "uv",
            "args": ["--directory", "/path/to/mcp-databricks-server", "run", "databricks-mcp-server"]
        }
    }
}
```

## Testing

```bash
uv run pytest
```
