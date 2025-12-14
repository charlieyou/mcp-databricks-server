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

### Single Workspace

Create a `.env` file:

```env
DATABRICKS_HOST="your-databricks-instance.cloud.databricks.com"
DATABRICKS_TOKEN="your-token"
DATABRICKS_SQL_WAREHOUSE_ID="your-warehouse-id"
```

### Multiple Workspaces

Configure multiple workspaces using named environment variables:

```env
# Default workspace (used when no workspace is specified)
DATABRICKS_HOST="prod.cloud.databricks.com"
DATABRICKS_TOKEN="prod-token"
DATABRICKS_SQL_WAREHOUSE_ID="prod-warehouse-id"

# Additional workspaces use the pattern DATABRICKS_{NAME}_{VAR}
DATABRICKS_DEV_HOST="dev.cloud.databricks.com"
DATABRICKS_DEV_TOKEN="dev-token"
DATABRICKS_DEV_SQL_WAREHOUSE_ID="dev-warehouse-id"

DATABRICKS_STAGING_HOST="staging.cloud.databricks.com"
DATABRICKS_STAGING_TOKEN="staging-token"
```

**Workspace Selection:**

1. **Per-request**: Pass `workspace="dev"` to any tool
2. **Per-session**: Use `set_databricks_active_workspace("dev")` to set the default for subsequent calls
3. **Fallback**: If only one workspace is configured, it's used automatically. Otherwise, the `default` workspace is used.

**Example Usage:**

```python
# List available workspaces
list_databricks_workspaces()

# Set dev as active for this session
set_databricks_active_workspace("dev")

# Now all calls use dev workspace
list_uc_catalogs()

# Override for a specific call
execute_sql_query("SELECT 1", workspace="prod")
```

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
