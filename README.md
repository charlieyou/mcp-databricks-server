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

## Setup

```bash
uv pip install -e .
```

Create a `.env` file:

```env
DATABRICKS_HOST="your-databricks-instance.cloud.databricks.com"
DATABRICKS_TOKEN="your-token"
DATABRICKS_SQL_WAREHOUSE_ID="your-warehouse-id"
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
