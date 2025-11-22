# Architecture

## Project Structure

```
mcp-databricks-server/
├── src/                          # Source code
│   └── databricks_mcp/          # Main package
│       ├── __init__.py          # Package initialization & exports
│       ├── main.py              # MCP server entry point & tools
│       ├── databricks_sdk_utils.py  # Databricks SDK wrapper functions
│       └── databricks_formatter.py  # Query result formatting
├── test/                         # Test suite
│   ├── conftest.py              # Shared test fixtures
│   ├── test_main.py             # Tests for MCP tools
│   ├── test_databricks_sdk_utils.py  # Tests for SDK utilities
│   └── test_databricks_formatter.py  # Tests for formatter
├── docs/                         # Documentation
│   ├── ARCHITECTURE.md          # This file
│   └── TESTING.md               # Testing documentation
├── assets/                       # Images and assets
├── pyproject.toml               # Project configuration
├── uv.lock                      # Dependency lock file
├── README.md                    # Main documentation
└── Dockerfile                   # Container configuration
```

## Module Overview

### `main.py` - MCP Server

The main entry point for the Databricks MCP server. Defines all MCP tools using FastMCP:

- **execute_sql_query** - Execute SQL queries against Databricks
- **describe_uc_table** - Get table details with optional lineage
- **describe_uc_catalog** - List schemas in a catalog
- **describe_uc_schema** - List tables in a schema with optional columns
- **list_uc_catalogs** - List all available catalogs

Each tool is wrapped with error handling for:

- `DatabricksConfigError` - Missing/invalid configuration
- General exceptions - Unexpected errors

### `databricks_sdk_utils.py` - SDK Wrapper

Provides high-level functions wrapping the Databricks SDK:

**Configuration:**

- `get_sdk_client()` - Lazy initialization of WorkspaceClient with caching
- Environment variables: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_SQL_WAREHOUSE_ID`

**Core Functions:**

- `execute_databricks_sql()` - Execute SQL queries via SQL warehouse
- `get_uc_all_catalogs_summary()` - List all catalogs
- `get_uc_catalog_details()` - Get catalog and schemas info
- `get_uc_schema_details()` - Get schema and tables info
- `get_uc_table_details()` - Get table metadata with optional lineage

**Lineage Functions:**

- `_get_table_lineage()` - Query system.access.table_lineage
- `_process_lineage_results()` - Parse lineage data with notebook/job context
- `_get_job_info_cached()` - Cached job metadata retrieval
- `_get_notebook_id_cached()` - Cached notebook resolution
- `clear_lineage_cache()` - Clear caching for memory management

**Formatting Functions:**

- `_format_column_details_md()` - Format column metadata to Markdown
- `_format_single_table_md()` - Format table details to Markdown

### `databricks_formatter.py` - Result Formatting

Formats SQL query results into human-readable tables:

- `format_query_results()` - Convert query results to Markdown table format
- Supports both SDK and legacy API response formats
- Handles NULL values, empty results, and errors

## Design Patterns

### Error Handling

All MCP tools use a decorator pattern for consistent error handling:

```python
@handle_tool_errors("tool_name")
async def tool_function(...):
    # Tool implementation
```

This provides:

- Consistent error message formatting
- Special handling for `DatabricksConfigError`
- Graceful degradation on failures

### Caching Strategy

The SDK utils module implements caching for:

- **SDK Client**: Single global instance, lazy initialized
- **Job Metadata**: Cached per job_id to reduce API calls
- **Notebook IDs**: Cached per notebook_path for performance

### Async Design

All MCP tools are async and use `asyncio.to_thread()` to run blocking Databricks SDK calls without blocking the event loop.

### Markdown Output

All tools return Markdown-formatted strings for:

- Consistent, readable output
- Easy parsing by LLMs
- Human-friendly presentation

## Data Flow

1. **MCP Tool Call** → User/LLM invokes tool via MCP protocol
2. **Error Wrapper** → `@handle_tool_errors` catches exceptions
3. **Async Execution** → `asyncio.to_thread()` runs SDK call
4. **SDK Function** → Databricks API interaction via SDK
5. **Result Formatting** → Convert to Markdown
6. **Response** → Return formatted string to caller

## Configuration

Required environment variables:

- `DATABRICKS_HOST` - Workspace URL (e.g., https://myworkspace.databricks.com)
- `DATABRICKS_TOKEN` - Personal access token or service principal token
- `DATABRICKS_SQL_WAREHOUSE_ID` - SQL warehouse ID for query execution (optional for catalog browsing)

Optional:

- `.env` file support via python-dotenv

## Dependencies

Core:

- `databricks-sdk` - Official Databricks SDK
- `mcp[cli]` - Model Context Protocol framework
- `python-dotenv` - Environment variable management
- `httpx` - HTTP client (MCP dependency)

Development:

- `pytest` - Test framework
- `pytest-asyncio` - Async test support
- `pytest-cov` - Coverage reporting
- `pytest-mock` - Mocking utilities

## Extension Points

To add new functionality:

1. **New MCP Tool**: Add function to `main.py` with `@mcp.tool()` decorator
2. **New SDK Function**: Add to `databricks_sdk_utils.py` following existing patterns
3. **New Format**: Extend `databricks_formatter.py` for custom output formats
4. **New Tests**: Add to appropriate test file in `test/` directory

## Performance Considerations

- SDK client is cached globally
- Job and notebook metadata are cached during lineage processing
- Async design prevents blocking
- Query timeouts configured (default: 50s)
- Results are streamed where possible
