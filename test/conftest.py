"""Shared test fixtures and configuration for pytest."""

import pytest
from unittest.mock import Mock
from databricks.sdk.service.catalog import (
    CatalogInfo,
    SchemaInfo,
    TableInfo,
    ColumnInfo as CatalogColumnInfo,
)
from databricks.sdk.service.sql import (
    StatementResponse,
    StatementState,
    StatementStatus,
    ResultData,
    ResultManifest,
    ResultSchema,
    ColumnInfo,
)


@pytest.fixture
def mock_catalog_info():
    """Create a mock CatalogInfo object."""
    catalog = Mock(spec=CatalogInfo)
    catalog.name = "test_catalog"
    catalog.comment = "Test catalog description"
    catalog.catalog_type = Mock()
    catalog.catalog_type.value = "MANAGED_CATALOG"
    return catalog


@pytest.fixture
def mock_schema_info():
    """Create a mock SchemaInfo object."""
    schema = Mock(spec=SchemaInfo)
    schema.name = "test_schema"
    schema.full_name = "test_catalog.test_schema"
    schema.comment = "Test schema description"
    schema.catalog_name = "test_catalog"
    return schema


@pytest.fixture
def mock_column_info():
    """Create a mock ColumnInfo object."""
    column = Mock(spec=CatalogColumnInfo)
    column.name = "id"
    column.type_text = "INT"
    column.type_name = Mock()
    column.type_name.value = "INT"
    column.nullable = False
    column.comment = "Primary key"
    column.partition_index = None
    return column


@pytest.fixture
def mock_table_info(mock_column_info):
    """Create a mock TableInfo object."""
    table = Mock(spec=TableInfo)
    table.name = "test_table"
    table.full_name = "test_catalog.test_schema.test_table"
    table.comment = "Test table description"
    table.catalog_name = "test_catalog"
    table.schema_name = "test_schema"
    table.columns = [mock_column_info]
    return table


@pytest.fixture
def mock_statement_response():
    """Create a mock StatementResponse for successful SQL query."""
    # Mock the response structure
    response = Mock(spec=StatementResponse)

    # Mock status
    status = Mock(spec=StatementStatus)
    status.state = StatementState.SUCCEEDED
    status.error = None
    response.status = status

    # Mock result data
    result = Mock(spec=ResultData)
    result.data_array = [
        ["1", "Alice", "30"],
        ["2", "Bob", "25"],
    ]
    response.result = result

    # Mock manifest/schema
    manifest = Mock(spec=ResultManifest)
    schema = Mock(spec=ResultSchema)

    col1 = Mock(spec=ColumnInfo)
    col1.name = "id"
    col2 = Mock(spec=ColumnInfo)
    col2.name = "name"
    col3 = Mock(spec=ColumnInfo)
    col3.name = "age"

    schema.columns = [col1, col2, col3]
    manifest.schema = schema
    response.manifest = manifest

    return response


@pytest.fixture
def mock_sql_success_result():
    """Create a mock successful SQL execution result."""
    return {
        "status": "success",
        "row_count": 2,
        "data": [
            {"id": "1", "name": "Alice", "age": "30"},
            {"id": "2", "name": "Bob", "age": "25"},
        ],
    }


@pytest.fixture
def mock_sql_error_result():
    """Create a mock SQL execution error result."""
    return {
        "status": "error",
        "error": "Table not found",
        "details": "The table 'invalid_table' does not exist",
    }


@pytest.fixture
def mock_lineage_data():
    """Create mock lineage data."""
    return {
        "status": "success",
        "row_count": 2,
        "data": [
            {
                "source_table_full_name": "catalog.schema.upstream_table",
                "target_table_full_name": "catalog.schema.test_table",
                "entity_type": "NOTEBOOK",
                "entity_id": "notebook123",
                "entity_metadata": '{"notebook_id": "notebook123", "job_info": {"job_id": "job456"}}',
            },
            {
                "source_table_full_name": "catalog.schema.test_table",
                "target_table_full_name": "catalog.schema.downstream_table",
                "entity_type": "NOTEBOOK",
                "entity_id": "notebook789",
                "entity_metadata": '{"notebook_id": "notebook789", "job_info": {"job_id": "job999"}}',
            },
        ],
    }


@pytest.fixture
def mock_databricks_client():
    """Create a mock Databricks WorkspaceClient."""
    client = Mock()

    # Mock catalogs API
    client.catalogs = Mock()
    client.catalogs.list = Mock()

    # Mock schemas API
    client.schemas = Mock()
    client.schemas.list = Mock()
    client.schemas.get = Mock()

    # Mock tables API
    client.tables = Mock()
    client.tables.list = Mock()
    client.tables.get = Mock()

    # Mock statement execution API
    client.statement_execution = Mock()
    client.statement_execution.execute_statement = Mock()

    # Mock jobs API
    client.jobs = Mock()
    client.jobs.get = Mock()

    # Mock workspace API
    client.workspace = Mock()
    client.workspace.get_status = Mock()

    return client


@pytest.fixture
def setup_env_vars(reset_sdk_client, monkeypatch, tmp_path):
    """Set up a temp .databrickscfg file with a DEFAULT profile for tests."""
    cfg_file = tmp_path / ".databrickscfg"
    cfg_file.write_text("""[DEFAULT]
host = https://test.databricks.com
token = test_token
sql_warehouse_id = test_warehouse_id
""")
    monkeypatch.setattr("databricks_mcp.config._get_databrickscfg_path", lambda: cfg_file)
    from databricks_mcp import databricks_sdk_utils
    databricks_sdk_utils.reload_workspace_configs()


@pytest.fixture
def setup_two_workspaces(reset_sdk_client, monkeypatch, tmp_path):
    """Set up two workspaces (default and dev) for multi-workspace tests."""
    cfg_file = tmp_path / ".databrickscfg"
    cfg_file.write_text("""[DEFAULT]
host = https://default.databricks.com
token = default_token
sql_warehouse_id = default_warehouse

[DEV]
host = https://dev.databricks.com
token = dev_token
sql_warehouse_id = dev_warehouse
""")
    monkeypatch.setattr("databricks_mcp.config._get_databrickscfg_path", lambda: cfg_file)
    from databricks_mcp import databricks_sdk_utils
    databricks_sdk_utils.reload_workspace_configs()


@pytest.fixture(autouse=True)
def reset_sdk_client(monkeypatch, tmp_path):
    """Reset workspace clients and force mock config for ALL tests to prevent real API calls."""
    from unittest.mock import Mock
    from databricks_mcp import databricks_sdk_utils
    
    # Create a default mock config file for all tests
    cfg_file = tmp_path / ".databrickscfg"
    cfg_file.write_text("""[DEFAULT]
host = https://test.databricks.com
token = test_token
sql_warehouse_id = test_warehouse_id
""")
    monkeypatch.setattr("databricks_mcp.config._get_databrickscfg_path", lambda: cfg_file)
    
    # Mock WorkspaceClient and SdkConfig to prevent any real network calls or profile validation
    mock_client = Mock()
    mock_sdk_config = Mock()
    monkeypatch.setattr("databricks_mcp.config.WorkspaceClient", lambda **kwargs: mock_client)
    monkeypatch.setattr("databricks.sdk.config.Config", lambda **kwargs: mock_sdk_config)

    databricks_sdk_utils._workspace_clients = {}
    databricks_sdk_utils.clear_lineage_cache()
    databricks_sdk_utils.reload_workspace_configs()
    yield
    databricks_sdk_utils._workspace_clients = {}
    databricks_sdk_utils.clear_lineage_cache()
