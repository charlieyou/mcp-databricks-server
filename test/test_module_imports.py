"""Test that module imports don't have circular dependencies."""


def test_imports_do_not_loop():
    """Verify all modules can be imported without circular import errors."""
    import databricks_mcp.databricks_sdk_utils as facade
    from databricks_mcp import config, jobs, lineage, unity_catalog

    assert hasattr(facade, "get_workspace_client")
    assert hasattr(facade, "execute_databricks_sql")
    assert hasattr(facade, "get_uc_table_details")
    assert hasattr(facade, "get_job")
    assert hasattr(facade, "clear_lineage_cache")

    assert hasattr(config, "get_workspace_client")
    assert hasattr(lineage, "_get_table_lineage")
    assert hasattr(unity_catalog, "get_uc_table_details")
    assert hasattr(jobs, "get_job")


def test_facade_reexports_match_submodules():
    """Verify facade re-exports are the same objects as submodule exports."""
    import databricks_mcp.databricks_sdk_utils as facade
    from databricks_mcp import config, jobs, lineage, unity_catalog

    assert facade.get_workspace_client is config.get_workspace_client
    assert facade.execute_databricks_sql is config.execute_databricks_sql
    assert facade.get_uc_table_details is unity_catalog.get_uc_table_details
    assert facade.get_job is jobs.get_job
    assert facade.clear_lineage_cache is lineage.clear_lineage_cache
