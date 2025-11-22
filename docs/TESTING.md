# Testing Documentation

## Overview

This document describes the comprehensive test suite for the Databricks MCP Server. All tests are written using pytest and can be run with the `uv` package manager.

## Running Tests

### Run all tests
```bash
uv run pytest -v
```

### Run tests with coverage
```bash
uv run pytest --cov=. --cov-report=term-missing --cov-report=html
```

### Run specific test file
```bash
uv run pytest test_databricks_formatter.py -v
uv run pytest test_databricks_sdk_utils.py -v
uv run pytest test_main.py -v
```

## Test Coverage

**Overall Coverage: 91%**

- `databricks_formatter.py`: 92% coverage
- `databricks_sdk_utils.py`: 78% coverage
- `main.py`: 98% coverage
- Test files: 100% coverage

## Test Structure

### 1. `conftest.py`
Contains shared fixtures and configuration:
- Mock objects for Databricks SDK types (CatalogInfo, SchemaInfo, TableInfo, ColumnInfo)
- Mock SQL responses and results
- Environment variable setup
- Auto-reset fixtures for clean test isolation

### 2. `test_databricks_formatter.py` (10 tests)
Tests the query result formatting functionality:
- ✅ Format successful SDK-based query results
- ✅ Format empty results
- ✅ Format error results
- ✅ Handle NULL values in results
- ✅ Format old API style output (backward compatibility)
- ✅ Handle empty/None input
- ✅ Handle missing columns
- ✅ Format complex data types (JSON, arrays)
- ✅ Format single row results
- ✅ Format results with many columns

### 3. `test_databricks_sdk_utils.py` (39 tests)
Tests core Databricks SDK utility functions:

#### SDK Client Initialization (4 tests)
- ✅ Successful client initialization
- ✅ Client caching behavior
- ✅ Missing DATABRICKS_HOST error handling
- ✅ Missing DATABRICKS_TOKEN error handling

#### SQL Execution (5 tests)
- ✅ Successful SQL query execution
- ✅ Missing warehouse ID error handling
- ✅ Failed query state handling
- ✅ Queries returning no data
- ✅ Exception handling during execution

#### Catalog Operations (3 tests)
- ✅ List all catalogs successfully
- ✅ Handle empty catalog list
- ✅ Handle catalog listing errors

#### Catalog Details (3 tests)
- ✅ Get catalog details with schemas
- ✅ Handle catalog with no schemas
- ✅ Handle catalog details errors

#### Schema Operations (4 tests)
- ✅ Get schema details without columns
- ✅ Get schema details with columns
- ✅ Handle schema with no tables
- ✅ Handle schema details errors

#### Table Operations (3 tests)
- ✅ Get table details without lineage
- ✅ Get table details with lineage
- ✅ Handle table details errors

#### Formatting Functions (3 tests)
- ✅ Format column details
- ✅ Handle empty column lists
- ✅ Format nullable columns

#### Lineage Processing (4 tests)
- ✅ Process lineage results successfully
- ✅ Handle empty lineage data
- ✅ Handle invalid lineage data
- ✅ Clear lineage cache

### 4. `test_main.py` (13 tests + 2 integration tests)
Tests MCP server tools and integration:

#### Utility Functions (2 tests)
- ✅ Format exception to markdown
- ✅ Handle multiline exception details

#### execute_sql_query Tool (6 tests)
- ✅ Execute successful SQL query
- ✅ Handle failed query status
- ✅ Handle error status
- ✅ Handle unexpected status
- ✅ Handle Databricks config errors
- ✅ Handle unexpected exceptions

#### describe_uc_table Tool (4 tests)
- ✅ Describe table without lineage
- ✅ Describe table with lineage
- ✅ Handle config errors
- ✅ Handle unexpected errors

#### describe_uc_catalog Tool (2 tests)
- ✅ Successful catalog description
- ✅ Handle errors

#### describe_uc_schema Tool (3 tests)
- ✅ Describe schema without columns
- ✅ Describe schema with columns
- ✅ Handle errors

#### list_uc_catalogs Tool (4 tests)
- ✅ List catalogs successfully
- ✅ Handle empty catalog list
- ✅ Handle config errors
- ✅ Handle unexpected errors

#### Integration Tests (2 tests)
- ✅ Complete catalog → schema → table discovery flow
- ✅ Concurrent SQL queries execution

## Key Testing Features

### Mocking Strategy
- All Databricks SDK calls are mocked to avoid requiring actual Databricks credentials
- Mock objects accurately simulate SDK response structures
- Environment variables are properly managed and reset between tests

### Error Handling
- Tests verify proper error handling for:
  - Missing configuration (host, token, warehouse ID)
  - API errors and exceptions
  - Invalid input data
  - Network failures

### Async Testing
- All async tools are properly tested using pytest-asyncio
- Concurrent operations are verified in integration tests

### Edge Cases
- Empty results and data
- NULL values
- Missing metadata
- Invalid inputs
- Malformed responses

## Test Commands Summary

All 5 MCP commands are thoroughly tested:

1. **execute_sql_query** - Execute SQL queries against Databricks
2. **describe_uc_table** - Get table details with optional lineage
3. **describe_uc_catalog** - List schemas in a catalog
4. **describe_uc_schema** - List tables in a schema with optional columns
5. **list_uc_catalogs** - List all available catalogs

## Test Results

```
62 tests passed
0 tests failed
Test execution time: ~0.3 seconds
```

## Development Dependencies

```toml
[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.1.0",
    "pytest-mock>=3.12.0",
]
```

## Future Improvements

Areas with lower coverage that could benefit from additional tests:
- Job info caching logic (currently 78% covered)
- Notebook resolution logic
- Some edge cases in lineage processing
- Error recovery scenarios

