# Agent Instructions

## Issue Tracking (beads)

**IMPORTANT**: Use **bd** for ALL issue tracking. No markdown TODOs.

### Commands

```bash
bd ready --json                    # Find unblocked work
bd create "Title" -t task -p 2 --json
bd update <id> --status in_progress --json
bd close <id> --reason "Done" --json
```

### Workflow

1. `bd ready` → find work
2. `bd update <id> --status in_progress` → claim
3. Create worktree: `git worktree add ../databricks-mcp-wt/<branch-name> -b <branch-name> origin/main`
4. Work in the worktree
5. Use oracle to review your changes
6. Push and create PR: `git push -u origin <branch-name> && gh pr create`
    a. Include Amp thread URL and beads ID in description
    b. Include how I can verify that the work is completed, a command to run to test new functionality
7. `bd close <id>` → complete

**Reference:**
- **Types**: `bug`, `feature`, `task`, `epic`, `chore`
- **Priorities**: `0` (Critical) to `4` (Backlog)
- **Planning Docs**: Store ephemeral plans in `history/` directory only.


#### Merging and Cleanup

1. `gh pr merge [pr-number]
2. Navigate out of the worktree: `cd ../../ucmt`
3. Remove worktree: `git worktree remove [worktree-branch]
4. Delete the branch: `git branch -d [worktree-branch]
5. Prune remote branches: `git fetch --prune`

## Project Overview & Tech Stack

**MCP Server for Databricks Unity Catalog.**

- **Stack**: Python 3.10+, `uv` (pkg mgr), `pytest` (async/mock), `ruff` (lint), `databricks-sdk`.
- **Entry**: `src/databricks_mcp/main.py`
- **Output**: Tools must return **Markdown**.

**Key Commands:**
- **Install**: `uv pip install -e .`
- **Test**: `uv run pytest` (Do not make real network calls; mock `WorkspaceClient`)
- **Lint/Format**: `uv run ruff check --fix .`
- **Run**: `uv run databricks-mcp-server`
