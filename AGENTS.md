# Agent Instructions

## Issue Tracking (beads)

**IMPORTANT**: Use **bd** for ALL issue tracking. No markdown TODOs.

**Workflow:**
1. **Check ready work**: `bd ready --json`
2. **Claim task**: `bd update <id> --status in_progress --json`
3. **Work**: Implement, test, document.
   - **Found new work?**: `bd create "Title" -t bug|feature|task -p 1 --deps discovered-from:<id> --json`
4. **Complete**: `bd close <id> --reason "Done" --json`
5. **Sync**: `bd sync` (Exports JSONL, commits, pulls, pushes) - **Run at end of every session.**

**Reference:**
- **Types**: `bug`, `feature`, `task`, `epic`, `chore`
- **Priorities**: `0` (Critical) to `4` (Backlog)
- **Planning Docs**: Store ephemeral plans in `history/` directory only.

## Landing the Plane

**MANDATORY** when user says "land the plane":

1. **File remaining work**: Create bead issues.
2. **Quality Gates**:
   ```bash
   uv run pytest
   uv run ruff check .
   ```
3. **Close/Update Issues**: `bd close ...`
4. **PUSH TO REMOTE** (Non-negotiable):
   ```bash
   git pull --rebase
   bd sync        # Exports, commits, pushes
   git push       # VERIFY "up to date with origin/main"
   ```
   *Do not stop until `git push` succeeds.*
5. **Cleanup**: `git stash clear && git remote prune origin`
6. **Handoff**: Suggest next issue (bd-X).

## GitHub Issues
Use CLI: `gh issue list`, `gh pr list`, `gh issue view <id>`.

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
