# Agent Instructions

## Repository Structure (Bare Repo + Worktrees)

This repo uses a **bare repo setup** where `.bare/` contains the Git database and all working directories are peer worktrees.

```
~/code/databricks-mcp/
├── .bare/                    # Git database (objects, refs, etc.)
├── .git                      # File pointing to .bare (enables git commands from root)
├── [main worktree files]     # Current directory IS the main worktree
└── feature-branch/           # Additional worktrees created here as siblings
```

**Git commands work from project root** thanks to the `.git` pointer file.

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
3. Create worktree from project root:
   ```bash
   cd ~/code/databricks-mcp
   git worktree add ./<branch-name> -b <branch-name> origin/main
   ```
4. Work in the worktree: `cd <branch-name>`
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

```bash
gh pr merge <pr-number> --squash
cd ~/code/databricks-mcp
git worktree remove ./<branch-name>
git branch -d <branch-name>
git fetch --prune
```

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
