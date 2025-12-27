# Context Index

Quick navigation for LLMs and developers. Read this first to find relevant documentation.

## Project Documentation

| File | Purpose | Read when... |
|------|---------|--------------|
| `PROJECT.md` | Project overview, goals, tech stack | Starting work or need high-level context |
| `BEST_PRACTICES.md` | Python and coding standards | Writing or reviewing code |

## Folders

| Folder | Purpose |
|--------|---------|
| `tasks/` | Active tasks and implementation work |
| `done/` | Completed tasks (reference only) |
| `archived/` | Deprecated docs that may be relevant later |
| `decisions/` | Architecture Decision Records (ADRs) |

## Decision Records

ADRs document *why* we chose specific approaches. Format: `ADR-XXX-title.md`

| ADR | Topic | Status |
|-----|-------|--------|
| [ADR-001](decisions/ADR-001-import-strategy.md) | Import strategy (hybrid COPY + upsert) | Accepted |
| [ADR-002](decisions/ADR-002-schema-and-api.md) | Schema (VARCHAR) and API design (REST + SFTP + webhook) | Accepted |

## Active Tasks

| Task | Status | File |
|------|--------|------|
| [TASK-001](tasks/TASK-001-implementation-plan.md) | Active | Implementation plan with 6 phases |
