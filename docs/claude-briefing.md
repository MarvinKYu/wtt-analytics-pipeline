# Claude Code Briefing — WTT Analytics Pipeline

This document briefs a new Claude Code instance on the user's background, preferences, and the workflows we've established across projects. Read this alongside `CLAUDE.md`.

---

## About the User

**Background**: CS degree from UIUC (~2025). First job was ~6 months as a data engineer at a small financial firm in NYC (built a centralized financial data platform on Snowflake). Recently relocated to San Francisco for a software engineering job search (open to data engineering as well).

**Working model**: The user provides architectural direction and product decisions; Claude implements. He has good instincts and does not need hand-holding on decisions. When explaining implementations, focus on *why* things are designed a certain way — frame in terms of patterns and principles, not just syntax.

**AI-assisted development mindset**: He believes strongly in this model (architectural thinking + good fundamentals > pure coding throughput, since AI handles implementation well given the right guidance). He's aware he's on the right side of this shift and leans into it. He also explicitly wants to understand the finer implementation details over time — not just ship features, but learn from the code. Seize natural moments to explain a non-obvious decision.

---

## Session Workflow

### Start of session
Run `/session-start` at the beginning of each working session. This skill:
1. Reads the latest session log in `docs/session-logs/`
2. Reads `CLAUDE.md` and any memory files
3. Produces a structured summary of current state for the user to verify before work begins

### End of session
Run `/wrap-up` at the end of each session (or when the user says "pause", "wrap up", or "set a checkpoint"). This skill:
1. Verifies the session log for today is complete
2. Updates memory files with anything new learned
3. Updates `CLAUDE.md` with anything worth preserving
4. Commits as `docs: session wrap-up YYYY-MM-DD`

`CHANGELOG.md` is **not** updated at wrap-up time — they are updated incrementally per patch during the session (see below).

---

## Session Log System

### File location and naming
`docs/session-logs/YYYY-MM-DD.md` — one file per calendar day, not per session.

**Same-day sessions**: if a log file for today already exists, append a new section (e.g. `## Session 2`) to it rather than creating a second file.

### Structure
Each log file has:
```markdown
## Session Log — YYYY-MM-DD

### Objective
[High-level goal for the session]

### Work Completed

#### v1.2.3 — Feature name
- Bullet summary of what was implemented/changed
- Key design decisions or non-obvious implementation notes

### Known Issues / Notes
[Issues discovered, decisions deferred, production state]

### Current State
[One-sentence status of where the project stands at end of session]
```

### Stub at start, fill per patch
At session start, create a stub with placeholder sections. Each time a version ships, write the "Work Completed" entry for that version in the same docs commit. Wrap-up only verifies completeness — it does not write the log from scratch.

**Why**: Writing the full log at wrap-up from memory causes omissions when sessions span multiple conversations or ship many versions.

---

## Versioning and Commit Workflow

### Implementation workflow (planned versions)
1. **Detail-drilling conversation first** — before writing any code, converse with the user to lock down all specifics for the version. Never start implementing without this step.
2. **Implement one version at a time** — complete it with passing tests, then commit + push.
3. **Wait for user sign-off** — after shipping, wait for the user to confirm no additional bugs before planning the next version.

### Patch versioning (bug fixes / UI tweaks after a planned version)
Every change shipped after a planned version gets a patch version increment (e.g. `v1.0.0 → v1.0.1 → v1.0.2`). Each patch produces exactly **two commits**:

1. **Code commit** — message format: `fix(v1.0.1): description`. Push immediately. Then create and push the git tag:
   ```bash
   git tag v1.0.1 HEAD && git push origin v1.0.1
   ```
2. **Docs commit** — updates `CHANGELOG.md` and `docs/session-logs/YYYY-MM-DD.md` together in one commit. No code changes in this commit.

Do this automatically for every shipped change — no need to ask the user.

### Commit + push cadence
Commit and push after each completed version. Do not ask — just do it when the version is done and tests pass.

### Git tags
Tag the final commit of every version immediately after pushing. This keeps CHANGELOG compare links functional.

---

## Documentation Maintenance

### `CHANGELOG.md`
Standard changelog format. Updated in the docs commit after each patch. Shipped versions live here permanently.

---

## Code Style and Scope

### No unsolicited cleanup
Complete exactly what was asked, nothing more. No refactoring, no improvements to surrounding code, no premature abstractions.

### Algorithm isolation
Keep algorithm modules as pure functions with zero side effects and no DB/IO imports. The user has explicitly asked for this pattern to keep algorithms tweakable independently.

### Features over one-off scripts
When the user needs a data operation, build it as a proper feature or reusable tool, not a throwaway script. If a script is truly the right tool, say so — but default to the feature approach.

### No comments unless the WHY is non-obvious
Default to writing no comments. Only add one when there's a hidden constraint, a subtle invariant, or a workaround for a specific bug. Never write multi-line comment blocks or docstrings that describe what the code does.

---

## Memory System

Claude Code in this project should maintain a memory system at `~/.claude/projects/<project-path>/memory/`. This persists context across sessions.

### Types
- **user** — who the user is, their role, preferences, knowledge level
- **feedback** — guidance on approach: what to avoid and what to keep doing
- **project** — ongoing work, goals, key decisions, blockers
- **reference** — pointers to external resources (dashboards, tickets, docs)

### Format (each memory file)
```markdown
---
name: Memory name
description: One-line description used to judge relevance
type: user | feedback | project | reference
---

[Content. For feedback/project types: lead with the rule/fact, then **Why:** and **How to apply:** lines]
```

### `MEMORY.md` index
Each memory file gets a one-line entry in `MEMORY.md`: `- [Title](file.md) — one-line hook`

`MEMORY.md` is always loaded into context; keep it under 200 lines.

### What NOT to save
Code patterns, file paths, git history, debugging solutions, anything already in CLAUDE.md. Save only what's non-obvious and won't rot.

---

## Related Projects

This pipeline is a companion to **RallyBase** (`C:\Users\marvi\Documents\Personal\TTRC_Project\project_root`), a competitive table tennis tournament management web app (Next.js + PostgreSQL). Both projects share the same Glicko-lite rating algorithm — the parameters in `rating_engine/glicko.py` here should stay in sync with the RallyBase implementation in spirit (though the data scale and player population are different).
