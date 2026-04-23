---
name: wrap-up
description: End-of-session workflow: commit and push pending changes, update memory files, and update CLAUDE.md with anything important learned during the session.
---

Perform the end-of-session wrap-up for the RallyBase project. Work through the following steps in order.

## Step 1 — Commit and push pending changes

Run `git status` and `git diff` to see what changed. If there are uncommitted changes:
- Stage and commit them with a descriptive message following the repo's commit style (seen in `git log`).
- Push to `main`.
- If there is nothing to commit, note that and skip.

## Step 2 — Verify the session log is complete

The session log at `docs/session-logs/YYYY-MM-DD.md` should already have been updated incrementally throughout the session (alongside each patch docs commit). Check that:
- The `### Objective` line is filled in (not `_TBD_`).
- The `### Work Completed` section has an entry for every version shipped today (cross-check against `git log --oneline --since=midnight` and today's entries in `CHANGELOG.md`).
- The `### Current State` paragraph is filled in.

If anything is missing or the stub was never filled in, complete those sections now. Do not rewrite entries that are already there — only fill gaps.

If the file for today does not exist at all, create and populate it in full (fallback to old behavior).

After filling any gaps, compact the session log:
- Remove duplicate information that appears more than once.
- Cross-reference against `CHANGELOG.md`. For any version ranges where all details are already fully documented in the changelog, replace those detail sections with a single line like `#### v0.X.X through v0.Y.Y — refer to CHANGELOG.md for details` under `### Work Completed`.
- Keep an `### Additional Details` section only for information not captured in the changelog (e.g. navigation contracts, known gotchas, deferred decisions).

## Step 3 — Update memory files

Review what happened this session and update memory files as needed:

- `project_wtt_analytics_pipeline.md` — update status, test counts, new features, new known issues/gotchas
- `feedback_patterns.md` — add any new preferences or corrections the user gave during this session

Do not duplicate content already captured. Update in-place rather than appending redundant lines. Check `MEMORY.md` to see if new memory files need to be added to the index.

## Step 4 — Update CLAUDE.md

Review the session and update `CLAUDE.md` if any of the following changed:
- Project status (current version, next target)
- Upcoming list (remove completed versions)
- Tech stack (new packages added or removed)
- Domain rules (new rules discovered or existing ones changed)
- Known issues / gotchas (new ones found, old ones resolved)

Do not add commentary or session notes to CLAUDE.md — it should remain a reference document, not a log.

## Step 5 — Commit and push

Stage and commit all files changed in steps 2–4 (session log, CLAUDE.md, and any memory files) with the message:

```
docs: session wrap-up YYYY-MM-DD
```

Push to `main`. If none of those files changed (all were already up to date), skip.

## Step 6 — Confirm

Tell the user what was committed in steps 1 and 5 (or that nothing needed committing in either), confirm the session log is complete, and list any memory or CLAUDE.md sections that were updated.
