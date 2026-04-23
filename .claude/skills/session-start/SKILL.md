---
name: session-start
description: Review project state at the start of a session by reading the latest session log, CLAUDE.md, and memory files, then produce a structured summary for the user to verify before work begins.
---

Review the current state of the WTT analytics pipeline project and summarize it for the user to verify before work begins. Follow these steps exactly:

1. Read the session logs directory to find the most recent log file: `docs/session-logs/`
2. Read the most recent session log file.
3. Read the memory files referenced in `C:\Users\marvi\.claude\projects\C--Users-marvi-Documents-Personal-TTRC-Project-wtt-analytics-pipeline\memory\MEMORY.md`.
5. CLAUDE.md is already in context — do not re-read it.

Then produce a summary with the following sections:

**Project: WTT Analytics Pipeline** — one-sentence description of what the app is.

**Status** — current completion state (what stages/phases are done).

**Last session (date)** — bullet list of what was completed in the most recent session log.

**Active constraints** — the 4–5 most important working rules from memory.

End with: "Does this look right? What are we working on today?"

Then, **create the session log stub for today** if one does not already exist:

- Check `docs/session-logs/` for a file named `YYYY-MM-DD.md` using today's date.
- If it does not exist, create it with this placeholder content:

```
## Session Log — YYYY-MM-DD

### Objective
_TBD_

### Work Completed

### Known Issues / Notes

### Current State
```

- If a file for today already exists, leave it unchanged.
- Do not commit the stub — it will be committed as part of the first patch docs commit of the session.
