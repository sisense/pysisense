---
name: report
description: Final, human-facing stage of the pysisense dev cycle — summarizes the whole cycle in plain language (what was done, why, and any decisions/tradeoffs made, including anything `rethink` flagged as low-stakes but worth knowing). Always run by `orchestrator` as the last stage; use directly to regenerate a summary from an already-completed cycle's state.
---

# report — final summary

Read the whole cycle's state — `.pysisense-dev/plan.md`, `struct.md`, `cycle.json` (including its `notes`) — and write a plain-language summary. This is a changelog of *reasoning*, not just a diff.

## Structure

- **What changed** — one or two sentences, plain language, no jargon dump.
- **Why** — the actual motivation from `plan.md`'s request, not a restatement of the diff.
- **Decisions & tradeoffs** — every `notes` entry from `rethink` (the low-stakes calls it made along the way), plus the breaking/non-breaking classification from `struct` and why. If a `needs_input` gate was hit and resolved mid-cycle, say what was asked and what the user decided.
- **Verification** — confirm `validate_result: pass` and `ci_result: pass`; if either is missing, say so plainly rather than implying the cycle finished clean.
- **Docs/examples** — explicitly state whether `docs/<module>.md` and `examples/<module>_example.md` were updated, and why or why not, per `plan.md`'s definition-of-done checklist.

Keep it readable by someone who wasn't watching the cycle run — assume no context beyond the original request.

## Handoff

Update `.pysisense-dev/cycle.json`: `status: done`. The cycle is complete; `.pysisense-dev/` can be cleared before the next one starts (or left as a record — user's call).
