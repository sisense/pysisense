---
name: report
description: Writes a plain-language summary of a finished pysisense dev cycle: what changed, why, and any tradeoffs made along the way, including anything `rethink` flagged as minor but worth knowing. Always the last step of `orchestrator`'s cycle. Invoke it directly to regenerate a summary from a cycle that already finished.
---

# report: final summary

Read the whole cycle's state, `.pysisense-dev/plan.md`, `struct.md`, `cycle.json` (including its `notes`), and write a plain-language summary. This is a changelog of *reasoning*, not just a diff.

## Structure

- **What changed**: one or two sentences, plain language, no jargon dump.
- **Why**: the actual motivation from `plan.md`'s request, not a restatement of the diff.
- **Decisions & tradeoffs**: every `notes` entry from `rethink` (the low-stakes calls it made along the way), plus the breaking/non-breaking classification from `struct` and why. If a `needs_input` gate was hit and resolved mid-cycle, say what was asked and what the user decided. If any part of the fix was a hypothesis about external-system behavior rather than a confirmed fact (see `rethink`), say so plainly here too, don't let it read as more certain than it is.
- **Verification**: confirm `validate_result: pass` and `ci_result: pass`. If either is missing, say so plainly rather than implying the cycle finished clean.
- **Docs/examples**: explicitly state whether `docs/<module>.md` and `examples/<module>_example.md` were updated, and why or why not, per `plan.md`'s definition-of-done checklist. If the change was breaking, also confirm `CHANGELOG.md` and `docs/upgrading.md` got entries, don't let a shape change land undocumented.

Keep it readable by someone who wasn't watching the cycle run. Assume no context beyond the original request.

## Handoff

Update `.pysisense-dev/cycle.json`: `status: done`. The cycle is complete. `.pysisense-dev/` can be cleared before the next one starts (or left as a record, user's call).
