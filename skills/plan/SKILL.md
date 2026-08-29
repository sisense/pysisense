---
name: plan
description: Use to turn a raw request to change pysisense's own source into a concrete task definition — scope, what changes, and what "done" looks like — before any impact analysis or code is written. Usually the first stage the `orchestrator` skill runs; use directly only when the user explicitly wants just a plan, or is resuming a cycle at this stage.
---

# plan — task definition

Turn the raw request into a concrete, written task definition. Nothing after this stage should have to re-ask "what are we actually building."

If `.pysisense-dev/debug.md` exists (this was a bug-fix request and `debug` already confirmed the repro), read it first. Scope this plan around the confirmed reproduction conditions and root-cause hypothesis it recorded — don't re-diagnose the bug from scratch.

## Output

Write `.pysisense-dev/plan.md` with:

- **Request** — the user's ask, restated precisely (don't editorialize).
- **Scope** — which module(s)/mixin file(s) are involved (use the mixin lookup table in `CLAUDE.md`; grep `pysisense/<module>/*.py` if the table doesn't cover it — it has known staleness in at least one spot).
- **Change type** — new method, changed method, new module, bug fix, or refactor. This determines what later stages must also touch (a new method needs a test + docs + example; a new module needs all of that plus `CLAUDE.md`'s Modules table and canonical init pattern).
- **Definition of done** — an explicit checklist, e.g.:
  - [ ] Method implemented in `<file>`
  - [ ] Unit test added/updated in `tests/unit/<file>`
  - [ ] `docs/<module>.md` checked/updated
  - [ ] `examples/<module>_example.md` checked/updated
  - [ ] `CLAUDE.md` mixin table updated (only if new method/module)
  - [ ] `ruff check`, `ruff format --check`, docstring checker, and tests all pass
- **Out of scope** — anything adjacent the request does *not* cover, stated explicitly so `code` doesn't scope-creep into it.

If the request is ambiguous about scope (e.g. "fix the users module" without saying which method), ask the user before writing the plan — don't guess at scope this early; a wrong guess here propagates through the whole cycle.

## Handoff

Update `.pysisense-dev/cycle.json`: `current_stage: struct`, add `"plan"` to `stages_completed`. `struct` reads `plan.md` next.
