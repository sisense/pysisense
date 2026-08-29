---
name: struct
description: Use for impact and compatibility analysis on a pysisense source change, before any code is touched — what areas of the codebase are affected, and whether the change is breaking (and if so, whether it needs a deprecation path or can be a straight delete-and-recreate). Usually run by `orchestrator` right after `plan`; use directly for a standalone "what would this change affect" question.
---

# struct — impact & compatibility analysis

Map what the change from `plan.md` actually touches, before any code is written, and classify whether it's breaking.

## What to check

- **Callers** — grep for every internal/test caller of the method(s)/class(es) in scope. A mixin method's own module tests, other mixins that call it, and any `examples/*.md`/`docs/*.md` snippet that demonstrates it.
- **Return-shape change** — if an existing method's output is changing, classify it:
  - **More information than before** (an extra field, a previously-unresolved value now resolved, a record that used to be silently dropped now included) → **safe**, no sign-off needed.
  - **Less information than before** (a field that used to carry a real value now comes back `None`/missing, a record that used to appear now silently skipped) → **breaking**. This is the single most common false-safe mistake — flag it even if it looks like a minor edge case.
- **Signature change** — a removed/renamed parameter, or a parameter that changes from optional to required, is breaking regardless of return shape.
- **Deletion** — code being removed (a method, a mixin, a whole module) is breaking by definition; note whether anything still references it.
- **If breaking**: decide **deprecation path vs. straight delete-and-recreate**. A deprecation path (keep the old method, mark it, forward to the new one) is appropriate when external callers plausibly exist outside this repo (this SDK ships to users); a straight delete-and-recreate is appropriate only for something clearly internal/unreleased, or when the user has explicitly asked for a hard replace.

## Output

Write `.pysisense-dev/struct.md`:

- **Affected files** — every file touched, directly or by reference (source, tests, docs, examples, `CLAUDE.md` tables).
- **Breaking?** — yes/no, with the specific reason (more/less info, signature, deletion).
- **Recommended path** — deprecate or delete-and-recreate, if breaking.

## Handoff

Update `.pysisense-dev/cycle.json`: `current_stage: rethink_struct`. `rethink` runs next — it decides whether this struct output is safe to act on or needs the user's sign-off before `build` starts.
