---
name: struct
description: Checks what a planned pysisense change would actually affect, before any code is touched: which files, and whether the change would break existing behavior (and if so, whether it needs a deprecation path or can just be replaced outright). Usually runs right after `plan`, as part of `orchestrator`'s cycle. Invoke it directly for a standalone "what would this change affect" question.
---

# struct: impact & compatibility analysis

Map what the change from `plan.md` actually touches, before any code is written, and classify whether it's breaking.

## What to check

- **Callers**: grep for every internal/test caller of the method(s)/class(es) in scope. A mixin method's own module tests, other mixins that call it, and any `examples/*.md`/`docs/*.md` snippet that demonstrates it. Also check whether those callers' own test fixtures hardcode the current shape (URL, field names) in a way that would break even though the caller's *code* doesn't need to change. Confirming production code needs no change is not the same as confirming its tests won't need one.
- **Return-shape change**: if an existing method's output is changing, classify it:
  - **More information than before** (an extra field, a previously-unresolved value now resolved, a record that used to be silently dropped now included) → **safe**, no sign-off needed.
  - **Less information than before** (a field that used to carry a real value now comes back `None`/missing, a record that used to appear now silently skipped) → **breaking**. This is the single most common false-safe mistake. Flag it even if it looks like a minor edge case.
- **Signature change**: a removed/renamed parameter, or a parameter that changes from optional to required, is breaking regardless of return shape.
- **Deletion**: code being removed (a method, a mixin, a whole module) is breaking by definition. Note whether anything still references it.
- **The failure-dict shape itself is a stable contract.** Renaming or removing `"ok"`/`"error"` on a failure return is breaking even with no signature change. Adding a key (e.g. an extra `"raw_body"`) is not breaking, but call it out, downstream consumers matching an exact key set need to widen. A method changing from returning `[]`/`None`/a bare string on failure to the standard `{"ok": False, "error": ...}` dict is a fix, not a new break, since the pre-2.0 shape was never a supported contract.
- **External-system behavior you can't verify from source**: if the root cause depends on how a live Sisense server actually behaves (not just how the SDK is internally consistent), say so explicitly and treat any fix as a hypothesis pending live confirmation, not a settled fact. Internal consistency with a sibling method is a plausible signal, not proof.
- **If breaking**: decide **deprecation path vs. straight delete-and-recreate**. A deprecation path (keep the old method, mark it, forward to the new one) is appropriate when external callers plausibly exist outside this repo (this SDK ships to users). A straight delete-and-recreate is appropriate only for something clearly internal/unreleased, or when the user has explicitly asked for a hard replace.

## Output

Write `.pysisense-dev/struct.md`:

- **Affected files**: every file touched, directly or by reference (source, tests, docs, examples, `CLAUDE.md` tables).
- **Breaking?**: yes/no, with the specific reason (more/less info, signature, deletion).
- **Recommended path**: deprecate or delete-and-recreate, if breaking.
- **If breaking**: note that `code` must add `CHANGELOG.md` and `docs/upgrading.md` entries in the same pass, this isn't optional per this repo's conventions.

## Handoff

Update `.pysisense-dev/cycle.json`: `current_stage: rethink_struct`. `rethink` runs next. It decides whether this struct output is safe to act on or needs the user's sign-off before `build` starts.
