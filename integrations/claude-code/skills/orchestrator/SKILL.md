---
name: orchestrator
description: Runs the full pysisense development cycle: reproduce the bug if there is one, plan the change, check its impact, write tests, implement, validate, run CI checks, and report back. Use this when the user wants to change pysisense's own source code (not write a script that uses it) and wants the whole guided process rather than a single quick edit. Also use it to resume a cycle that was interrupted partway through, run via the individual stage skills (`debug`, `plan`, `struct`, `rethink`, `build`, `code`, `validate`, `ci`, `report`). For scripts that use pysisense as a library, use the `script` skill instead.
---

# Dev cycle orchestrator

Owns the pipeline for changing pysisense's own source. Runs each stage in order, keeps shared state on disk so the cycle survives across separate invocations (even separate sessions), and stops for the user whenever `debug` can't reproduce a reported problem or `rethink` decides a call is too risky to make alone.

## Stage order

```
[debug] → plan → struct → rethink → build → code → rethink → validate → ci → report
```

`debug` is **conditional**. Run it first only when the request is shaped like a bug report or fix ("X is broken", "this returns the wrong thing", "check if Y actually happens"). For a new-feature/refactor request, skip straight to `plan`. Deciding this is the orchestrator's job, before any state file exists: read the raw request, not the change type (that's `plan`'s output, and `plan` hasn't run yet). If it reads as "something is/might be wrong," run `debug` first.

`rethink` runs twice: after `struct` (is this change safe to make at all, and how) and after `code` (did the implementation actually stay inside those lines). Together with `debug`, these are the stages that can halt the cycle.

## Shared state

All state lives in `.pysisense-dev/` at the repo root. **Gitignore it** (add the entry if it's missing; this is working state, not a deliverable).

```
.pysisense-dev/
  cycle.json     # machine state, see schema below
  debug.md       # debug stage output (only if debug ran)
  plan.md        # plan stage output
  struct.md      # struct stage output
  report.md      # report stage output (final)
```

`cycle.json` schema:

```json
{
  "task": "one-line description of the request",
  "status": "in_progress | needs_input | done | failed",
  "current_stage": "debug | plan | struct | rethink_struct | build | code | rethink_code | validate | ci | report",
  "stages_completed": ["debug", "plan", "struct"],
  "debug_result": "reproduced | partial | not_reproduced | skipped",
  "needs_input": {
    "stage": "debug",
    "reason": "why this needs a human decision"
  },
  "test_files": ["tests/unit/unit_test_foo.py"],
  "code_files": ["pysisense/foo/bar.py"],
  "validate_result": "pass | fail | null",
  "ci_result": "pass | fail | null",
  "notes": ["low-stakes ambiguities rethink logged along the way"]
}
```

Set `debug_result: "skipped"` (and don't run `debug` at all) for a non-bug-shaped request. The field still exists so `report` can say plainly whether a repro check happened.

Every stage reads `cycle.json` before starting and updates it before finishing. Treat it as the source of truth for what's already been done. Never redo a stage whose output is already recorded, unless the user explicitly asks to redo it.

## Running the cycle

1. On a fresh request: decide whether this is bug-shaped. If so, create `.pysisense-dev/` and `cycle.json` (`status: in_progress`, `current_stage: debug`) and run `debug` first. Otherwise create the same state with `current_stage: plan` and `debug_result: "skipped"`, and run `plan` directly.
2. After each stage completes, update `cycle.json`, then move to the next stage in the order above.
3. Whenever `debug` reports `not_reproduced`, or `rethink` sets `status: needs_input`, **stop**. Summarize `needs_input.reason` to the user in plain language and wait. Do not guess and continue. Resume from the same `current_stage` once the user responds.
4. Whenever `validate` or `ci` reports `fail`, go back to `code` (do not silently patch and skip validation) rather than proceeding to the next stage.
5. On success, always end with `report`. Never skip the final human-facing summary, even for a small change.

## Resuming a manually run stage

If the user (or Claude, in an earlier turn) ran an individual stage skill directly instead of through this one, `.pysisense-dev/cycle.json` will already exist with some `stages_completed`. Read it, don't restart from `debug`/`plan`. Pick up at whatever `current_stage` says is next. If no state file exists yet, that manual stage run wasn't part of a cycle (a one-off "just show me the impact analysis" or "just check if this repros" ask). Start a fresh cycle only if the user now wants the full pipeline; otherwise fold that stage's output in as context and proceed from there.

## When something isn't covered here

Each stage's own `SKILL.md` (`debug`, `plan`, `struct`, `rethink`, `build`, `code`, `validate`, `ci`, `report`) is the source of truth for what that stage actually does. This file only owns sequencing and shared state.
