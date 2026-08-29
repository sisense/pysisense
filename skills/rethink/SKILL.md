---
name: rethink
description: Checkpoint/gate skill — evaluates confidence in the decision just made after the riskiest stages of the pysisense dev cycle (`struct` and `code`), not after every stage. Low-stakes ambiguity gets logged as a note for the final report; high-stakes ambiguity (breaking change, deleted code, altered public API) halts the cycle for the user to decide. Invoked by `orchestrator` — invoke directly only to re-check a decision you're already unsure about.
---

# rethink — confidence gate

Runs twice in a cycle, evaluating a different question each time. It does not re-do the prior stage's work — it judges the *decision*, not the *execution*.

## After `struct`

Question: **is this classification actually safe to act on without asking the user first?**

- The change is **not breaking**, or is breaking but unambiguously trivial (e.g. an unreleased/internal-only helper with zero external callers, explicitly confirmed) → low-stakes. Log a one-line note (e.g. "confirmed non-breaking: return shape gained a field, no callers depend on its absence") to `cycle.json`'s `notes` array and let the cycle continue.
- The change **is breaking** in a way that affects a public method's signature or return shape, **deletes** code, or **alters a public API** in any way a downstream user of this SDK could be relying on → high-stakes. Set `status: needs_input` and `needs_input.reason` in `cycle.json` to a specific, answerable question (not "is this ok?" — e.g. "`get_users_with_role_names_and_group_names` will start returning `None` instead of a stale role ID when the role was deleted — is losing that stale ID acceptable, or should it be preserved?"). Halt.

## After `code`

Question: **did the implementation actually stay inside what `plan` and `struct` said it would?**

- Check the diff against `plan.md`'s scope and definition of done, and against `struct.md`'s affected-files list.
- Implementation matches scope, no unplanned deletions, no signature drift beyond what `struct` already classified → low-stakes (or nothing to note at all).
- Implementation quietly deleted something not called out in `struct`, changed a signature `struct` didn't flag, or expanded scope beyond `plan` (e.g. touched a second module "while I was in there") → high-stakes. Same halt behavior as above — specific question, `needs_input`, stop.

## Tiering rule of thumb

The tiering question is always: **could a wrong call here break someone else's code, silently lose data/information, or require redoing significant work if wrong?** If yes, it's high-stakes regardless of how small the change looks. If a wrong call is cheap to notice and cheap to reverse, it's low-stakes — note it and move on.

## Handoff

- Low-stakes: update `cycle.json` (`notes` appended, `current_stage` advances to whatever follows — `build` after struct's rethink, `validate` after code's rethink).
- High-stakes: update `cycle.json` (`status: needs_input`), and stop. Do not advance `current_stage`. `orchestrator` resumes from here once the user answers.
