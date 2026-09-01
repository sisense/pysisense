# pysisense Claude Code plugin

Teaches Claude Code to write correct `pysisense` SDK scripts (dashboards,
users, groups, admin operations) using this repo's real API surface and
conventions, and to safely extend the SDK itself via a structured dev cycle.
See [`plugin.json`](plugin.json) for the manifest. There's no umbrella
skill; every skill below triggers on its own description.

**Writing scripts that use pysisense:**
[`../skills/scaffold/SKILL.md`](../skills/scaffold/SKILL.md),
[`../skills/config/SKILL.md`](../skills/config/SKILL.md),
[`../skills/script/SKILL.md`](../skills/script/SKILL.md).

**Contributing to pysisense's own source** (a different audience: SDK
maintainers, not script authors). A 10-stage dev cycle, not a single skill:
[`../skills/orchestrator/SKILL.md`](../skills/orchestrator/SKILL.md),
[`debug`](../skills/debug/SKILL.md) (conditional), [`plan`](../skills/plan/SKILL.md),
[`struct`](../skills/struct/SKILL.md), [`rethink`](../skills/rethink/SKILL.md),
[`build`](../skills/build/SKILL.md), [`code`](../skills/code/SKILL.md),
[`validate`](../skills/validate/SKILL.md), [`ci`](../skills/ci/SKILL.md),
[`report`](../skills/report/SKILL.md).

## Layout

```
.claude-plugin/
  plugin.json          # plugin manifest
  README.md            # this file
skills/
  scaffold/
    SKILL.md            # new-project setup: folder shell, uv init, config template, README, .gitignore
  config/
    SKILL.md            # connection YAML: fields, generation, validation, troubleshooting
  script/
    SKILL.md            # trigger conditions, conventions, worked examples
    references/
      auth.md              # connection/auth boilerplate
      dashboards.md        # dashboard/widget/share/ownership API reference
      users_groups.md      # user/group/folder-ownership API reference
      datamodel.md         # data model/connection/build/RLS/shares API reference
      folder.md            # folder CRUD/structure API reference
      custom_code.md       # custom-code notebook API reference
      blox.md               # BloX action/widget-style API reference
      metadata.md          # datasource metadata/schema introspection API reference
      queries.md           # JAQL/SQL query execution API reference
      encryption.md        # connection-credential encrypt/decrypt API reference
      migration.md         # groups/users/dashboards/data-models migration API reference
      mergetool.md         # full cross-environment migration API reference (superset of migration.md)
      plugins.md           # plugin enable/disable/snapshot API reference
      report_manager.md    # scheduled report CRUD/run API reference
      wellcheck.md          # dashboard/data-model health-check API reference
  orchestrator/
    SKILL.md            # owns the dev-cycle pipeline: sequencing, shared state (.pysisense-dev/), resume-from-gate
  debug/
    SKILL.md            # stage 0 (conditional, bug reports only): confirm the reported problem actually reproduces
  plan/
    SKILL.md            # stage 1: task definition, scope, change type, definition-of-done checklist
  struct/
    SKILL.md            # stage 2: impact/compatibility analysis, breaking-change classification
  rethink/
    SKILL.md            # gate: runs after struct and after code, halts the cycle on high-stakes ambiguity
  build/
    SKILL.md            # stage 3: TDD, write tests before implementation
  code/
    SKILL.md            # stage 4: implementation + required docs/examples sync
  validate/
    SKILL.md            # stage 5: run the test suite, report pass/fail
  ci/
    SKILL.md            # stage 6: local CI emulation (lint, format, docstrings, tests)
  report/
    SKILL.md            # stage 7 (final): human-facing summary of the whole cycle
```

One module has no dedicated reference yet: `sisenseclient.py` itself (the base
HTTP client) is covered inline in `auth.md` rather than its own file, since
its surface is small and mostly about the config/init boilerplate every other
reference already assumes.

Thirteen skills total, invoked as `/pysisense:<name>` where `<name>` is the
directory name under `skills/`. Three (`scaffold`, `config`, `script`) are
independent and cover writing automation scripts. The other ten form the
**dev cycle** for editing pysisense's own source. `orchestrator` sequences
them (`[debug] → plan → struct → rethink → build → code → rethink →
validate → ci → report`), keeping shared state in `.pysisense-dev/`
(gitignored) so the cycle can pause and resume later, even in a different
session. `debug` only runs for bug-shaped requests, confirming the reported
problem actually reproduces before `plan` scopes a fix for it. `rethink`
runs after `struct` and after `code`. Either can halt the cycle for the
user. Each stage skill can also be invoked directly for a one-off ask, e.g.
`/pysisense:struct` just for an impact analysis, or `/pysisense:debug` to
check whether a suspected bug is real, without running the full cycle.

`skills/` lives as a **sibling** of `.claude-plugin/`, not nested inside it.
That's the layout Claude Code's plugin loader expects by default; only the
manifest belongs inside `.claude-plugin/`.

## Install for local testing (before publishing)

From the repo root:

```bash
claude --plugin-dir .
```

`--plugin-dir` must point at the **plugin root**, the directory that
contains `.claude-plugin/` (i.e. this repo's root, since `.claude-plugin/`
and `skills/` both live there).

Then try a prompt that should auto-trigger the skill, for example:

```
write a script to transfer all dashboards from alice@example.com to bob@example.com
```

Confirm that:

- A skill triggers automatically (no need to name it explicitly):
  `script` for a direct "write me a script" ask.
- Claude reads `script/SKILL.md` and pulls in the right `references/*.md`
  file for the task (dashboards vs. users/groups vs. auth).
- Generated code matches this repo's real method signatures. Cross-check
  against `pysisense/<module>/*.py` or `examples/<module>_example.md` if
  anything looks off.
- For a "set up a new script project for X" style ask, `scaffold` triggers
  and creates the project shell (folder, `uv init`, config template, README,
  `.gitignore`) before any script logic is written.
- For a config/connection question ("SSL error connecting to Sisense", "what
  port for a Windows deployment"), `config` triggers with the field reference
  and troubleshooting table.
- For "add a method to Dashboard" or similar, `orchestrator` triggers instead
  of `script`, and actually runs the full cycle in order. `.pysisense-dev/`
  gets created, `plan.md`/`struct.md` get written, and the cycle stops with a
  clear question if a breaking change is detected (test this deliberately by
  asking for a change to an existing public method's return shape).
- For a bug-report-shaped ask ("X returns the wrong thing", "this is
  broken"), `orchestrator` runs `debug` before `plan` and writes
  `.pysisense-dev/debug.md`. If the repro genuinely doesn't reproduce, the
  cycle stops and asks for more detail instead of quietly planning a fix
  anyway. For a plain feature/refactor ask, confirm `debug` is skipped
  (`debug_result: "skipped"` in `cycle.json`) rather than run unnecessarily.
- Explicit invocation also works for every stage individually, e.g.
  `/pysisense:struct` for a standalone impact analysis, or `/pysisense:debug`
  to just check whether a suspected bug reproduces, without running the
  rest of the cycle.

## Installing for regular use

Once verified, install the plugin the same way as any other local Claude Code
plugin: add this repo as a plugin source (or, for team distribution, publish
it via a plugin marketplace) and enable it per Claude Code's plugin docs.
There is no separate build step; the plugin is just this `.claude-plugin/` +
`skills/` pair.

## Updating the skill

When `pysisense`'s public API changes (new methods, changed signatures,
renamed fields), keep this plugin in sync:

1. Update the relevant `skills/script/references/*.md` file.
2. Update `skills/script/SKILL.md`'s worked examples if they call an
   affected method.
3. Re-test with `claude --plugin-dir .` before committing.

This mirrors the repo's own `examples/*.md` / `docs/*.md` maintenance rule in
`CLAUDE.md`. The skill is effectively a third, Claude-facing copy of that
same "how do I call this API" knowledge, and it goes stale the same way.
