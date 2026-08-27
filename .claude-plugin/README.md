# pysisense Claude Code plugin

Teaches Claude Code to write correct `pysisense` SDK scripts (dashboards,
users, groups, admin operations) using this repo's real API surface and
conventions, and to safely extend the SDK itself. See
[`plugin.json`](plugin.json) for the manifest and the four independent
skills below — there's no umbrella skill; each triggers on its own
description:
[`../skills/scaffold/SKILL.md`](../skills/scaffold/SKILL.md),
[`../skills/config/SKILL.md`](../skills/config/SKILL.md),
[`../skills/script/SKILL.md`](../skills/script/SKILL.md), and (for SDK
maintainers, not script authors)
[`../skills/dev/SKILL.md`](../skills/dev/SKILL.md).

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
  dev/
    SKILL.md            # editing pysisense's own source — mixin/docstring/docs-sync workflow checklist
```

One module has no dedicated reference yet: `sisenseclient.py` itself (the base
HTTP client) is covered inline in `auth.md` rather than its own file, since
its surface is small and mostly about the config/init boilerplate every other
reference already assumes.

Four independent skills, invoked as `/pysisense:scaffold` (new-project
setup), `/pysisense:config` (connection YAML), `/pysisense:script`
(script logic + API references + worked examples), and `/pysisense:dev`
(contributing to the SDK itself) — the slash-command suffix comes from each
skill's directory name under `skills/`. There's no umbrella/router skill:
each one's frontmatter `description` is written to auto-trigger on its own
without needing a central dispatcher.

`skills/` lives as a **sibling** of `.claude-plugin/`, not nested inside it —
that's the layout Claude Code's plugin loader expects by default; only the
manifest belongs inside `.claude-plugin/`.

## Install for local testing (before publishing)

From the repo root:

```bash
claude --plugin-dir .
```

`--plugin-dir` must point at the **plugin root** — the directory that
contains `.claude-plugin/` (i.e. this repo's root, since `.claude-plugin/`
and `skills/` both live there).

Then try a prompt that should auto-trigger the skill, for example:

```
write a script to transfer all dashboards from alice@example.com to bob@example.com
```

Confirm that:

- A skill triggers automatically (no need to name it explicitly) —
  `script` for a direct "write me a script" ask.
- Claude reads `script/SKILL.md` and pulls in the right `references/*.md`
  file for the task (dashboards vs. users/groups vs. auth).
- Generated code matches this repo's real method signatures — cross-check
  against `pysisense/<module>/*.py` or `examples/<module>_example.md` if
  anything looks off.
- For a "set up a new script project for X" style ask, `scaffold` triggers
  and creates the project shell (folder, `uv init`, config template, README,
  `.gitignore`) before any script logic is written.
- For a config/connection question ("SSL error connecting to Sisense", "what
  port for a Windows deployment"), `config` triggers with the field reference
  and troubleshooting table.
- For "add a method to Dashboard" or similar, `dev` triggers instead of
  `script` — check it points at the mixin lookup table and docs/examples
  sync steps, not at writing a standalone script.
- Explicit invocation also works: `/pysisense:script`, `/pysisense:scaffold`,
  `/pysisense:config`, `/pysisense:dev`.

## Installing for regular use

Once verified, install the plugin the same way as any other local Claude Code
plugin — add this repo as a plugin source (or, for team distribution, publish
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
`CLAUDE.md` — the skill is effectively a third, Claude-facing copy of that
same "how do I call this API" knowledge, and it goes stale the same way.
