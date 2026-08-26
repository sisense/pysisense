---
name: pysisense
description: Use whenever the user asks for anything involving the pysisense SDK or Sisense automation, and it's not yet clear whether they need a new project scaffolded, script logic written, or both — e.g. a first ask like "write a script to transfer dashboards from A to B" or "help me automate X in Sisense". Routes to the `scaffolding` skill (new project setup) and/or the `scripting` skill (script logic, conventions, API references, worked examples). If the request is unambiguously about one or the other, prefer invoking that skill directly (`/pysisense:scaffolding` or `/pysisense:scripting`) instead of this one.
---

# pysisense plugin — entry point

This plugin teaches Claude to write correct, production-safe automation scripts against the **pysisense SDK** (the official Python wrapper around the Sisense REST API used throughout this repository).

It's split into two focused skills:

| Skill | Use for |
|---|---|
| [`scaffolding`](../scaffolding/SKILL.md) (`/pysisense:scaffolding`) | Setting up a brand-new script's project shell — folder, `uv init`, `config.example.yaml`, README, `.gitignore`. |
| [`scripting`](../scripting/SKILL.md) (`/pysisense:scripting`) | Writing the actual script logic — conventions (dry-run safety, error handling, ID resolution), the module reference table, and worked examples. |

## What to do when this skill fires

1. If the request is for a **brand-new script** and there's no existing project shell to work in, start with the `scaffolding` skill to create the project, then move to the `scripting` skill to write the logic into it.
2. If the request is to **add to, fix, or extend an existing script**, or is clearly a quick inline snippet the user wants pasted into a REPL or existing file, skip straight to the `scripting` skill.
3. When genuinely unsure which applies, read both `SKILL.md` files linked above before writing anything — they're short.

Never invent pysisense method names or endpoints. If a needed capability isn't covered by the `scripting` skill's reference table, grep `pysisense/<module>/*.py` in this repo before guessing.
