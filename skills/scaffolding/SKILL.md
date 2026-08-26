---
name: scaffolding
description: Use when the user wants to start a brand-new pysisense automation script as its own small project — creating the project folder, `uv init`, a `config.example.yaml` template, a README skeleton, and `.gitignore` — before any script logic is written. Triggers on requests like "set up a new project for a script that does X", "scaffold a pysisense script", or as the first step whenever the `scripting` skill determines a new script needs its own project shell (not a quick inline snippet or an addition to an existing project). Not for writing the actual script logic — use `/pysisense:scripting` for that, after scaffolding.
---

# pysisense script project scaffolding

Create the project shell for a new **pysisense** automation script — folder structure, dependency setup, and config templates. This skill only sets up the empty shell; it does not write the script's actual logic. Once scaffolding is done, hand off to the `scripting` skill (`/pysisense:scripting`) to fill in `<script_name>.py`.

A pysisense automation script is a deliverable someone else may run, rerun, and hand off — not a scratch snippet. Unless the user explicitly asks for a quick inline snippet, or is clearly just pasting into a REPL or an existing project, scaffold it as its own small project:

```
transfer-dashboards/
├── README.md               # what it does, prerequisites, how to configure and run it
├── pyproject.toml          # created by `uv init`
├── config.example.yaml     # template — placeholder domain/token, safe to commit
├── config.yaml             # real credentials — gitignored, never committed
├── .gitignore              # config.yaml, logs/, .venv/
└── transfer_dashboards.py
```

## Create the project with `uv` (preferred over raw `pip`/`venv`)

```bash
uv init transfer-dashboards
cd transfer-dashboards
uv add pysisense
uv run transfer_dashboards.py
```

If `uv` isn't available, fall back to `pip`:

```bash
mkdir transfer-dashboards && cd transfer-dashboards
python -m venv .venv && source .venv/bin/activate
pip install pysisense
python transfer_dashboards.py
```

## `config.example.yaml`

A placeholder template committed alongside the script, mirroring this repo's own `config.example.yaml`:

```yaml
domain: "your-domain.com"
is_ssl: true
token: "<your_api_token>"
```

The real `config.yaml` (or `source.yaml`/`target.yaml` for a migration script) is created by the user from that template and must be `.gitignore`d — same rule as this repo's own README: never commit real tokens.

## `.gitignore`

At minimum: `config.yaml` (and `source.yaml`/`target.yaml` for migration scripts), `logs/`, `.venv/`.

## `README.md`

For the script itself — not this plugin's docs — covering: what the script does, the config file it expects, how to run it (`uv run ...`), and if the script is destructive/bulk, how its dry-run flag works and what to check in the preview before flipping it live.

## When to skip scaffolding

For a one-off answer the user clearly just wants pasted into a REPL or an existing file, skip scaffolding entirely and go straight to the `scripting` skill — use judgment based on how the request is phrased.

## Next step

Once the shell exists, switch to `/pysisense:scripting` to write the actual script logic (conventions, API references, worked examples).
