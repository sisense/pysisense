# MergeTool reference — cross-environment content migration

`from pysisense import MergeTool` — connects to a source and target Sisense environment and copies content between them.

```python
# YAML config files
merge = MergeTool(source_yaml="source.yaml", target_yaml="target.yaml", debug=False)

# Or pass SisenseClient instances directly
from pysisense import SisenseClient
src = SisenseClient(config_file="source.yaml")
tgt = SisenseClient(config_file="target.yaml")
merge = MergeTool(source_client=src, target_client=tgt)
```

Same two init modes as `Migration` (see `references/auth.md`), and `source_client`/`target_client` take precedence over the YAML args whenever both are given.

## MergeTool vs. Migration

`Migration` covers four asset types: groups, users, data models, and dashboards — see `references/migration.md` for that narrower workflow and its ordering rationale. `MergeTool` is the superset used for full environment merges: it adds custom-code notebooks, folders, BloX actions, data security, and saved formulas/filters, plus two things `Migration` doesn't have — a `concurrency` parameter (parallel per-item processing) and an explicit `action="skip"|"overwrite"|"duplicate"` conflict-resolution parameter on every migrate method. Reach for `MergeTool` whenever the source environment has notebooks, folders, BloX actions, RLS rules, or saved formulas/filters that also need to move; reach for `Migration` when only groups/users/datamodels/dashboards are in scope.

## The `action` parameter (shared pattern)

Every `migrate_X`/`migrate_all_X` method takes `action: Literal["skip", "overwrite", "duplicate"] = "skip"`, resolved per item against a conflict key (varies by asset type — see the table below):

| Value | Behavior |
|---|---|
| `"skip"` (default) | Leave the existing item on target unchanged. |
| `"overwrite"` | Delete/replace the existing item on target, then recreate from source. |
| `"duplicate"` | Always create a new item, regardless of conflicts. |

Two asset types can't honor `"overwrite"` literally: **saved formulas and saved filters** have no update/delete endpoint in the Sisense metadata API, so `"overwrite"` behaves identically to `"duplicate"` for `migrate_saved_formulas`/`migrate_saved_filters`. Everywhere else `"overwrite"` does a real delete-then-recreate.

**Conflict keys by asset type** — this is what "already exists on target" is matched against:

| Asset | Conflict key |
|---|---|
| Notebooks | `displayName` |
| Folders | full `parent/child` path (not name alone — same-named folders in different branches are independent) |
| BloX actions | `type` |
| Groups | `name` |
| Users | `email` |
| Data models | `title` (not OID) |
| Datasecurity | n/a — always rewrites the target model's rules |
| Saved formulas / filters | `title` |
| Dashboards | `oid` (not title) — safe to re-run `"skip"` after a partial migration |

## Concurrency

Every method except groups/users/datasecurity/formulas/filters (which write via bulk or per-item loops with no independent parallel step) accepts `concurrency: int = 1`. Internally this goes through a private shared scheduler (`_run_concurrently` in `mergetool/base.py`) — not something scripts call directly. Values `<= 1` process items one at a time in a plain loop (default, matches pre-concurrency behavior exactly). Values `> 1` run each item's migration in a background thread via `asyncio.to_thread`, bounded by an `asyncio.Semaphore(concurrency)`, since the SDK's HTTP client (`requests`) is synchronous — this is a concurrency *scheduler*, not non-blocking I/O.

Folders are the one exception worth knowing: they're batched by hierarchy depth level, and only folders within the *same* depth run concurrently — a child's target parent OID must exist before it can be created, so depth levels remain a hard barrier. If `_run_concurrently` is called from code already inside a running asyncio event loop, it falls back to sequential processing for that batch and logs a warning (a nested event loop can't be started via `asyncio.run`).

## Asset type -> methods -> mixin file

| Asset | Methods | Mixin file |
|---|---|---|
| Notebooks | `migrate_notebooks`, `migrate_all_notebooks` | `mergetool/custom_code.py` |
| Folders | `migrate_folders`, `migrate_all_folders` | `mergetool/folder.py` |
| BloX actions | `migrate_blox_actions`, `migrate_all_blox_actions` | `mergetool/blox.py` |
| Groups | `migrate_groups`, `migrate_all_groups` | `mergetool/groups.py` |
| Users | `migrate_users`, `migrate_all_users` | `mergetool/users.py` |
| Data models | `migrate_datamodels`, `migrate_all_datamodels` | `mergetool/datamodels.py` |
| Data security (RLS) | `migrate_datasecurity`, `migrate_all_datasecurity` | `mergetool/datasecurity.py` |
| Saved formulas | `migrate_saved_formulas`, `migrate_all_saved_formulas` | `mergetool/formulas.py` |
| Saved filters | `migrate_saved_filters`, `migrate_all_saved_filters` | `mergetool/filters.py` |
| Dashboards | `migrate_dashboards`, `migrate_all_dashboards` | `mergetool/dashboards.py` |

BloX migration additionally requires the **target** to be a Linux deployment — saving/deleting BloX actions is Linux-only; the method returns `{"error": "...not supported on Windows..."}` rather than hitting a wrong endpoint.

## Return shape

Every `migrate_X`/`migrate_all_X` method returns the same summary dict shape (field names vary slightly by asset — e.g. `succeeded` entries carry `name`/`email`/`title` depending on type):

```python
{
    "ok": bool,
    "status": "success" | "failed" | "noop",
    "succeeded": [...],   # one entry per migrated item
    "skipped": [...],     # each has a "reason"
    "failed": [...],      # each has a "reason"
    "source_count": int,
    "succeeded_count": int,
    "skipped_count": int,
    "failed_count": int,
}
```

`"noop"` means nothing matched on source (not an error). `ok` is `True` only when `source_count > 0` and `failed_count == 0` (for formulas/filters, `ok` is based on `failed_count == 0` alone, since a datamodel with zero saved formulas is still a legitimate no-op per-model). `migrate_notebooks`/`migrate_folders`/`migrate_datamodels`/`migrate_datasecurity`/`migrate_saved_formulas`/`migrate_saved_filters`/`migrate_dashboards` raise `ValueError` if you pass both or neither of the `_ids`/`_names` selector pair; `migrate_blox_actions`/`migrate_groups`/`migrate_users` take a single optional filter list instead (no such pairing, so no raise).

An optional `emit: Callable[[dict], None]` progress callback is accepted everywhere; each call carries at least `type`, `step`, `message`. With `concurrency > 1`, `emit` may be invoked from multiple worker threads concurrently.

## Migration order

Order still matters exactly as it does for `Migration` (see `references/migration.md`'s ordering rationale) — groups before users, users/groups/folders/data models before dashboards. `MergeTool` layers three more ordering rules on top:

- **Data models before data security, saved formulas, and saved filters** — all three write onto a target data model that must already exist (`migrate_datasecurity`/`migrate_saved_formulas`/`migrate_saved_filters` skip any datamodel not found on target, with `"reason": "Data model not found on target. Migrate the data model first."`).
- **Folders before dashboards** — so each dashboard can be placed into the target folder whose path matches its source parent; if that folder path isn't found, the dashboard is left at the root (not failed).
- **Data models before dashboards** — dashboard widgets reference their data model's local Elasticube by title.

## Worked pattern: notebooks with skip vs. overwrite

```python
results = merge.migrate_notebooks(notebook_names=["ETL Pipeline"], action="overwrite")
# action="overwrite" deletes the existing target notebook (matched by displayName) then recreates it
print(results["status"], results["succeeded_count"], results["failed_count"])

# Full-environment pass with parallelism — notebooks are independent of each other
results = merge.migrate_all_notebooks(action="skip", concurrency=5)
```

## Worked pattern: folders (subtree expansion + depth-batched concurrency)

```python
# Migrating a folder by name pulls in its entire subtree automatically
results = merge.migrate_folders(folder_names=["Analytics"], action="skip", concurrency=5)
# concurrency here parallelizes *within* each depth level only — parents always finish before children start.
# action="overwrite" on a folder deletes it AND every dashboard inside it on the target before recreating.
```

## Worked pattern: groups then users (ID resolution across environments)

```python
merge.migrate_all_groups(action="skip")     # groups first — user payloads reference target group IDs
merge.migrate_all_users(action="skip", ignore_custom_roles=False)
```

`migrate_users` resolves each user's role to a target role ID, mirroring the legacy merge tool: on a multi-tenant target (its role list includes `tenantAdmin`), source `super`/`admin` map to `tenantAdmin`; on a Windows target, source `tenantAdmin` maps back to `admin`. `ignore_custom_roles=True` strips a `custom_` prefix from source role names before matching. A user whose role can't be resolved on target lands in `failed`, not `skipped`. `migrate_all_users` excludes built-in `super`-role accounts (each environment is expected to have its own).

## Worked pattern: data models with dependencies, connection remap, and shares

```python
results = merge.migrate_datamodels(
    datamodel_names=["Sales Elasticube"],
    action="skip",
    dependencies="all",  # or a list: ["dataSecurity", "formulas", "hierarchies", "perspectives"]
    provider_connection_map={"Athena": "target-connection-oid"},
    shares=True,  # also remap and migrate shares by email/name after a successful import
    concurrency=5,
)
```

Connection credentials are **never** copied as-is. A dataset's provider is repointed to `provider_connection_map[provider]` when present; otherwise its connection `parameters` are stripped and must be reconnected manually on the target. `dependencies` has no effect when the **source** is Windows — its export endpoint accepts no dependencies parameter.

## Worked pattern: datasecurity, formulas, filters (post-datamodel passes)

```python
merge.migrate_all_datamodels(action="skip")     # must run first
merge.migrate_all_datasecurity()                # rules remapped by email (users) / name (groups)
merge.migrate_all_saved_formulas(action="skip")
merge.migrate_all_saved_filters(action="skip")
```

None of these three accept `concurrency` — they loop per data model rather than dispatching independent worker items.

## Worked pattern: full dashboard migration with progress callback

```python
def on_progress(event: dict) -> None:
    print(f"[{event.get('type', '').upper()}] {event.get('step')} — {event.get('message')}")

results = merge.migrate_dashboards(
    dashboard_names=["Sales Overview"],
    action="skip",
    concurrency=5,
    emit=on_progress,
)
```

Per dashboard, after a successful import: the embedded datasource reference is repointed to the target's local Elasticube, owner is remapped by email, shares are remapped by email/name, and the dashboard is moved into the target folder matching its source parent's path (if that folder was already migrated). Any of these post-import steps failing silently downgrades to a warning log — the dashboard itself still counts as `succeeded`.
