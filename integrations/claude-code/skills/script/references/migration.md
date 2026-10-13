# Migration reference (cross-environment: groups, users, dashboards, data models)

`from pysisense import Migration`

```python
# Mode 1: YAML config files
migration = Migration(source_yaml="source.yaml", target_yaml="target.yaml", debug=False)

# Mode 2: pre-built SisenseClient instances
from pysisense import SisenseClient

src = SisenseClient(config_file="source.yaml")
tgt = SisenseClient(config_file="target.yaml")
migration = Migration(source_client=src, target_client=tgt)
```

Exactly one mode must be provided — `(source_client and target_client)` or `(source_yaml and target_yaml)`. Mixing or omitting both raises `ValueError` from `__init__`. `migration.logger` is the **source** client's logger.

For anything beyond groups/users/dashboards/data models — custom-code notebooks, folders, BloX actions, data security, saved formulas/filters — use `MergeTool` instead; see `references/mergetool.md`.

## Progress streaming with `_emit`

Every `migrate_all_*` method, plus `migrate_dashboards` and `migrate_datamodels`, accepts an optional `emit: Callable[[dict], None]` callback and calls it at each milestone with a structured event (`type`: `"started"|"progress"|"warning"|"error"|"completed"`, `step`, `message`, plus step-specific fields like counts/status codes). `_emit` swallows any exception the callback raises (logged at debug) so a broken UI callback can never break the migration itself.

```python
def my_progress_callback(event: dict) -> None:
    print(f"[{event['type']}] {event['step']}: {event['message']}")


migration.migrate_all_users(emit=my_progress_callback)
```

`migrate_groups`, `migrate_users`, and `migrate_dashboard_shares` do **not** accept `emit` — only their bulk/`_all` counterparts (and `migrate_dashboards`/`migrate_datamodels`) do.

## Migration order matters

**Groups → Users → Data models → Dashboards.** Each stage's payload is built by mapping source names/IDs to target IDs that must already exist:

- **Users** reference groups by name — migrate groups first or user group memberships silently resolve to an empty/partial list (unmapped group names are just dropped, not errored).
- **Dashboard shares/ownership** map source user/group IDs to target IDs by email/name lookup — if a shared user or group hasn't been migrated yet, that share entry is skipped (`user_mapping`/`group_mapping` returns `None` for it) rather than failing loudly.
- **Data model shares** do the same email/name-based user and group mapping before applying permissions.
- **Dashboards** reference data models via widget queries — migrate data models before dashboards or the dashboards import successfully but their widgets have nothing to query against in the target.

## Groups

| Method | Scope | Notes |
|---|---|---|
| `migrate_groups(group_name_list)` | Named subset | No exclusion filtering — pass `"Everyone"`/`"Admins"` and it will try to migrate them too (likely to conflict, since those exist by default on target). |
| `migrate_all_groups(emit=None)` | Everything | Automatically excludes `Admins`, `All users in system`, `Everyone`, and resolves the source's **system tenant** so only system-tenant groups are migrated (skips multi-tenant groups). |

Both POST to the target's `/api/v1/groups/bulk` with `created`/`lastUpdated`/`tenantId`/`_id` stripped from each group record.

**Return shape differs by path.** `migrate_groups` returns either a bare `list[dict]` with an informational `{"message": ...}` entry (source fetch failed, or no name matched) or a `dict` with `{"results": [...], "total_count": int, "raw_error": ...}` on the bulk-POST path — check `isinstance(result, dict)` before indexing. `migrate_all_groups` always returns the richer, consistent shape: `{"ok": bool, "status": "success"|"failed"|"noop", "results": [...], "source_count", "eligible_count", "success_count", "failed_count", "skipped_count", "skipped_multi_tenant_count", "raw_error", "warnings"}`.

```python
migration.migrate_groups(["mig_test", "mig_test_2"])
migration.migrate_all_groups()
```

## Users

| Method | Scope | Notes |
|---|---|---|
| `migrate_users(user_name_list)` | Named subset, by email | Resolves each user's role name and group names to target role/group IDs (fetching `/api/roles` and `/api/v1/groups` from target). Does **not** skip `super` (sysAdmin) users or filter by tenant. |
| `migrate_all_users(emit=None)` | Everything | Same role/group ID mapping, but also skips `super`-role users and non-system-tenant users, and tracks `missing_role_mappings_count`/`missing_group_mappings_count` for memberships that couldn't be resolved in target. |

Both POST to `/api/v1/users/bulk`; `"Everyone"`/`"All users in system"` are always excluded from the mapped `groups` list regardless of which method is used.

```python
migration.migrate_users(["john.doe@sisense.com"])
migration.migrate_all_users()
```

Same return-shape caveat as groups: `migrate_users` can return a bare `list[dict]` (`{"message": ...}`) on early-exit paths (source/target fetch failure, no match) or a `dict` with `results`/`total_count`/`raw_error` on the bulk-POST path. `migrate_all_users` always returns the consistent `ok`/`status`/counts shape.

## Dashboards

`migrate_dashboard_shares(source_dashboard_ids, target_dashboard_ids, change_ownership=False)` pairs the two ID lists **positionally** — same length required. Raises `ValueError` (not an error dict) if either list is empty/missing or the lengths don't match. Maps source share/owner IDs to target via email/name lookup; unmapped users or groups are dropped from the share payload. Returns `{"summary": {"total_dashboard_count", "total_share_success_count", "total_share_fail_count"}, "dashboard_results": [...]}`.

`migrate_dashboards(dashboard_ids=None, dashboard_names=None, action=None, republish=False, migrate_share=False, change_ownership=False, emit=None)` exports from source and bulk-imports into target in one request.

- Provide exactly one of `dashboard_ids` / `dashboard_names` — providing both, or neither, raises `ValueError`.
- `change_ownership=True` requires `migrate_share=True`, else `ValueError`.
- `action`: `"skip"` | `"overwrite"` | `"duplicate"` | `None` (Sisense default, typically skip). **Shares/ownership migration is silently skipped whenever `action` is `"overwrite"` or `"duplicate"`**, even if `migrate_share=True`.
- Returns `{"succeeded": [...], "skipped": [...], "failed": [...], "meta": {...}}` — each item carries `title`, `source_id`, `target_id` (`succeeded`/`skipped`) or `title`, `source_id`, `reason` (`failed`).

```python
migration.migrate_dashboards(
    dashboard_names=["Export to PDF text widget"],
    action="skip",
    republish=True,
    migrate_share=True,
    change_ownership=True,
)
```

`migrate_all_dashboards(action=None, republish=False, migrate_share=False, change_ownership=False, batch_size=10, sleep_time=10, emit=None)` paginates every dashboard on source, then calls `migrate_dashboards` per batch. If a batch raises, it enters **salvage mode**: retries that batch's dashboards one at a time with `action="skip"` to avoid duplicating anything already created, rather than failing the whole run.

## Data models

`migrate_datamodels(datamodel_ids=None, datamodel_names=None, provider_connection_map=None, dependencies=None, shares=False, action=None, new_title=None, emit=None)`

- Provide exactly one of `datamodel_ids` / `datamodel_names` — both or neither raises `ValueError`.
- `dependencies`: any of `"dataSecurity"`, `"formulas"`, `"hierarchies"`, `"perspectives"`, a list of those, or `None`/`"all"` (default — includes everything). **Ignored on Windows-hosted sources** — the Windows export endpoint has no dependencies parameter (logged as a warning per data model).
- `provider_connection_map`: `{"Databricks": "<target connection oid>", "GoogleBigQuery": "<target connection oid>"}` — remaps each dataset's connection to an existing target connection by provider. Providers **not** in the map have their connection `parameters` blanked out on import — credentials are never copied across environments, so unmapped connections need to be reconnected manually in the target afterward.
- `action`: `None` (create new), `"overwrite"` (reuses the source `oid` via `datamodelId`; falls back to plain create on 404, and returns a clear failure reason if the title already exists in target under a *different* ID), or `"duplicate"` (new title via `new_title`, defaulting to `"<original> (Duplicate)"`).
- `shares=True` migrates data security/permissions after the schema import, mapping users/groups by email/name. For `"live"` models this **publishes the target model first** (`POST /api/v2/builds`) before patching permissions — publishing failure means shares are skipped for that model.
- Returns `{"succeeded": [...], "skipped": [...], "failed": [...], "meta": {...}}`, with `meta` carrying `share_success_count`/`share_fail_count`/`share_details`/`failure_reasons` alongside export/import counts.

```python
migration.migrate_datamodels(
    datamodel_names=["pysense_databricks"],
    provider_connection_map={"Databricks": "53874a46-1360-45d8-a005-1cca41ef3e1c"},
    dependencies="all",
    shares=True,
)
```

`migrate_all_datamodels(dependencies=None, shares=False, batch_size=10, sleep_time=5, action=None, emit=None)` fetches every data model on source and migrates in batches, with the same per-batch salvage-mode retry as `migrate_all_dashboards` (falls back to migrating one-by-one on a batch exception, preserving whatever `action` was passed rather than forcing `"skip"`).
