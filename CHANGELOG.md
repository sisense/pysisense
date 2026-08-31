# Changelog

All notable changes to `pysisense` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/) — breaking changes land only in a major release.

## [2.0.0] — unreleased

Upgrading? Read the **[migration guide](docs/migration-2.0.md)** — it maps every change
old-to-new and starts with a symptom → cause → fix table.

### What you must change

1. **`ROLE_NAME` now holds the raw Sisense value** (`super` / `consumer` / `contributor`).
   The UI name moved to the new `ROLE_DISPLAY_NAME` (`sysAdmin` / `viewer` /
   `dashboardDesigner`). **This one fails silently** — `ROLE_NAME == "sysAdmin"` now matches
   zero users instead of raising.
2. **`GROUPS` is gone.** Use `GROUP_NAMES` (names) or `GROUP_IDS` (IDs). Note `GROUP_NAMES`
   includes `Everyone`, which `get_users_all()` used to strip out.
3. **Detect failures with `result.get("ok") is False`**, never by matching an exact key set.
4. **`get_unused_columns_bulk` returns a dict**, not a list — read `result["results"]`.
5. **`get_connections` was removed** — use `get_connections_all`.

### Breaking

- **Canonical user row** — `get_user()` and `get_users_all()` now return one shared shape:
  `USER_ID, USER_NAME, EMAIL, FIRST_NAME, LAST_NAME, IS_ACTIVE, ROLE_ID, ROLE_NAME,
  ROLE_DISPLAY_NAME, GROUP_IDS, GROUP_NAMES`. In 1.x the two methods disagreed with each
  other on both the role vocabulary and the group fields.
- `Everyone` is no longer filtered out of group memberships. The SDK reports what Sisense
  stores; consumers decide what to hide.
- `get_users_all()` returns a plain error dict on failure instead of a list-wrapped
  `[{"error": ...}]`.
- **`users_per_group(group_name=None)`** returns flat membership rows, one per (group, user).
  `None` returns all memberships. Groups with no members contribute no rows, so the row
  count equals the real membership count. The synthetic `Admins` bucket is gone.
- **Every failure dict carries `"ok": False`** — the self-identifying, forward-compatible
  failure marker.
- **Failure shapes converged.** `get_data`, `get_dashboard_share`, `get_dashboard_columns`,
  `get_datamodel_shares`, `get_datasecurity` and `get_datasecurity_detail` no longer return
  `[]` on failure; `create_connections` no longer returns `None`; `add_dashboard_shares`,
  `add_dashboard_script` and `add_widget_script` no longer return message strings. An empty
  list from a read method now always means a genuinely empty result.
- Those three write methods also return dicts on **success**:
  `{"success": True, "message": ...}`, with `new_shares` / `updated_shares` counts for
  `add_dashboard_shares`.
- **`get_unused_columns_bulk`** always returns `{"results": [...], "errors": [{"ref", "error"}]}`.
  Total failure adds `"ok": False` and a top-level `"error"`. The `failed_references` key was
  renamed to `errors`.
- **`get_groups(name=...)`** with an unknown name returns an error dict naming it, instead of
  an empty list. The unfiltered call still returns `[]` when the server has no groups.
- **Removed** `get_connections` (deprecated in 1.1.0) — use `get_connections_all`.

### Deprecated

Still functional, behavior frozen at the 1.x shape, marked with PEP 702 `__deprecated__`:
`get_user_with_role_and_group_names` → `get_user`;
`get_users_with_role_names_and_group_names` → `get_users_all`;
`get_users_expanded` → `get_users_all`; `get_group` → `get_groups`;
`users_per_group_all` → `users_per_group`; `get_unused_columns` → `get_unused_columns_bulk`.

### Added

- `ROLE_DISPLAY_NAME` on every canonical user row.
- `get_groups(name=...)` — exact-match server-side group lookup.
- **`create_user` / `update_user` accept both role vocabularies** — raw (`super`), UI
  (`sysAdmin`), or a human phrasing (`sys admin`, `System Administrator`), matched ignoring
  case, spaces and punctuation. Previously `sysAdmin` and `dashboardDesigner` were rejected,
  so a role read from a user could not be written back. Roles the instance defines itself
  (`dataDesigner`, `dataAdmin`, `admin`, `custom_*`) always win over an alias, so `admin`
  never resolves to `super` and `data designer` never resolves to `contributor`. An
  unmatched name now reports the roles the instance actually has.
- **`add_datamodel_shares` supports EXTRACT models** — the "Fixing Bug… will be fixed in V2"
  error is retired. Returns `{"success": True, "message", "new_shares", "updated_shares",
  "skipped"}`, where `skipped` names every requested share that was not submitted.
- `raw_body` on failure dicts: when a server error body cannot be recognised, `error` stays a
  clean sentence and the redacted, truncated dump travels in `raw_body`.

### Fixed

- Shares for **inactive users** are no longer submitted to Sisense, which accepts them
  (HTTP 200) and then silently drops them — they are reported in `skipped` instead.
- Both EXTRACT and LIVE data model permissions are written with the `partyId` key
  (live-verified; a `party`-keyed entry is silently discarded).
- `add_datamodel_shares` fails loudly when none of the requested shares resolve, instead of
  writing the existing shares back unchanged and reporting success.
- Datasecurity readers return `[]` for a rule-less model instead of a fabricated blank row,
  so row counts always equal real rule counts.
- `get_all_dashboard_shares` no longer emits placeholder rows for unshared dashboards.
- `WellCheck`'s class docstring no longer claims unused-column analysis, which lives on
  `AccessManagement`.

---

Releases before 2.0.0 are not recorded here; see the
[commit history](https://github.com/sisense/pysisense/commits/main) and
[GitHub releases](https://github.com/sisense/pysisense/releases).
