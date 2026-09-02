# Changelog

All notable changes to `pysisense` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/) — breaking changes land only in a major release.

## [Unreleased]

### Deprecated

- **`CustomCode.rename_notebook_folder`** — fixed-path specialization of
  `rename_notebook_file` with no behavior of its own beyond the endpoint it builds.
  Use `rename_notebook_file(f"notebooks/custom_code_notebooks/notebooks/{old_id}/",
  payload)` directly. Marked with PEP 702 `__deprecated__`, behavior frozen.

## [2.0.1] — 2026-08-31

### Fixed

- **`get_user` and `get_users_all` now read group membership from the group side**, the same
  source `users_per_group` and the Sisense UI use. 2.0.0 moved `users_per_group` to that
  source but left the user readers on the user record, which Sisense does not populate with
  its derived groups (`Admins`, `All users in system`) — so the two canonical methods
  disagreed about the same person: `get_user(x)["GROUPS"]` returned `['Everyone']` for a user
  `users_per_group("Admins")` listed as a member.

  `GROUPS` and `GROUP_IDS` now include those derived memberships, so all three methods agree.
  Consumers counting entries in `GROUPS` will see larger lists for affected users. If the
  group fetch fails the fields fall back to the user record rather than coming back empty.

  Reported by the FES Assistant project.

- **Documented the rule for the universal groups**, which the fix above makes visible:
  targeted questions give complete answers, only the all-groups view filters.
  `get_user(email)["GROUPS"]` and `users_per_group("Everyone")` both report every group a
  user is in; `users_per_group()` omits `Everyone` and `All users in system` for readability.
  Do not derive one person's groups from the all-groups view — use `get_user`.

## [2.0.0] — 2026-08-31

**A focused release, not a rewrite.** The major version is here because some changes are
incompatible, not because the SDK was rebuilt — it is the signal that stops `pip install -U`
from upgrading anyone into them unasked. Pinning `pysisense>=1,<2` keeps you on 1.x.

Most code needs no changes: the canonical user row deliberately keeps its 1.x field names
**and meanings** (`ROLE_NAME`, `GROUPS`), and both are joined by new fields rather than
replaced. The upgrade list below is short.

Upgrading? Read the **[upgrade guide](docs/upgrading.md)** — it maps every change
old-to-new and starts with a symptom → cause → fix table.

### What you must change

The user row is **additive** — `ROLE_NAME` and `GROUPS` keep their 1.x names and meanings,
so role comparisons and group reads keep working. What needs action:

1. **`GROUPS` now includes `Everyone`**, which `get_users_all()` used to strip out. The key
   and its meaning are unchanged; only this value was added.
2. **Detect failures with `result.get("ok") is False`**, never by matching an exact key set.
3. **`get_unused_columns_bulk` returns a dict**, not a list — read `result["results"]`.
4. **`get_connections` was removed** — use `get_connections_all`.

### Breaking

- **Canonical user row** — `get_user()` and `get_users_all()` now return one shared shape:
  `USER_ID, USER_NAME, EMAIL, FIRST_NAME, LAST_NAME, IS_ACTIVE, ROLE_ID, ROLE_NAME,
  ROLE_DISPLAY_NAME, ROLE_RAW_NAME, GROUP_IDS, GROUPS`. In 1.x the two methods disagreed
  with each other on both the role vocabulary and the group fields; `ROLE_NAME` and
  `GROUPS` now mean the same thing in both.
- `Everyone` is no longer filtered out of group memberships. The SDK reports what Sisense
  stores; consumers decide what to hide.
- `get_users_all()` returns a plain error dict on failure instead of a list-wrapped
  `[{"error": ...}]`.
- **`users_per_group(group_name=None)`** returns flat membership rows, one per (group, user).
  `None` returns all memberships. Groups with no members contribute no rows, so the row
  count equals the real membership count. Membership is read from the group side
  (`GET /api/v1/groups?expand=users`) — the source the Sisense UI shows — so the
  auto-generated `Admins` and `All users in system` groups report their real members;
  Sisense does not expose those on the user side. `Everyone` and `All users in system` are
  omitted from the all-groups view by default (Sisense fills both with every user, so they
  duplicate `get_users_all`); naming one directly still returns its members. `users_per_group_all()` derived its `Admins` entry from
  users' roles instead of membership.
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
- **`get_user_email_and_group_name_maps` is now private** (`_get_user_email_and_group_name_maps`).
  It existed only to serve `get_all_dashboard_shares` and `Dashboard.get_dashboard_share`,
  returning raw ID-to-name lookup maps rather than answering a question anyone asks. Build the
  maps from `get_users_all()` and `get_groups()` if you were using it.

### Deprecated

Still functional, behavior frozen at the 1.x shape, marked with PEP 702 `__deprecated__`:
`get_user_with_role_and_group_names` → `get_user`;
`get_users_with_role_names_and_group_names` → `get_users_all`;
`get_users_expanded` → `get_users_all`; `get_group` → `get_groups`;
`users_per_group_all` → `users_per_group`; `get_unused_columns` → `get_unused_columns_bulk`.

### Added

- `ROLE_DISPLAY_NAME`, `ROLE_RAW_NAME` and `GROUP_IDS` on every canonical user row.
  `ROLE_NAME` and `GROUPS` keep their 1.x meanings (the UI role name and the group names);
  `ROLE_DISPLAY_NAME` restates `ROLE_NAME` unambiguously and `ROLE_RAW_NAME` carries the
  raw Sisense role value.
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
