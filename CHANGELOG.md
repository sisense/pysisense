# Changelog

All notable changes to `pysisense` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/) — breaking changes land only in a major release.

## [Unreleased]

### Changed

- **`get_dashboard_columns` and `get_unused_columns_bulk` now see the whole dashboard.** The
  shared walk (`_extract_dashboard_references`) reads, in addition to dashboard filters and
  widget panel items: default filters, measured filters (`filter.by`), **drill hierarchies**
  (2,357 field references across 509 dashboards on one instance that were never counted),
  widget drill history, formulas nested inside formulas at any depth, conditional-formatting
  expressions, `jaql.dimension` wrappers, drill chains (`parent`/`through`), the `query.metadata`
  block some widgets carry, and table-widget headers — plus a safety net that keeps and flags a
  reference found anywhere else. Rows keep their shape; `source` gains the value `"hierarchy"`.
  `get_unused_columns_bulk` no longer counts widgets or filters that point at a different
  datasource than the model being analysed (a third of widgets on one instance did). A panel
  item with no `dim` no longer yields a fabricated `Unknown` / `Table` row.

### Added

- **`DataModel.get_perspectives(perspectives=None, datamodel=None, include_default=False)`** — list
  every perspective on the instance, those built over one root model, or look specific ones up by
  name or `oid`, in one method; each object also carries `datamodelTitle`. Perspectives are metadata-only views over a root data model that
  keep a subset of its tables and columns. The hidden per-model `Default` perspective is skipped
  unless asked for. Unknown references return the standard error dict plus `missing` and `results`.
- **`DataModel.create_perspective(datamodel, name, tables, description="", ai_context=None)`** — create a
  perspective from table and column **names** (`PerspectiveTableSpec` list, or bare table names for all
  columns); resolved against the schema before anything is sent, refuses a duplicate name, reads the
  result back and reports any mismatch in `warnings`. The request carries the kept tables as `include`
  entries listing only the kept columns. `ai_context` fills the perspective's `aiContext`.
- **`DataModel.delete_perspective(perspective, datamodel=None)`** — delete a perspective by name or
  `oid`; the root model is untouched, the built-in `Default` is refused, and a name shared by several
  models must be disambiguated with `datamodel`.
- **`Dashboard.get_dashboards_by_datasource(datamodel, deep=False)`** — every dashboard that uses a
  model, by title or oid, including dashboards linked only through a widget (`match: "widget"`), from
  one listing call; `deep=True` also exports dashboards with an empty widget-datasource summary.
  Rows carry the owner's email, folder and last-updated time.
- **`Dashboard.duplicate_dashboard(dashboard)`** — copy of a dashboard via export + import with Sisense's
  `duplicate` action; the copy is titled `<original>_perspective_stage` so copies made for testing are
  easy to find and remove. Returns the new id, both titles and
  the widget count.
- **`Dashboard.replace_datasource(dashboard, datasource, from_datasource=None)`** — change the datasource a
  dashboard queries, e.g. from a model to a perspective over it, via Sisense's own
  `replace_datasource` route; widgets and filters on the old datasource follow, others are untouched.
  Sent as owner and read back; Sisense silently ignores a non-owner's call, so an unchanged dashboard triggers a retry with admin access, and a change that still does not apply fails with the dashboard's `owner`. A perspective
  is addressed through its root model (the datasource catalogue does not reliably list perspectives). Republishes the
  dashboard afterwards (`publish=True`) so shared viewers see the change; where only the owner may
  publish, the result carries `owner` instead of `published: True`. Returns the previous
  datasource object (for reverting), the new one, widget counts and `published`.
- **`Dashboard.delete_dashboard(dashboard_id, title)`** — delete a dashboard only when both its id and its
  exact current title match; a stale id or a renamed dashboard is refused.
- **Config from JSON or a dict, not only YAML.** `SisenseClient(config_file=...)` now accepts a
  `.yaml`/`.yml` path, a `.json` path, an `os.PathLike`, or a plain dict with the same keys.
  `Migration` and `MergeTool` gain `source_config` / `target_config` taking the same forms;
  `source_yaml` / `target_yaml` keep working as aliases. The loader is exported as
  `pysisense.load_config`. A config missing `domain` or `token` now raises a `ValueError`
  naming the key instead of a bare `KeyError`.

### Fixed

- **`publish_dashboard` no longer fails outright on Sisense versions that reject `adminAccess`.** It
  sent `adminAccess=true` by default; live-observed, this version answers 422 "must NOT have
  additional properties" to that flag, so every publish failed. It now sends the plain call first and
  retries with the flag only on a 403; a 422 on the retry yields the original 403, which is the honest
  answer (only the owner may publish). Failures go through the shared error helper.
- **Error dicts now relay Sisense's message from nested error bodies.** Sisense wraps some failures as
  `{"error": {"code": 5002, "message": "Invalid token.", "status": 401, ...}}`; the shared error helper
  read only the top level and reported `unrecognized error body (HTTP 401)`. It now looks one level
  inside `error`, so a bad token reads `Invalid token. (HTTP 401)` everywhere in the SDK, and appends the
  first `subErrors[].message` of a validation failure, e.g. `... schema validation error: must match
  pattern "^[0-9a-fA-F]{24}$" (HTTP 422)`.
- **`get_datamodel_columns` no longer returns nothing for models whose per-dataset endpoints fail, and
  no longer crashes on `null` schema entries.** It read `/schema/datasets` then `/datasets/{id}/tables`;
  live-observed, that path answers "Elasticube not found" for some models while the full schema is
  available, and a `null` in a column list raised `AttributeError`. It now reads the full schema in one
  call (identical rows on every comparable model), falls back to the per-dataset endpoints only when
  that yields nothing, and skips malformed entries. `get_unused_columns_bulk` inherits both fixes.
- **`get_unused_columns_bulk` now finds dashboards that reach the model only through a widget.** It
  discovered dashboards with the listing's `datasourceTitle` filter, which matches a dashboard's own
  datasource only; live-reproduced, a dashboard built on model B with a widget and a filter on model A
  was never examined for A, so the columns they used were reported unused. Discovery is now the same
  as `get_dashboards_by_datasource` (dashboard- and widget-level matches, case-insensitive).
- **`get_unused_columns_bulk` and `get_dashboard_columns` no longer misread field references,
  so columns that dashboards use are no longer reported unused.** Both read a field as
  `dim.strip("[]").split(".", 1)`, which assumes the table name has no dot. Any table with a
  dot in its name — every CSV upload is called `something.csv` — came out as table `T1` /
  column `csv.C1`, never matched the schema, and every such column was marked unused (82
  references across 20 of 509 dashboards on one instance). Names that begin with `[` or end
  with `]` (Sisense enforces no naming restriction and emits them raw, e.g. `[[region.col]`)
  lost the bracket the same way. The two methods now share one traversal
  (`_extract_dashboard_columns`) that reads the same places as before — dashboard filters,
  dependent filter levels, widget panel items and formula `context` — and takes each node's
  explicit `table` and `column` keys, which Sisense writes beside `dim`, parsing `dim` only as
  a fallback through a permissive candidate parser. When a name itself contains dots and only
  `dim` is available, the model's own columns decide where the table ends; a case difference
  between dashboard and model resolves to the model's spelling. The deprecated
  `get_unused_columns` alias inherits the fix. `get_dashboard_columns` rows now carry the
  column name as the model spells it, without the `" (Calendar)"` suffix Sisense appends to
  date dimensions in `dim`.
- `get_dashboard_columns` reported `widget_id` from the widget's position in the dashboard
  layout, which is wrong for any layout with more than one column; it now reports the
  widget's own `oid`.
- A filter or panel item whose `dim` is `null` crashed both walkers with `TypeError`; it is
  now skipped.
- `update_user` with an unknown email raised `KeyError` instead of returning the standard
  failure dict — the 2.0 `get_user` failure dict is never empty, so the old `if not user`
  guard never fired.
- `create_user` / `update_user` no longer modify the caller's payload dict while resolving
  `role` and `groups` to IDs, and no longer write the raw payload (which may carry
  `password`) to the debug log.
- `get_user` / `get_users_all` docstrings described `ROLE_NAME` as the raw role value; it
  keeps the 1.x display-name meaning, as the changelog and upgrade guide say.

### Changed

- `users_per_group(name)` no longer makes a separate `?name=` lookup before fetching the
  expanded group listing — one fewer request per call, same results and same error for an
  unknown name. `get_groups(name=...)` sends the name as a URL-encoded query parameter.

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
