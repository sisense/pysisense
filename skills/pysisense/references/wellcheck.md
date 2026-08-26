# WellCheck reference

`from pysisense import WellCheck` — `wellcheck = WellCheck(api_client=api_client, debug=False)`

The constructor wires up its own internal `Dashboard`, `DataModel`, and `AccessManagement` instances from the shared `api_client` (`self.dashboard`, `self.datamodel`, `self.access_mgmt`) — you don't need to build/pass those separately. `access_mgmt` is what backs the `unused_columns` section of `run_full_wellcheck`; there's no way to opt it out short of monkeypatching `wellcheck.access_mgmt = None` (which just makes that section skip with a warning, same as if it were never configured).

All check methods accept either a 24-char dashboard/data-model **ID** or a **title** string, and are tolerant of a single string in place of a list (normalized to a one-element list internally). References that fail to resolve are skipped with a `logger.warning` — they don't raise or short-circuit the batch. If every reference in a call fails to resolve (or none are valid), the method returns `[]`; there's no `{"error": ...}` return from these checks, unlike most other modules — check for an empty list, not an `"error"` key.

## Dashboard checks

| Method | Key output fields | Use when |
|---|---|---|
| `check_dashboard_structure(dashboards)` | `pivot_count`, `tabber_count`, `accordion_count`, `jtd_count` (+ `dashboard_id`, `dashboard_title`) | Counting pivot/tabber/accordion widgets and jump-to-dashboard (JTD) targets — JTD is deduped by target dashboard ID across both widget options (`drillTarget`) and dashboard/widget script (`prism.jumpToDashboard(...)` calls). |
| `check_dashboard_widget_counts(dashboards)` | `widget_count` (+ `dashboard_id`, `dashboard_title`) | Just need total widgets per dashboard, no structural breakdown. |
| `check_pivot_widget_fields(dashboards, max_fields=20)` | `field_count`, `has_more_fields` (+ `dashboard_id`, `dashboard_title`, `widget_id`) | Flagging pivot widgets with too many fields. |

```python
rows = wellcheck.check_dashboard_structure(dashboards=["663b8f519ef48f00345bea45", "Academy AI Content"])
```

**`check_pivot_widget_fields` only returns rows *above* the threshold.** The comparison is strictly `panel_count > max_fields` (not `>=`) — a pivot with exactly `max_fields` items is not flagged and produces no row for that widget. `has_more_fields` is always `True` on returned rows (there is no `False` row for widgets under the threshold; those are simply omitted, though they're still counted in the debug/info logs).

## Data model checks

| Method | Key output fields | Use when |
|---|---|---|
| `check_datamodel_custom_tables(datamodels)` | `has_union` (`"yes"`/`"no"`) (+ `data_model`, `table`) | Flagging custom SQL tables whose expression contains `UNION` (case-insensitive substring match). |
| `check_datamodel_island_tables(datamodels)` | `relation` (`"no"` = island), `type` (e.g. `fact`/`dim`/`custom`) (+ `datamodel`, `datamodel_oid`, `table`, `table_oid`) | Finding tables with no relationship — the returned rows **are** the island tables (`relation` is always `"no"` on returned rows; tables that do have a relation aren't included). |
| `check_datamodel_rls_datatypes(datamodels)` | `datatype` (+ `datamodel`, `table`, `column`) | Auditing RLS column datatypes — e.g. surfacing non-numeric RLS rules. |
| `check_datamodel_import_queries(datamodels)` | `has_import_query` (`"yes"`/`"no"`) (+ `data_model`, `table`) | One row per **table** (not just flagged ones) — use this to see the full table inventory alongside the flag. |
| `check_datamodel_m2m_relationships(datamodels)` | `is_m2m` (bool) (+ `data_model`, `left_table`, `left_column`, `right_table`, `right_column`) | Detecting potential many-to-many joins. |

```python
rows = wellcheck.check_datamodel_island_tables(datamodels="MyDataModel_ec")
```

Note the field-name inconsistency across these checks (this mirrors the actual return payloads, not a typo here): `check_datamodel_custom_tables`/`check_datamodel_import_queries` key the model under `"data_model"`, while `check_datamodel_island_tables`/`check_datamodel_rls_datatypes`/`check_datamodel_m2m_relationships` use `"datamodel"`. Don't assume a single normalized column name when combining sections into one DataFrame.

**`check_datamodel_m2m_relationships` runs real aggregate SQL against the data source** — for every relation column pair it issues two `GROUP BY ... HAVING COUNT(...) > 1` queries via `/api/datasources/{model}/sql`. `is_m2m` is `True` only when *both* sides return more than one duplicate-key row. This is the slowest check in the module by far — expect it to scale with relation count and table size, and budget accordingly in scripts (it's not something to run casually across every model on an instance).

## Full orchestrator

`run_full_wellcheck(dashboards=None, datamodels=None, max_pivot_fields=20)` runs every check above in one call and returns a **nested** dict — the two top-level keys are always present, but their sub-lists stay `[]` if the corresponding `dashboards`/`datamodels` argument is omitted (that side is skipped entirely, logged as info, not an error):

```python
report = wellcheck.run_full_wellcheck(
    dashboards=["663b8f519ef48f00345bea45", "Academy AI Content"],
    datamodels=["MyDataModel_ec"],
    max_pivot_fields=20,
)
# {
#     "dashboards": {
#         "structure": [...],             # check_dashboard_structure
#         "widget_counts": [...],          # check_dashboard_widget_counts
#         "pivot_widget_fields": [...],    # check_pivot_widget_fields(max_fields=max_pivot_fields)
#     },
#     "datamodels": {
#         "custom_tables": [...],          # check_datamodel_custom_tables
#         "island_tables": [...],          # check_datamodel_island_tables
#         "rls_datatypes": [...],          # check_datamodel_rls_datatypes
#         "import_queries": [...],         # check_datamodel_import_queries
#         "m2m_relationships": [...],      # check_datamodel_m2m_relationships (slow — see above)
#         "unused_columns": [...],         # AccessManagement.get_unused_columns_bulk
#     },
# }
```

`unused_columns` is delegated to `self.access_mgmt.get_unused_columns_bulk(datamodels=datamodel_refs)` — it only runs when `datamodels` is provided *and* `wellcheck.access_mgmt` is set (true by default per the constructor above). If `access_mgmt` were ever unset, that key comes back `[]` with a `logger.warning`, not an error.

```python
structure_rows = report["dashboards"]["structure"]
m2m_rows = report["datamodels"]["m2m_relationships"]
df = api_client.to_dataframe(m2m_rows)
api_client.export_to_csv(m2m_rows, file_name="m2m_relationships.csv")
```

`max_pivot_fields` only affects `pivot_widget_fields` — the other checks have no configurable thresholds.
