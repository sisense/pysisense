# DataModel operations reference

`from pysisense import DataModel`, `datamodel = DataModel(api_client=api_client)`

Six mixins: core (listing/describe/lifecycle), connections, build (provisioning/deploy), security (RLS), shares (permissions), data (SQL/row counts).

**Failure contract:** every method below returns the standard `{"ok": False, "error": "...", "status_code": <int, when available>}` dict on failure, unless a method's own notes say otherwise. An empty list from a read method means a genuinely empty result, not a failure.

## Model types: extract vs live

| Type | String | Notes |
|---|---|---|
| Elasticube | `"extract"` | Supports all build behaviors (`replace`, `replace_changes`, `append`, `increment`) |
| Live | `"live"` | `build_behavior_config` is ignored; deploy is always a publish |

Type strings are lowercase on input (`create_datamodel`, `setup_datamodel`) but come back uppercase (`"EXTRACT"`/`"LIVE"`) in schema objects like `datamodel.get("type")`. Most internal branching does `.upper()` before comparing.

## Core: get, list, describe, resolve, lifecycle

| Method | Use when |
|---|---|
| `get_datamodel(name)` | Full schema object by title (`GET /api/v2/datamodels/schema?title=`). The building block most other methods call internally. |
| `get_all_datamodel()` | All models via an internal GraphQL endpoint, richer metadata (`oid`, `title`, `type`, `status`, `sizeInMb`) than `get_elasticubes`. |
| `get_elasticubes()` | Legacy `/api/v1/elasticubes/getElasticubes`, lighter payload (`title`, `address`, `fullname`), but works on **both** Linux and Windows. Prefer `get_all_datamodel` on Linux when you need build status/size. |
| `describe_datamodel_raw(name)` | Nested `{name, id, type, datamodel_last_build_publish, datamodel_last_updated, datasets: [...]}`. |
| `describe_datamodel(name)` | Same info flattened to one row per table, feed straight into `to_dataframe`/`export_to_csv`. |
| `get_model_schema(name)` | One row per **column** (`datamodel_name`, `dataset_name`, `table_name`, `column_name`, `column_type`). Numeric Sisense type codes are mapped to readable names (`DateTime`, `Double`, `Integer`, `BigInt`, `Text`, `Decimal`, `Float`, `Real`); unmapped codes come back as `"Unknown Type"`. |
| `resolve_datamodel_reference(ref)` | Accepts ID or title; returns `{"success", "status_code", "datamodel_id", "datamodel_title", "error"}`. Tries as an ID first, falls back to title lookup (exact case-insensitive match preferred, else first candidate). This is its own stable envelope shape, detected via `success`, not the generic `ok`/`error` failure dict. |
| `create_datamodel(name, type)` | Checks for a duplicate title first, returning a clear "already exists" error instead of the API's opaque HTTP 500. |
| `load_datamodel(title, server="LocalHost")` | GraphQL OID lookup by exact title, returns `{"oid", "__typename"}`. GraphQL errors arrive as HTTP 200 with an `"errors"` key; the method unwraps that into the standard failure dict. |
| `delete_datamodel(title, server)` | GraphQL delete mutation. `server` is required (no default), typically `"LocalHost"`. Returns `{"success": True}`. |

```python
dm = datamodel.get_datamodel("pysense_databricks_ec")
flat_rows = datamodel.describe_datamodel("pysense_databricks_ec")
schema_rows = datamodel.get_model_schema("pysense_databricks_ec")

resolved = datamodel.resolve_datamodel_reference("MyDataModel_ec")
if resolved["success"]:
    datamodel_id = resolved["datamodel_id"]
```

**`get_all_datamodel` vs `get_elasticubes`**: both are undocumented/legacy-flavored endpoints. `get_all_datamodel` normalizes `status` (collapses a `"building"` status list down to the single string `"building"`, otherwise takes the first status) and rounds `sizeInMb`, useful for dashboards/audits. `get_elasticubes` is the thin cross-platform fallback.

## Cross-environment schema export/import

```python
schema = datamodel.export_datamodel_schema("datamodel-oid", dependencies=["dataContext", "formulaManagement"])

# Plain create
result = datamodel.import_datamodel_schema(schema)
# Overwrite an existing model (falls back to plain create if target OID is 404)
result = datamodel.import_datamodel_schema(schema, action="overwrite", target_datamodel_id="existing-oid")
# Duplicate under a new title
result = datamodel.import_datamodel_schema(schema, action="duplicate", new_title="SalesCube (Copy)")
# {"datamodel_id": "new-oid", "already_exists": False}
```

- `export_datamodel_schema`'s `dependencies` param (e.g. `"dataContext"`, `"scopeConfiguration"`, `"formulaManagement"`, `"drillHierarchies"`, `"perspectives"`) is **ignored on Windows**, the legacy streaming export endpoint used there takes no dependencies parameter, and a warning is logged if you pass any.
- `import_datamodel_schema` returns `{"ok": False, "error": "...", "already_exists": True}` specifically when the failure is `ElasticubeAlreadyExists` (HTTP 400 with that title). Check `already_exists` to distinguish "name collision" from other failures.

## Connections

| Method | Notes |
|---|---|
| `get_connection(name)` | Returns a **list** of matches (not a single object), index `[0]` for the common case. |
| `get_connections_all()` | All connections. The old `get_connections()` name is a deprecated alias, use `get_connections_all` in new code. |
| `update_connection(id, data)` | `PATCH`, only fields present in `connection_data` are sent. |
| `get_table_schema(connection_name, database_name, schema_name, table_name)` | Undocumented endpoint (`/api/v1/connection/{id}/table_schema_details`), resolves the connection by name first, so it fails if the name doesn't match exactly. |
| `create_connections(payload)` | Returns the created connection dict on HTTP 201, or the standard `{"ok": False, "error": "...", ...}` dict on failure. |

### `generate_connections_payload(datasource_type, connection_params)`

Supported types (case-insensitive, `Literal["Athena", "RedShift", "BigQuery", "DataBricks"]`): `"Athena"`, `"RedShift"`, `"BigQuery"`, `"DataBricks"`. Builds the provider-specific body consumed by `create_connections`. Raises (does not return a failure dict) on bad input: `KeyError` for a missing required param, `ValueError` for an unsupported type.

```python
params = {
    "name": "pysense_databricks",
    "connection_string": "jdbc:databricks://<host>:443;httpPath=<path>;AuthMech=3;",
    "token": "XYZ1234567890",  # required, becomes "password" in the payload
}
payload = datamodel.generate_connections_payload("DataBricks", params)
created = datamodel.create_connections(payload)
```

Required keys per type: Athena: `name`, `region`, `s3_output_location`, `aws_access_key`, `aws_secret_key`; RedShift: `server`, `username`, `password`; BigQuery: `name`, `service_account_key_path`; DataBricks: `name`, `connection_string`, `token`. Everything else is optional with sane defaults.

**Credentials and logging**: raw secrets (`password`, `token`, service-account paths, etc.) go straight into the `parameters` block of the generated payload. Every debug log of the payload, inside `generate_connections_payload` and `create_connections`, is passed through `redact_secrets()` first, so credentials never hit `logs/pysisense.log` in the clear. This module does not call into `Encryption` itself; if you need to encrypt connection params for a cross-server datamodel migration payload (as opposed to just not logging them), use the separate `Encryption` class explicitly.

## Build: provisioning and deploy

```python
datamodel.create_datamodel("MyDataModel_ec", "extract")  # or "live"
datamodel.create_dataset(datamodel_name="MyDataModel_ec", connection_name="pysense_bigquery", database_name="fda_food", schema_name="fda_food")
datamodel.create_table(
    datamodel_name="MyDataModel_ec", table_name="housing", import_query="SELECT * FROM `fda_food`.`housing` LIMIT 10", build_behavior_config={"mode": "increment", "column_name": "latitude"}
)
```

- `create_dataset` infers `dataset_name` from `schema_name` when omitted.
- `create_table` infers `dataset_id`/`database_name`/`schema_name` from the model when not given, but **fails if the model has more than one dataset** (`"Multiple datasets found... Provide a dataset_id"`); pass `dataset_id` explicitly in that case.
- `build_behavior_config` (extract only): `mode` one of `"replace"`, `"replace_changes"`, `"append"`, `"increment"`; `"increment"` additionally requires `column_name` and fails with the standard error dict if that column isn't found on the created table. Passed but ignored on live models per `setup_datamodel`'s table dicts (leave `{}` or omit).

`setup_datamodel(...)` chains `create_datamodel` → `create_dataset` → `create_table` (once per entry in `tables`) and aborts on the first failure, returning `{"datamodel_id", "dataset_id", "tables": [names]}` on success.

### `deploy_datamodel(datamodel_name, build_type="full", row_limit=0, schema_origin="latest")`

```python
# Extract
datamodel.deploy_datamodel("MyDataModel_ec", build_type="by_table", row_limit=1000, schema_origin="latest")
# Live: build_type/row_limit/schema_origin are all ignored; internally forced to "publish"
datamodel.deploy_datamodel("MyDataModel_live")
```

`build_type`: `"schema_changes"` | `"by_table"` | `"full"` (default). `schema_origin`: `"latest"` (Data page schema, default) | `"running"` (last successfully built version). Both are meaningless for live models, the payload is overridden to `{"datamodelId": ..., "buildType": "publish"}` regardless of what you pass.

## Security: row-level security (RLS)

| Method | Model type | Semantics |
|---|---|---|
| `get_datasecurity(name)` | either | Flat, deduplicated `(table, column)` rows, good for a quick audit. |
| `get_datasecurity_detail(name)` | either | One row **per share** per rule, with a human-readable `rule_description` (`"Can see only [...]"`, `"Cannot see any value"`, etc.). |
| `get_datasecurity_raw(name, datamodel_type=None)` | either | Unflattened rules exactly as the API returns them (`members`, `exclusionary`, raw `shares`), the one to use for round-tripping to another environment. Pass `datamodel_type` if already known to skip the resolve call. |
| `update_datasecurity(name, rules)` | **extract only** | `POST`, **adds** the given rules; does not replace the existing set. To replace a column's rules, delete them first with `delete_datasecurity`. The elasticube must be built and running, draft cubes reject every write. Server-managed fields (`_id`, `created`, `lastModified`, `importedIdIdentifier`) are stripped automatically, so a rule read via `get_datasecurity_raw` can be re-submitted as-is. |
| `set_live_datasecurity_add_many(name, rules)` | **live only** | `POST .../addMany`, appends to the existing set. The live model must be **published**, draft live models fail with `"Elasticube has not been found"`. Rules additionally require `allMembers`, `live`, and `fullname` (`"live:{title}"`); `live`/`fullname` are filled in automatically when omitted, and server-managed fields are stripped the same way as `update_datasecurity`. |
| `delete_datasecurity(name, table, column)` | either | Deletes all rules for one table/column. Combine with the two methods above for replace semantics: delete a column's rules, then add the new ones. |

**Empty means empty.** `get_datasecurity`, `get_datasecurity_detail`, and `get_datasecurity_raw` all return `[]` only when a model genuinely has no RLS rules, never a placeholder row, and never as a failure disguise (a resolve/fetch failure returns the standard error dict instead). Row counts always equal real rule counts.

```python
rules = [{"table": "orders", "column": "region", "datatype": "text", "members": ["EMEA"], "exclusionary": False, "shares": [{"type": "user", "partyId": "user_oid", "partyName": "user@example.com"}]}]
datamodel.update_datasecurity("SalesCube", rules)  # extract, adds rules
datamodel.set_live_datasecurity_add_many("LiveSalesCube", rules)  # live, adds rules
datamodel.delete_datasecurity("SalesCube", "orders", "region")  # remove a column's rules first, for a replace
```

## Shares: data model permissions

Permission strings are **uppercase**: `"EDIT"` | `"USE"` | `"READ"`. Internally these map to single-letter API codes (`w`/`r`/`a` respectively) and back.

| Method | Returns |
|---|---|
| `get_datamodel_shares(name)` | Resolved, flat rows: `{"datamodel_name", "datamodel_id", "party_name", "party_type", "permission"}`, names looked up against the full user/group lists. |
| `add_datamodel_shares(name, shares)` | Merges new shares (resolved by email/group name) with existing ones and submits the combined list. Works on both extract and live models. |
| `get_datamodel_permissions_extract(title)` / `get_datamodel_permissions_live(id)` | Raw, unresolved shares keyed by `partyId`, for round-tripping between environments. |
| `update_datamodel_permissions_extract(title, shares)` / `update_datamodel_permissions_live(id, shares)` | Replace the raw share list wholesale. `PUT` for extract, `PATCH` for live, an API difference between the two endpoints, not an inconsistency in the SDK. |

```python
shares = [
    {"name": "autotest@sisense.com", "type": "user", "permission": "EDIT"},
    {"name": "mig_test", "type": "group", "permission": "USE"},
]
result = datamodel.add_datamodel_shares("pysense_databricks", shares)
# {"success": True, "message": "...", "new_shares": 2, "updated_shares": 0, "skipped": []}
```

`add_datamodel_shares` on success returns `{"success": True, "message": "...", "new_shares": <n>, "updated_shares": <n>, "skipped": [...]}`. `skipped` lists every requested share that wasn't submitted (unknown user/group, inactive user, invalid type) as `{"name", "type", "reason"}` entries, an empty list means everything landed. Sisense silently drops shares for inactive users on write (HTTP 200, entry never lands), the SDK checks for this and reports it in `skipped` instead of claiming success for a share that never took effect. On failure, or when none of the given shares can be resolved, returns the standard error dict, also carrying the `skipped` list.

`update_datamodel_permissions_live` requires the live model to already be **published** at least once, publish with `deploy_datamodel` first if it has never been built.

## Data: direct table access

```python
rows = datamodel.get_data("pysense_databricks", "trips")  # SELECT * FROM [trips]
rows = datamodel.get_data("pysense_databricks", "trips", query="SELECT count(*) FROM trips")
counts = datamodel.get_row_count("pysense_databricks_ec")
```

- `get_data` runs against `/api/datasources/{datamodel_name}/sql`; table name is bracket-escaped for you. An empty list means the query genuinely returned no rows. On failure, returns the standard error dict.
- `get_row_count` calls `get_data` once per table (`SELECT COUNT(*) FROM [...]`) and appends a final `{"table_name": "total_row_count", "row_count": <sum>}` row. A table whose count query returns nothing is silently skipped (logged as a warning) rather than failing the whole call.

## Common gotchas across the module

- Almost every method that takes `datamodel_name` resolves it via `get_datamodel` internally, an unresolvable name surfaces as the standard error dict (or `[]` for the `list`-returning data/describe methods).
- `datamodel_type` comparisons are case-sensitive against `.upper()` results (`"EXTRACT"`/`"LIVE"`) inside the SDK, but you pass lowercase (`"extract"`/`"live"`) into `create_datamodel`/`setup_datamodel`.
- Live vs. extract flavors of the same capability often differ in more than just the URL, verb, required identifier (title vs. oid), and required payload fields can all differ too. Don't extrapolate one flavor's behavior from the other without checking.
