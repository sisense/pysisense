# DataModel operations reference

`from pysisense import DataModel` — `datamodel = DataModel(api_client=api_client)`

Six mixins: core (listing/describe/lifecycle), connections, build (provisioning/deploy), security (RLS), shares (permissions), data (SQL/row counts).

## Model types — extract vs live

| Type | String | Notes |
|---|---|---|
| Elasticube | `"extract"` | Supports all build behaviors (`replace`, `replace_changes`, `append`, `increment`) |
| Live | `"live"` | `build_behavior_config` is ignored; deploy is always a publish |

Type strings are lowercase on input (`create_datamodel`, `setup_datamodel`) but come back uppercase (`"EXTRACT"`/`"LIVE"`) in schema objects like `datamodel.get("type")` — most internal branching does `.upper()` before comparing.

## Core — get, list, describe, resolve, lifecycle

| Method | Use when |
|---|---|
| `get_datamodel(name)` | Full schema object by title (`GET /api/v2/datamodels/schema?title=`). The building block most other methods call internally. |
| `get_all_datamodel()` | All models via an internal GraphQL endpoint — richer metadata (`oid`, `title`, `type`, `status`, `sizeInMb`) than `get_elasticubes`. |
| `get_elasticubes()` | Legacy `/api/v1/elasticubes/getElasticubes` — lighter payload (`title`, `address`, `fullname`), but works on **both** Linux and Windows. Prefer `get_all_datamodel` on Linux when you need build status/size. |
| `describe_datamodel_raw(name)` | Nested `{name, id, type, datamodel_last_build_publish, datamodel_last_updated, datasets: [...]}`. |
| `describe_datamodel(name)` | Same info flattened to one row per table — feed straight into `to_dataframe`/`export_to_csv`. |
| `get_model_schema(name)` | One row per **column** (`datamodel_name`, `dataset_name`, `table_name`, `column_name`, `column_type`). Numeric Sisense type codes are mapped to readable names (`DateTime`, `Double`, `Integer`, `BigInt`, `Text`, `Decimal`, `Float`, `Real`) — unmapped codes come back as `"Unknown Type"`. |
| `resolve_datamodel_reference(ref)` | Accepts ID or title; returns `{"success", "status_code", "datamodel_id", "datamodel_title", "error"}`. Tries as an ID first, falls back to title lookup (exact case-insensitive match preferred, else first candidate). |
| `load_datamodel(title, server="LocalHost")` | GraphQL OID lookup by exact title — returns `{"oid", "__typename"}`. Note GraphQL errors arrive as HTTP 200 with an `"errors"` key; the method unwraps that into `{"error": "..."}` for you. |
| `delete_datamodel(title, server)` | GraphQL delete mutation. `server` is required (no default) — typically `"LocalHost"`. Returns `{"success": True}`. |

```python
dm = datamodel.get_datamodel("pysense_databricks_ec")
flat_rows = datamodel.describe_datamodel("pysense_databricks_ec")
schema_rows = datamodel.get_model_schema("pysense_databricks_ec")

resolved = datamodel.resolve_datamodel_reference("MyDataModel_ec")
if resolved["success"]:
    datamodel_id = resolved["datamodel_id"]
```

**`get_all_datamodel` vs `get_elasticubes`**: both are undocumented/legacy-flavored endpoints. `get_all_datamodel` normalizes `status` (collapses a `"building"` status list down to the single string `"building"`, otherwise takes the first status) and rounds `sizeInMb` — useful for dashboards/audits. `get_elasticubes` is the thin cross-platform fallback.

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

- `export_datamodel_schema`'s `dependencies` param (e.g. `"dataContext"`, `"scopeConfiguration"`, `"formulaManagement"`, `"drillHierarchies"`, `"perspectives"`) is **ignored on Windows** — the legacy streaming export endpoint used there takes no dependencies parameter, and a warning is logged if you pass any.
- `import_datamodel_schema` returns `{"error": "...", "already_exists": True}` specifically when the failure is `ElasticubeAlreadyExists` (HTTP 400 with that title) — check `already_exists` to distinguish "name collision" from other failures.

## Connections

| Method | Notes |
|---|---|
| `get_connection(name)` | Returns a **list** of matches (not a single object) — index `[0]` for the common case. |
| `get_connections()` | All connections. |
| `update_connection(id, data)` | `PATCH` — only fields present in `connection_data` are sent. |
| `get_table_schema(connection_name, database_name, schema_name, table_name)` | Undocumented endpoint (`/api/v1/connection/{id}/table_schema_details`) — resolves the connection by name first, so it fails if the name doesn't match exactly. |
| `create_connections(payload)` | Returns the created connection dict on HTTP 201, or **`None`** (not an `{"error": ...}` dict) on any other outcome — check for `None` explicitly, this one doesn't follow the usual error-dict convention. |

### `generate_connections_payload(datasource_type, connection_params)`

Supported types (case-insensitive): `"Athena"`, `"RedShift"`, `"BigQuery"`, `"DataBricks"`. Builds the provider-specific body consumed by `create_connections`. Raises (does not return an error dict) on bad input — `KeyError` for a missing required param, `ValueError` for an unsupported type.

```python
params = {
    "name": "pysense_databricks",
    "connection_string": "jdbc:databricks://<host>:443;httpPath=<path>;AuthMech=3;",
    "token": "XYZ1234567890",          # required — becomes "password" in the payload
}
payload = datamodel.generate_connections_payload("DataBricks", params)
created = datamodel.create_connections(payload)
```

Required keys per type: Athena — `name`, `region`, `s3_output_location`, `aws_access_key`, `aws_secret_key`; RedShift — `server`, `username`, `password`; BigQuery — `name`, `service_account_key_path`; DataBricks — `name`, `connection_string`, `token`. Everything else is optional with sane defaults.

**Credentials and logging**: raw secrets (`password`, `token`, service-account paths, etc.) go straight into the `parameters` block of the generated payload. Every debug log of the payload — inside `generate_connections_payload` and `create_connections` — is passed through `redact_secrets()` first, so credentials never hit `logs/pysisense.log` in the clear. This module does not call into `Encryption` itself; if you need to encrypt connection params for a cross-server datamodel migration payload (as opposed to just not logging them), use the separate `Encryption` class explicitly.

## Build — provisioning and deploy

```python
datamodel.create_datamodel("MyDataModel_ec", "extract")   # or "live"
datamodel.create_dataset(datamodel_name="MyDataModel_ec", connection_name="pysense_bigquery",
                          database_name="fda_food", schema_name="fda_food")
datamodel.create_table(datamodel_name="MyDataModel_ec", table_name="housing",
                        import_query="SELECT * FROM `fda_food`.`housing` LIMIT 10",
                        build_behavior_config={"mode": "increment", "column_name": "latitude"})
```

- `create_dataset` infers `dataset_name` from `schema_name` when omitted.
- `create_table` infers `dataset_id`/`database_name`/`schema_name` from the model when not given — but **fails if the model has more than one dataset** (`"Multiple datasets found... Provide a dataset_id"`); pass `dataset_id` explicitly in that case.
- `build_behavior_config` (extract only) — `mode` one of `"replace"`, `"replace_changes"`, `"append"`, `"increment"`; `"increment"` additionally requires `column_name` and fails with `{"error": ...}` if that column isn't found on the created table. Passed but ignored on live models per `setup_datamodel`'s table dicts (leave `{}` or omit).

`setup_datamodel(...)` chains `create_datamodel` → `create_dataset` → `create_table` (once per entry in `tables`) and aborts on the first failure, returning `{"datamodel_id", "dataset_id", "tables": [names]}` on success.

### `deploy_datamodel(datamodel_name, build_type="full", row_limit=0, schema_origin="latest")`

```python
# Extract
datamodel.deploy_datamodel("MyDataModel_ec", build_type="by_table", row_limit=1000, schema_origin="latest")
# Live — build_type/row_limit/schema_origin are all ignored; internally forced to "publish"
datamodel.deploy_datamodel("MyDataModel_live")
```

`build_type`: `"schema_changes"` | `"by_table"` | `"full"` (default). `schema_origin`: `"latest"` (Data page schema, default) | `"running"` (last successfully built version). Both are meaningless for live models — the payload is overridden to `{"datamodelId": ..., "buildType": "publish"}` regardless of what you pass.

## Security — row-level security (RLS)

| Method | Model type | Semantics |
|---|---|---|
| `get_datasecurity(name)` | either | Flat, deduplicated `(table, column)` rows — good for a quick audit. |
| `get_datasecurity_detail(name)` | either | One row **per share** per rule, with a human-readable `rule_description` (`"Can see only [...]"`, `"Cannot see any value"`, etc.). |
| `get_datasecurity_raw(name, datamodel_type=None)` | either | Unflattened rules exactly as the API returns them (`members`, `exclusionary`, raw `shares`) — the one to use for round-tripping to another environment. Pass `datamodel_type` if already known to skip the resolve call. |
| `update_datasecurity(name, rules)` | **extract only** | `PUT` — replaces the entire rule set. Pass `[]` to clear all rules. Errors on a live model. |
| `set_live_datasecurity_add_many(name, rules)` | **live only** | `POST .../addMany` — **appends** to the existing set, does not replace it. Errors on an extract model. |

**Edge case (per CLAUDE.md, confirmed in `security.py`)**: when no RLS rules exist, `get_datasecurity` and `get_datasecurity_detail` both return a **single-entry list** with empty string values (and the resolved `datamodel_name`) — never an empty list. Don't treat `len(result) == 0` as "no rules"; check for the empty-values sentinel row instead, or just check `result[0]["table_name"] == ""`.

```python
rules = [{"table": "orders", "column": "region", "datatype": "text",
          "members": ["EMEA"], "exclusionary": False,
          "shares": [{"type": "user", "partyId": "user_oid", "partyName": "user@example.com"}]}]
datamodel.update_datasecurity("SalesCube", rules)              # extract
datamodel.set_live_datasecurity_add_many("LiveSalesCube", rules)  # live
```

## Shares — data model permissions

Permission strings are **uppercase**: `"EDIT"` | `"USE"` | `"READ"`. Internally these map to single-letter API codes (`w`/`r`/`a` respectively) and back.

| Method | Returns |
|---|---|
| `get_datamodel_shares(name)` | Resolved, flat rows: `{"datamodel_name", "datamodel_id", "party_name", "party_type", "permission"}` — names looked up against the full user/group lists. |
| `add_datamodel_shares(name, shares)` | Merges new shares (resolved by email/group name) with existing ones and submits the combined list. |
| `get_datamodel_permissions_extract(title)` / `get_datamodel_permissions_live(id)` | Raw, unresolved shares keyed by `partyId` — for round-tripping between environments. |
| `update_datamodel_permissions_extract(title, shares)` / `update_datamodel_permissions_live(id, shares)` | Replace the raw share list wholesale. |

```python
shares = [
    {"name": "autotest@sisense.com", "type": "user", "permission": "EDIT"},
    {"name": "mig_test", "type": "group", "permission": "USE"},
]
datamodel.add_datamodel_shares("pysense_databricks", shares)
```

**Known gap — `add_datamodel_shares` on EXTRACT models is currently blocked.** The method returns `{"error": "Fixing Bug: Cannot add shares to EXTRACT DataModels. Will be fixed in V2."}` unconditionally for extract-type models (a deliberate short-circuit in the source, not a transient failure) — it only actually works against LIVE models today. For extract share management, use the raw `update_datamodel_permissions_extract`/`get_datamodel_permissions_extract` pair instead (note: `PUT` for extract, `PATCH` for live — an API difference between the two endpoints, not an inconsistency in the SDK).

`update_datamodel_permissions_live` requires the live model to already be **published** at least once — publish with `deploy_datamodel` first if it has never been built.

## Data — direct table access

```python
rows = datamodel.get_data("pysense_databricks", "trips")                      # SELECT * FROM [trips]
rows = datamodel.get_data("pysense_databricks", "trips", query="SELECT count(*) FROM trips")
counts = datamodel.get_row_count("pysense_databricks_ec")
```

- `get_data` runs against `/api/datasources/{datamodel_name}/sql` and returns `[]` on any failure or empty result (no error dict) — table name is bracket-escaped for you.
- `get_row_count` calls `get_data` once per table (`SELECT COUNT(*) FROM [...]`) and appends a final `{"table_name": "total_row_count", "row_count": <sum>}` row. A table whose count query returns nothing is silently skipped (logged as a warning) rather than failing the whole call.

## Common gotchas across the module

- Almost every method that takes `datamodel_name` resolves it via `get_datamodel` internally — an unresolvable name surfaces as `{"error": "DataModel '<name>' not found."}` (or `[]` for the `list`-returning data/describe methods).
- `datamodel_type` comparisons are case-sensitive against `.upper()` results (`"EXTRACT"`/`"LIVE"`) inside the SDK, but you pass lowercase (`"extract"`/`"live"`) into `create_datamodel`/`setup_datamodel`.
- `create_connections` is the one method here that returns `None` instead of an `{"error": ...}` dict on failure — don't assume the module-wide error convention holds for it.
