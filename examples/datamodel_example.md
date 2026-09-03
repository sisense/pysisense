# DataModel Example Usage

This guide demonstrates how to use the `DataModel` class from the `pysisense` package. Each example below includes a description and a code snippet for easy reference and copying.

---

## Prerequisites

- Ensure `config.yaml` is in the same folder as your script.
- Import required modules and initialize the API client and `DataModel` class.

```python
import os
import json
from pysisense import DataModel, SisenseClient

# Set the path to your config file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

# Initialize the API client
api_client = SisenseClient(config_file=config_path, debug=True)

# --- Initialize the DataModel class using the shared APIClient ---
datamodel = DataModel(api_client=api_client)
```

---

## Example 1: Get DataModel

Retrieve a specific DataModel by name.

```python
response = datamodel.get_datamodel("pysense_databricks_ec")
print(json.dumps(response, indent=4))
```

---

## Example 2: Get All DataModels

Fetch all DataModels and display as DataFrame.

```python
response = datamodel.get_all_datamodel()
print(json.dumps(response, indent=4))

# Optional: Convert the response to a DataFrame and print
df = api_client.to_dataframe(response)
print(df)
```

---

## Example 3: Get Connection

Get connection details for a specific connection name.

```python
response = datamodel.get_connection("pysense_databricks")
print(json.dumps(response, indent=4))
```

---

## Example 3b: List All Connections

```python
response = datamodel.get_connections_all()
print(json.dumps(response, indent=4))
```

The former `get_connections()` alias was removed in 2.0 — use `get_connections_all`.

---

## Example 3c: Update a Connection (Remapping)

```python
connection_id = "65d62c9574851800339cf49e"
response = datamodel.update_connection(
    connection_id,
    {"name": "target_connection_name", "parameters": {"Server": "new-host.example.com"}},
)
print(json.dumps(response, indent=4))
```

---

## Example 4: Get Table Schema from DataSource

Retrieve the schema for a table in a data source.

```python
connection_name = "pysense_databricks"
database_name = "samples"
schema_name = "nyctaxi"
table_name = "trips"
response = datamodel.get_table_schema(connection_name=connection_name, database_name=database_name, schema_name=schema_name, table_name=table_name)
print(json.dumps(response, indent=4))
df = api_client.to_dataframe(response)
print(df)
```

---

## Example 5: Create DataModel

Create new DataModels (ELASTICUBE and LIVE).

```python
# ELASTICUBE DataModel Creation
response = datamodel.create_datamodel("MyDataModel_ec", "extract")
print(json.dumps(response, indent=4))
# LIVE DataModel Creation
response = datamodel.create_datamodel("MyDataModel_live", "live")
print(json.dumps(response, indent=4))
```

---

## Example 6: Generate Connection Payload

Generate connection payloads for various data sources.

**Athena Example:**
```python
datasource_type = "Athena"
connection_params = {
    "name": "athena_basic",
    "description": "this is a description",  # Optional, specify if needed
    "region": "us-east-1",
    "s3_output_location": "s3://pysense-sdk/athena-output/",
    "aws_access_key": "XYZ1234567890",
    "aws_secret_key": "XYZ1234567890",
    "schema": "pysense",  # Optional, specify if needed
    "additional_parameters": "timeout=60;",  # Optional, specify if needed
}
athena_connection = datamodel.generate_connections_payload(datasource_type, connection_params)
print(json.dumps(athena_connection, indent=4))
```

**Redshift Example:**
```python
datasource_type = "RedShift"
connection_params = {
    "name": "pysense_redshift",
    "description": "Redshift connection example",  # Optional, specify if needed
    "server": "examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439",
    "username": "XYZ@sisense.com",
    "password": "password",
    "default_database": "dev",  # Optional, specify if needed
    "additional_parameters": "",  # Optional, specify if needed
}
redshift_connection = datamodel.generate_connections_payload(datasource_type, connection_params)
print(json.dumps(redshift_connection, indent=4))
```

**BigQuery Example:**
```python
datasource_type = "BigQuery"
connection_params = {
    "name": "pysense_bigquery",
    "description": "BigQuery connection example",
    "use_service_account": True,
    "service_account_key_path": "/opt/sisense/storage/gcp/service_account_key.json",
    "use_proxy_server": False,
    "use_dynamic_schema": False,
    "record_field_flattening_level": "2",
    "unnest_arrays": False,
    "allow_large_results": False,
    "use_storage_api": True,
    "additional_parameters": "timeout=60;",
    "database": "fda_food",
}
bigquery_connection = datamodel.generate_connections_payload(datasource_type, connection_params)
print(json.dumps(bigquery_connection, indent=4))
```

**DataBricks Example:**
```python
datasource_type = "DataBricks"
connection_params = {
    "name": "pysense_databricks",
    "description": "DataBricks connection example",  # Optional, specify if needed
    "connection_string": "jdbc:databricks://<server-hostname>:443;httpPath=<http-path>;AuthMech=3;",
    "token": "XYZ1234567890",
    "use_dynamic_schema": False,  # Optional, specify if needed
    "schema": "",  # Optional, specify if needed
}
databricks_connection = datamodel.generate_connections_payload(datasource_type, connection_params)
print(json.dumps(databricks_connection, indent=4))
```

---

## Example 7: Create Connection

Create a connection using a generated payload.

```python
response = datamodel.create_connections(athena_connection)
print(json.dumps(response, indent=4))
```

On failure this returns `{"ok": False, "error": "...", "status_code": ...}` (it no longer returns `None`).

---

## Example 8: Create Dataset

Create a dataset for a DataModel.

```python
response = datamodel.create_dataset(
    datamodel_name="MyDataModel_ec",
    connection_name="pysense_bigquery",
    database_name="fda_food",  # Data source database name
    schema_name="fda_food",  # Data source schema name
    dataset_name="",  # Optional, defaults to schema name
)
print(json.dumps(response, indent=4))
```

---

## Example 9: Create Table
<div style="background-color:#fff3cd; border-left:4px solid #ffeeba; padding:8px;">
<strong>Important:</strong> For LIVE DataModel, only 1 database is supported. If tables have different schema, provide them explicitly.
</div>
<br>

Create tables with full configuration or minimal inputs.

**Full Configuration:**
```python
datamodel_name = "MyDataModel_ec"
# dataset_id = "945ce5ac-ce68-4a5f-a4e8-577631023add"                     # Optional, will be inferred if not provided
table_name = "housing"
import_query = "SELECT * FROM `fda_food`.`housing` LIMIT 10"  # Optional
description = "Housing table for ML use case"  # Optional
tags = ["housing", "ML"]  # Optional
build_behavior_config = {  # Required only for 'extract' models
    "mode": "increment",  # Options: "replace", "replace_changes", "append", "increment"
    "column_name": "latitude",  # Required if mode is "increment"
}
# schema_name = "fda_food"                                                # If your table's schema is different from the dataset's schema, provide it here
# database_name = "fda_food"                                              # If your table's database is different from the dataset's database, provide it here

response = datamodel.create_table(
    datamodel_name=datamodel_name,
    table_name=table_name,
    # dataset_id=dataset_id,
    import_query=import_query,
    description=description,
    tags=tags,
    build_behavior_config=build_behavior_config,
)
print(json.dumps(response, indent=4))
```

**Minimal Inputs:**

###### Optional parameters like dataset_id, import_query, description, tags, and build_behavior_config will be handled internally

```python
datamodel_name = "MyDataModel_ec"
table_name = "food_enforcement"
response = datamodel.create_table(datamodel_name=datamodel_name, table_name=table_name)
print(json.dumps(response, indent=4))
```

---

## Example 10: Setup DataModel

Setup a DataModel with multiple tables.

```python
datamodel_name = "MyDataModel_live"
datamodel_type = "live"  # Options: "extract", "live"
connection_name = "pysense_databricks"
dataset_name = ""  # Optional, defaults to schema name
database_name = "samples"  # Data source database name
schema_name = "nyctaxi"  # Data source schema name
tables = [
    {
        "database_name": "samples",  # Data source database name
        "schema_name": "nyctaxi",  # Data source schema name
        "table_name": "trips",
        "import_query": "SELECT * FROM `nyctaxi`.`trips` LIMIT 10",  # Optional, specify if needed
        "description": "Trips data for FY23",  # Optional, specify if needed
        "tags": ["trips", "fact"],  # Optional, specify if needed
        "build_behavior_config": {  # Required only for "extract" datamodel otherwise leave blank {} or gets ignored in the case of "live"
            "mode": "increment",  # Options: "replace", "replace_changes", "append", "increment"
            "column_name": "tpep_pickup_datetime",  # Required only for "increment" otherwise leave blank ""
        },
    },
    {
        "database_name": "samples",  # Data source database name
        "schema_name": "tpch",  # Data source schema name
        "table_name": "customer",
        "import_query": "SELECT * FROM `tpch`.`customer` LIMIT 10",  # Optional, specify if needed
        "description": "Customer master data",  # Optional, specify if needed
        "tags": ["customer", "dimension"],  # Optional, specify if needed
        "build_behavior_config": {  # Required only for "extract" datamodel otherwise leave blank {} or gets ignored in the case of "live"
            "mode": "replace"  # Options: "replace", "replace_changes", "append", "increment"
        },
    },
]

response = datamodel.setup_datamodel(
    datamodel_name=datamodel_name,
    datamodel_type=datamodel_type,
    connection_name=connection_name,
    dataset_name=dataset_name,
    database_name=database_name,  # Data source database name
    schema_name=schema_name,  # Data source schema name
    tables=tables,
)
print(json.dumps(response, indent=4))
```

---

## Example 11: Deploy DataModel

Deploy ELASTICUBE and LIVE DataModels.

```python
# ELASTICUBE Deployment
datamodel_name = "MyDataModel_ec2"
response = datamodel.deploy_datamodel(
    datamodel_name=datamodel_name,
    build_type="by_table",  # Other options: "full", "schema_changes"
    row_limit=1000,
    schema_origin="latest",  # Other option: "running"
)
print(json.dumps(response, indent=4))

# LIVE Deployment
datamodel_name = "MyDataModel_live"
response = datamodel.deploy_datamodel(
    datamodel_name=datamodel_name
    # No need to specify build_type, row_limit, or schema_origin
    # These will be internally set to "publish" and ignored
)
print(json.dumps(response, indent=4))
```

---

## Example 12: Describe DataModel

Describe a DataModel and export info to CSV.

```python
response = datamodel.describe_datamodel("pysense_databricks_ec")
print(json.dumps(response, indent=4))
df = api_client.to_dataframe(response)
print(df)

# Optional: Export the response to a CSV file
datamodel_info = api_client.export_to_csv(response, file_name="datamodel_info.csv")
```

---

## Example 13: Describe DataModel (Raw)

Get raw description of a DataModel.

```python
response = datamodel.describe_datamodel_raw("pysense_databricks_ec")
print(json.dumps(response, indent=4))
```

---

## Example 14: Get DataModel Shares

Get sharing information for a DataModel. An empty list means the model genuinely has no shares; if the model cannot be resolved, `{"ok": False, "error": "..."}` is returned instead (no longer an empty list).

```python
response = datamodel.get_datamodel_shares("pysense_databricks_ec")
print(json.dumps(response, indent=4))
df = api_client.to_dataframe(response)
print(df)
```

---

## Example 15: Get Datasecurity

Get datasecurity info for a datamodel. An empty list always means the model genuinely has zero rules; failures (model cannot be resolved, or rules cannot be fetched) return `{"ok": False, "error": "..."}` instead.

```python
response = datamodel.get_datasecurity("pysense_databricks")
print(json.dumps(response, indent=4))
df = api_client.to_dataframe(response)
print(df)
```

Get Datasecurity for all datamodels.

```python
response = datamodel.get_all_datamodel()
all_datamodels = []
for model in response:
    all_datamodels.append(model["title"])

all_ds = []
for datamodel_name in all_datamodels:
    response = datamodel.get_datasecurity(datamodel_name)
    if isinstance(response, list):  # a dict here is the {"ok": False, "error": "..."} failure shape
        all_ds.extend(response)
# Print the combined list of all datasecurity details
df = api_client.to_dataframe(all_ds)
print(df)
api_client.export_to_csv(all_ds, file_name="datamodel_security.csv")
```

---

## Example 15b: Update Datasecurity (EXTRACT)

Replace datasecurity rules on an EXTRACT datamodel (standalone migration phase).

```python
# Typically sourced from GET on the source environment
rules = [
    {
        "table": "orders",
        "column": "region",
        "datatype": "text",
        "members": ["EMEA"],
        "exclusionary": False,
        "shares": [{"type": "user", "partyId": "user_oid", "partyName": "user@example.com"}],
    }
]
response = datamodel.update_datasecurity("pysense_databricks", rules)
print(json.dumps(response, indent=4))
```

---

## Example 15c: Add Live Datasecurity Rules (Bulk)

Add multiple datasecurity rules to a LIVE datamodel.

```python
rules = [
    {
        "table": "orders",
        "column": "region",
        "datatype": "text",
        "members": ["EMEA"],
        "exclusionary": False,
        "shares": [{"type": "group", "partyId": "group_oid", "partyName": "Analysts"}],
    }
]
response = datamodel.set_live_datasecurity_add_many("live_sales_model", rules)
print(json.dumps(response, indent=4))
```

Notes: the extract cube must be **built and running** and the live model must be **published** for datasecurity writes; both methods **add** rules (`allMembers` is required, `live`/`fullname` are auto-filled for live models).

---

## Example 15d: Delete Datasecurity Rules for a Column

Remove all rules for one table/column (works for both EXTRACT and LIVE models). Combined with the add methods, this enables replace semantics.

```python
response = datamodel.delete_datasecurity("pysense_databricks", "orders", "region")
print(json.dumps(response, indent=4))  # {"success": True}
```

---

## Example 16: Get Datasecurity Information in Detail

Get detailed datasecurity info for a datamodel. An empty list always means the model genuinely has zero rules; failures (model cannot be resolved, or rules cannot be fetched) return `{"ok": False, "error": "..."}` instead.

```python
response = datamodel.get_datasecurity_detail("pysense_databricks")
df = api_client.to_dataframe(response)
print(df)
```

Get detailed datasecurity info for all datamodels.

```python
response = datamodel.get_all_datamodel()
all_datamodels = []
for model in response:
    all_datamodels.append(model["title"])

all_ds = []
for datamodel_name in all_datamodels:
    response = datamodel.get_datasecurity_detail(datamodel_name)
    if isinstance(response, list):  # a dict here is the {"ok": False, "error": "..."} failure shape
        all_ds.extend(response)
df = api_client.to_dataframe(all_ds)
print(df)
api_client.export_to_csv(all_ds, file_name="get_datasecurity_detail.csv")
```

---

## Example 17: Get Table and Columns

Get schema (tables and columns) for a DataModel.

```python
response = datamodel.get_model_schema("pysense_databricks")
print(json.dumps(response, indent=4))
df = api_client.to_dataframe(response)
print(df)
```

---

## Example 18: Add Shares

Add sharing permissions to a DataModel. Works for both EXTRACT (Elasticube)
and LIVE DataModels — EXTRACT support is new in 2.0.

```python
datamodel_name = "pysense_databricks"
shares_to_add = [
    {"name": "autotest@sisense.com", "type": "user", "permission": "EDIT"},
    {"name": "mig_test", "type": "group", "permission": "USE"},
    {"name": "viewer@sisense.com", "type": "user", "permission": "READ"},
]
response = datamodel.add_datamodel_shares(datamodel_name, shares_to_add)
if response.get("ok") is False:
    print(response["error"])
else:
    print(f"{response['message']} new={response['new_shares']} updated={response['updated_shares']}")
    for skip in response["skipped"]:
        print(f"skipped {skip['type']} '{skip['name']}': {skip['reason']}")
```

Notes (live-verified):

- On success this returns `{"success": True, "message": "...", "new_shares": <n>, "updated_shares": <n>, "skipped": [...]}`. `skipped` lists every requested share that was **not** submitted (`{"name", "type", "reason"}` — unknown user/group, inactive user, invalid type); always check it, since a skipped share never reached Sisense.
- EXTRACT shares merge with the model's existing raw permission list and are written via `PUT /api/elasticubes/localhost/{title}/permissions`; the LIVE path is unchanged (`PATCH` by oid). Both are keyed by `partyId`.
- A party that already has a share gets its permission updated in place (EXTRACT path) rather than duplicated.
- Shares for INACTIVE users are skipped and reported in `skipped` — Sisense returns HTTP 200 but silently drops such entries.
- When no given share resolves (unknown or inactive parties), the method returns `{"ok": False, "error": "...", "skipped": [...]}` instead of writing the existing shares back unchanged.

---

## Example 19: Get Data

Query data from a table in a DataModel and export to CSV. An empty list always means the query genuinely returned no rows; failures return `{"ok": False, "error": "..."}` (no longer an empty list).

```python
datamodel_name = "pysense_databricks"
table_name = "trips"
query = "SELECT count(*) FROM trips"
response = datamodel.get_data(datamodel_name, table_name, query)
print(json.dumps(response, indent=4))

# As DataFrame
df = api_client.to_dataframe(datamodel.get_data("pysense_databricks", "trips"))
print(df)

# As CSV
api_client.export_to_csv(response, file_name=f"{table_name}.csv")
```

---

## Example 20: Get DataModel Row Count

Get row count for a DataModel and export to CSV. If the model cannot be resolved, the `{"ok": False, "error": "..."}` dict is propagated; tables whose count query fails are skipped with a logged warning.

```python
datamodel_name = "pysense_databricks_ec"
response = datamodel.get_row_count(datamodel_name)
print(json.dumps(response, indent=4))
df = api_client.to_dataframe(datamodel.get_row_count(datamodel_name))
print(df)
api_client.export_to_csv(response, file_name=f"{datamodel_name}_count.csv")
```

---

## Example 21: Resolve DataModel Reference (ID or Name)

Resolve one or more DataModel references that may be either IDs or names.

```python
import json

# Mix of DataModel IDs and titles
datamodel_refs = [
    "60ca5fe3-dc7b-4db7-aaa4-7dff0ac30bcb",  # DataModel ID
    "MyDataModel_ec",  # DataModel title
]

for ref in datamodel_refs:
    result = datamodel.resolve_datamodel_reference(ref)
    print(f"Input reference: {ref}")
    print(json.dumps(result, indent=4))
    print("-" * 60)

# Example of using the resolved ID/title for another call
resolved = datamodel.resolve_datamodel_reference("MyDataModel_ec")
if resolved.get("success"):
    datamodel_id = resolved.get("datamodel_id")
    datamodel_title = resolved.get("datamodel_title")
    print(f"Resolved DataModel: ID={datamodel_id}, Title={datamodel_title}")
```

---

## Notes

- Adjust parameters as needed for your environment.
- For more details, refer to the documentation in the `docs/` folder.

---

---

## Example 22: Get ElastiCubes (Legacy / Windows-Compatible)

List all ElastiCubes using the legacy v1 endpoint. Works on both Linux and Windows Sisense deployments. Returns basic metadata (title, address, fullname). Prefer `get_all_datamodel` on Linux for richer metadata.

```python
response = datamodel.get_elasticubes()
print(json.dumps(response, indent=4))

df = api_client.to_dataframe(response)
api_client.export_to_csv(response, "elasticubes.csv")
```

---

## Example 23: Look Up a DataModel OID by Title

Resolve a data model title to its internal OID using the GraphQL ECM endpoint. Use this when you have a model title but need the OID for other API calls.

```python
result = datamodel.load_datamodel("SalesCube")
if "error" not in result:
    print(f"OID: {result['oid']}")
else:
    print(result["error"])
```

---

## Example 24: Delete a DataModel

Permanently delete a data model by title and server. Use with caution — this is irreversible.

```python
response = datamodel.delete_datamodel("SalesCube", "LocalHost")
print(response)
# {"success": True}
```

---

## Example 25: Replace Datasecurity Rules (Extract Model)

Overwrite all row-level security rules on an extract (ElastiCube) data model. Pass an empty list to remove all rules.

```python
rules = [{"table": "Orders", "column": "Region", "datatype": "text", "members": ["West", "North"], "exclusionary": False, "shares": [{"type": "user", "partyId": "user_oid_here"}]}]

response = datamodel.update_datasecurity("SalesCube", rules)
print(json.dumps(response, indent=4))
```

---

## Example 26: Add Datasecurity Rules to a Live Model

Append row-level security rules to a live data model.

```python
rules = [{"table": "Sales", "column": "Country", "datatype": "text", "members": ["USA"], "exclusionary": False, "shares": [{"type": "group", "partyId": "group_oid_here"}]}]

response = datamodel.set_live_datasecurity_add_many("LiveSalesCube", rules)
print(json.dumps(response, indent=4))
```

---

## Example 27: Get Raw Datasecurity Rules (for Migration)

Retrieve datasecurity rules exactly as the API returns them — unflattened, with raw `shares` — suitable for round-tripping to another environment.

```python
rules = datamodel.get_datasecurity_raw("SalesCube", datamodel_type="extract")
print(json.dumps(rules, indent=4))
```

---

## Example 28: Export a Data Model Schema

Export a data model's full schema, ready to be imported into a different Sisense environment.

```python
schema = datamodel.export_datamodel_schema("datamodel-oid-here", dependencies=["dataContext", "formulaManagement"])
print(schema.get("title"))
```

---

## Example 29: Import a Data Model Schema

Import a previously exported schema — as a plain create, an overwrite of an existing model, or a duplicate under a new title.

```python
# Plain create
result = datamodel.import_datamodel_schema(schema)

# Overwrite an existing model by OID (falls back to a plain create if the target OID is not found)
result = datamodel.import_datamodel_schema(schema, action="overwrite", target_datamodel_id="existing-datamodel-oid")

# Duplicate under a new title
result = datamodel.import_datamodel_schema(schema, action="duplicate", new_title="SalesCube (Copy)")

print(result)
# {"datamodel_id": "new-oid", "already_exists": False}
```

---

## Example 30: Get Raw Permissions (for Migration)

Retrieve a data model's share entries exactly as the API returns them — keyed by `partyId`, unresolved to names — suitable for round-tripping to another environment.

```python
extract_shares = datamodel.get_datamodel_permissions_extract("SalesCube")
live_shares = datamodel.get_datamodel_permissions_live("live-datamodel-oid")
print(extract_shares)
```

---

## Example 31: Replace Raw Permissions (Extract Model)

```python
shares = [{"partyId": "user_oid_here", "type": "user", "permission": "a"}]
response = datamodel.update_datamodel_permissions_extract("SalesCube", shares)
print(response)
```

---

## Example 32: Replace Raw Permissions (Live Model)

The LIVE model must already be published — publish it first with `deploy_datamodel` if it has never been built.

```python
shares = [{"partyId": "group_oid_here", "type": "group", "permission": "a"}]
response = datamodel.update_datamodel_permissions_live("live-datamodel-oid", shares)
print(response)
```

## Example 33: List and Look Up Perspectives

```python
# Every real perspective on the instance (the hidden per-model "Default" ones are skipped)
all_perspectives = datamodel.get_perspectives()

# Perspectives built over one root model (ID or title)
ecommerce_perspectives = datamodel.get_perspectives(datamodel="Sample ECommerce")

# Specific perspectives by name or oid — one string or a list
sales = datamodel.get_perspectives("Company Sales")
several = datamodel.get_perspectives(["Company Sales", "9674a154-0bc5-4bf2-b88b-0064f50db2e9"])

# Each object is what Sisense returns: oid, name, datamodelOid, and tables keyed by
# table/column oids, e.g. {"oid": ..., "diffType": "include", "columnsDiff": [{"oid": ..., "enabled": True}]}
for perspective in all_perspectives:
    print(perspective["datamodelTitle"], "->", perspective["name"], len(perspective["tables"]), "tables kept")

# Unknown names fail loudly and still hand back what was found
result = datamodel.get_perspectives(["Company Sales", "no-such-perspective"])
if result.get("ok") is False:
    print(result["error"], "| missing:", result["missing"], "| found:", len(result["results"]))
```

## Example 34: Delete a Perspective

```python
# By name (case-insensitive) or by oid. The root model and its data are untouched.
result = datamodel.delete_perspective("Company Sales")
# {"success": True, "message": "Perspective 'Company Sales' deleted.", "oid": "...", "name": "Company Sales",
#  "datamodelOid": "...", "datamodelTitle": "Sample ECommerce"}

# If the same name exists on two models, say which one
result = datamodel.delete_perspective("Company Sales", datamodel="Sample ECommerce")

# The built-in Default perspective is refused, and unknown names return the standard error dict
result = datamodel.delete_perspective("no-such-perspective")
if result.get("ok") is False:
    print(result["error"])
```

## Example 35: Create a Perspective

```python
# Keep two columns of @trips and every column of region; every other table is left out.
result = datamodel.create_perspective(
    "fes_assistant",
    "trips_for_chatbot",
    [
        {"table": "@trips", "columns": ["fare_amount", "tpep_dropoff_datetime"]},
        "region",  # a bare name keeps all of its columns
    ],
    description="Only what the dashboards use",
    ai_context="Taxi trips joined to regions; revenue is fare_amount.",
)
# {"success": True, "oid": "...", "name": "trips_for_chatbot", "datamodelOid": "...", "datamodelTitle": "fes_assistant",
#  "tables": [{"table": "@trips", "table_oid": "...", "columns_kept": 2, "columns_total": 6},
#             {"table": "region", "table_oid": "...", "columns_kept": 3, "columns_total": 3}],
#  "excluded_tables": [], "warnings": []}

# Typos fail before anything is created
bad = datamodel.create_perspective("fes_assistant", "oops", [{"table": "region", "columns": ["r_name", "zzz"]}])
# {"ok": False, "error": "Cannot create perspective 'oops' on 'fes_assistant': column 'zzz' not found in table 'region'"}
```
