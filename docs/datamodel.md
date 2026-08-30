# DataModel Class Documentation

The `DataModel` class provides access to various Sisense API endpoints related to data models, connections, and table schemas. This documentation outlines the methods available in the class and the expected inputs and outputs.

## Initialization

### `__init__(self, api_client=None, debug=False)`

Initializes the `DataModel` class.

#### Parameters:

* `api_client` (APIClient, optional): An existing `APIClient` instance. If `None`, a new `APIClient` is created.

* `debug` (bool, optional): Enables debug logging if set to `True`. Default is `False`.

---

## Methods

> **Failure contract (2.0):** every failure return in this module is the standard error dict `{"ok": False, "error": "...", "status_code": <int, when an HTTP status exists>}`. No method returns `[]`, `None`, or a bare `{"error": ...}` on failure — an empty list from a read method always means a genuinely empty result.

### `get_datamodel(self, datamodel_name)`

Retrieves a DataModel by its name.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve.

#### Returns:

* `dict`: Full DataModel details if found, or a dictionary with an error message.

---

### `get_all_datamodel(self)`

Retrieves metadata details of all DataModels using an undocumented internal API. This includes fields like build status, size, and timestamps which may not be available through standard public endpoints.

#### Returns:

* `dict`: Parsed metadata details of all DataModels, including:

  * `oid`: Unique identifier of the DataModel

  * `title`: Title of the DataModel

  * `type`: Type of the DataModel (e.g., Elasticube, Live)

  * `status`: Build or connection status

  * `sizeInMb`: Size of the DataModel in megabytes

  Or a dictionary with an error message.

---

### `get_connection(self, connection_name)`

Retrieves a Connection by its name.

#### Parameters:

* `connection_name` (str): Name of the connection to filter by.

#### Returns:

* `list`: Connection details if found, or a dictionary with an error message.

---

### `get_connections_all()`

Lists all connections via `GET /api/v2/connections`.

The former `get_connections()` alias (deprecated in 1.1.0) was removed in 2.0 — use `get_connections_all`, which makes the all-vs-single distinction from `get_connection` explicit.

#### Returns:

* `list`: Connection objects on success, or `{"ok": False, "error": "..."}` on failure.

---

### `update_connection(connection_id, connection_data)`

Updates a connection via `PATCH /api/v2/connections/{connection_id}`. Only fields in `connection_data` are sent.

#### Parameters:

* `connection_id` (str): Connection `oid`.
* `connection_data` (dict): Fields to update (for example `name`, `parameters`).

#### Returns:

* `dict`: Updated connection on success, or `{"ok": False, "error": "..."}` on failure.

---

### `get_table_schema(self, connection_name, database_name, schema_name, table_name)`

Retrieves the schema of a table in a specified connection from Data Source.
This method uses an undocumented Sisense API endpoint to fetch the table schema details.
NOTE: This endpoint is undocumented and may change in future versions of Sisense.
It is recommended to use this method with caution.

> **Note:** Uses an undocumented Sisense API endpoint:

> `/api/v1/connection/{connection_id}/table_schema_details`

> Use with caution as behavior may change in future Sisense versions.

#### Parameters:

* `connection_name` (str): Name of the connection.

* `database_name` (str): Name of the database.

* `schema_name` (str): Name of the schema.

* `table_name` (str): Name of the table.

#### Returns:

* `dict`: Contains catalog name, schema name, table name, table type (if available), and a list of column definitions. Each column includes:

  * `columnName`: Name of the column

  * `columnOrder`: Position of the column in the table

  * `dbType`: Numeric code representing the column's data type

  * `size`: Size limit of the column

  * `precision`: Total number of significant digits

  * `scale`: Number of digits to the right of the decimal point

  * `nestedIn`: If applicable, indicates nesting structure

  If the table is not found or an error occurs, a dictionary with an error message is returned.

---

### `create_datamodel(self, datamodel_name, datamodel_type)`

Creates a new DataModel in Sisense. Before creating, checks that no DataModel with the same title already exists — a duplicate title otherwise surfaces as an opaque HTTP 500 from the API, and now returns a clear "already exists" error instead.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel.

* `datamodel_type` (`Literal["extract", "live"]`): Type of the DataModel — "extract" (Elasticube) or "live".

#### Returns:

* `dict`: Dictionary with the DataModel ID if created successfully, or an error message (including a clear "already exists" error when the title is taken).

---

### `generate_connections_payload(self, datasource_type, connection_params)`

Generates the appropriate connection payload based on the specified datasource type.

#### Parameters:

- `datasource_type` (str): Type of datasource.  
  Currently supported values are:
  - `"ATHENA"`
  - `"REDSHIFT"`
  - `"DATABRICKS"`
  - `"BIGQUERY"`

- `connection_params` (dict): Dictionary containing required parameters for the specified datasource.

#### Returns:

- `dict`: A structured connection payload formatted for the specified datasource type.

*Note:*  
Only the above four data sources are supported at this time.  
See [`examples/datamodel_example.md`](../examples/datamodel_example.md) for detailed examples of how to define `connection_params` for each supported datasource.

---

### `create_connections(self, connection_payload)`

Creates a new connection using the provided payload.

#### Parameters:

* `connection_payload` (dict): The configuration payload for the connection.

#### Returns:

* `dict`: JSON response with connection details if successful (HTTP 201), or `{"ok": False, "error": "..."}` on failure (no longer `None`).

---

### `create_dataset(self, datamodel_name, connection_name, database_name, schema_name, dataset_name=None)`

Creates a new dataset in the specified DataModel.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel where the dataset will be created.

* `connection_name` (str): Name of the connection to use.

* `database_name` (str): Name of the data source database.

* `schema_name` (str): Name of the data source schema.

* `dataset_name` (str, optional): Name of the dataset. Defaults to schema name if not provided.

#### Returns:

* `dict`: A dictionary containing the full dataset object on success, or an error message on failure.

---

### `create_table(self, datamodel_name, table_name, database_name=None, schema_name=None, dataset_id=None, import_query=None, description="", tags=None, build_behavior_config=None)`

Creates a new table in the specified DataModel.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel where the table will be created.

* `table_name` (str): Name of the table to create.

* `database_name` (str, optional): Name of the data source database. If not provided, will try to infer from the DataModel.

* `schema_name` (str, optional): Name of the data source schema. If not provided, will try to infer from the DataModel.

* `dataset_id` (str, optional): ID of the dataset where the table will be created. If not provided, will try to infer from the DataModel.

* `import_query` (str, optional): SQL statement used as custom import query.

* `description` (str, optional): Description for the table.

* `tags` (list, optional): List of tags to apply to the table.

* `build_behavior_config` (dict, optional): Configuration for table build behavior.

#### Returns:

* `dict`: Table object if created successfully, or a dictionary with an error message.

---

### `setup_datamodel(self, datamodel_name, datamodel_type, connection_name, database_name, schema_name, tables, dataset_name=None)`

Sets up a DataModel using an existing connection by creating a DataModel, dataset, and table(s).

#### Parameters:

* `datamodel_name` (str): Name of the DataModel.

* `datamodel_type` (str): Type of the DataModel. Should be either "extract" (for Elasticube) or "live" (for Live).

* `connection_name` (str): Name of the connection to use.

* `database_name` (str): Name of the data source database.

* `schema_name` (str): Name of the data source schema.

* `dataset_name` (str, optional): Name of the dataset. Defaults to schema name if not provided.

* `tables` (list): List of tables to create in the DataModel. Each table should be a dictionary with keys:

  * `table_name`

  * `import_query` (optional)

  * `description` (optional)

  * `tags` (optional)

  * `build_behavior_config` (optional)

#### Returns:

* `dict`: A dictionary containing the created DataModel components:

  * `datamodel_id`

  * `dataset_id`

  * `tables`: List of created table objects

    Or a dictionary with an error message.

---

### `deploy_datamodel(self, datamodel_name, build_type="full", row_limit=0, schema_origin="latest")`

Deploys (builds or publishes) the specified DataModel based on its type.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to deploy.

* `build_type` (`Literal["full", "by_table", "schema_changes"]`): Type of deployment. Required for EXTRACT only. Options:

  * `schema_changes`

  * `by_table`

  * `full`

* `row_limit` (int): Row limit for build. Applicable only for EXTRACT.

* `schema_origin` (str): Schema origin for build. Options:

  * `latest`

  * `running`

#### Returns:

* `dict`: Deployment result including build or publish status. For Elasticube, includes build outcome. For Live model, includes publish status, e.g., `{ "publishElasticube": true }`. If failed, returns error details.

---

### `describe_datamodel_raw(self, datamodel_name)`

Retrieves detailed information about a specific DataModel, including share details.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to describe.

#### Returns:

* `dict`: Dictionary containing detailed raw DataModel metadata including name, ID, type, timestamps for last build and update, and a list of dataset objects. Returns an error message if not found.

---

### `describe_datamodel(self, datamodel_name)`

Retrieves detailed DataModel structure in a flat, row-based format suitable for DataFrame or CSV export.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to describe.

#### Returns:

* `list`: List of dictionaries, each representing a single table row. Fields include DataModel metadata and connection context such as datamodel name, ID, type, last build time, provider, connection name, table name, and table type.

---

### `get_datamodel_shares(self, datamodel_name)`

Retrieves all share entries (users and groups) for a given DataModel in flat row format.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve shares for.

#### Returns:

* `list`: List of dictionaries containing DataModel name, DataModel ID, party name, party type (user or group), and assigned permission level. An empty list means the model genuinely has no shares. If the model cannot be resolved, returns `{"ok": False, "error": "..."}` (no longer an empty list).

---

### `get_datasecurity(self, datamodel_name)`

Retrieves datasecurity table and column entries for a given DataModel in flat row format.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve datasecurity for.

#### Returns:

* `list`: List of dictionaries containing datamodel name, table name, column name, and associated security type — one row per secured column, so the row count equals the number of rules. An empty list always means the model genuinely has zero rules. On failure (the model cannot be resolved, or the rules cannot be fetched), returns `{"ok": False, "error": "..."}` instead.

---

### `get_datasecurity_detail(self, datamodel_name)`

Retrieves detailed datasecurity rules for a specific DataModel, including visibility at the share level.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve datasecurity rules for.

#### Returns:

* `list`: List of dictionaries, where each dictionary represents a column-level rule repeated for each share. Includes datamodel name, table name, column name, data type, value, exclusionary flag, share type, share name, and a user-friendly rule description. An empty list always means the model genuinely has zero rules. On failure (the model cannot be resolved, or the rules cannot be fetched), returns `{"ok": False, "error": "..."}` instead.

---

### `update_datasecurity(self, datamodel_name, datasecurity)`

**Adds** datasecurity rules to an EXTRACT datamodel via `POST /api/elasticubes/localhost/{datamodel_name}/datasecurity` (the API adds rules in bulk; it does not replace — to replace a column's rules, remove them first with `delete_datasecurity`). Server-managed fields (`_id`, `created`, `lastModified`, `importedIdIdentifier`) are stripped automatically so read-back rules can be re-submitted. The Elasticube must be **built and running** — writes are rejected for draft cubes.

#### Parameters:

* `datamodel_name` (str): Title of the EXTRACT datamodel.
* `datasecurity` (list): Rule list in Sisense API format (`table`, `column`, `datatype`, `members` — list of strings, `exclusionary`, `shares`, `allMembers`).

#### Returns:

* `dict`: API response on success, or `{"ok": False, "error": "..."}` on failure.

---

### `set_live_datasecurity_add_many(self, datamodel_name, rules)`

Adds multiple datasecurity rules to a LIVE datamodel via `POST /api/v1/elasticubes/live/{datamodel_name}/datasecurity/addMany`. Each rule requires `table`, `column`, `datatype`, `members` (list of strings), `exclusionary`, `shares`, `allMembers`, `live`, and `fullname` (`"live:{title}"`) — `live` and `fullname` are auto-filled when omitted, and server-managed fields are stripped automatically. The live model must be **published**; draft live models fail with "Elasticube has not been found".

#### Parameters:

* `datamodel_name` (str): Title of the LIVE datamodel.
* `rules` (list): Rule objects to add in Sisense API format.

#### Returns:

* `dict`: API response on success, or `{"ok": False, "error": "..."}` on failure.

---

### `delete_datasecurity(self, datamodel_name, table, column)`

Deletes all datasecurity rules for one table/column via `DELETE {datasecurity_endpoint}/{table}/{column}`, using the endpoint flavor for the model's type (EXTRACT or LIVE). Combined with the add methods above, this enables replace semantics.

#### Parameters:

* `datamodel_name` (str): Title of the datamodel.
* `table` (str): Table name the rules apply to.
* `column` (str): Column name the rules apply to.

#### Returns:

* `dict`: `{"success": True}` on success, or `{"ok": False, "error": "..."}` on failure.

---

### `get_datasecurity_raw(self, datamodel_name, datamodel_type=None)`

Retrieves the raw, unprocessed datasecurity rules for a DataModel — each rule exactly as the API returns it (`members`, `exclusionary`, raw `shares`), with no flattening, deduplication, or share-name resolution. Use this instead of `get_datasecurity`/`get_datasecurity_detail` when a rule needs to be round-tripped as-is, for example when migrating rules between environments.

#### Parameters:

* `datamodel_name` (str): Name (title) of the DataModel to retrieve raw datasecurity rules for.
* `datamodel_type` (str, optional): The DataModel's type (`"extract"` or `"live"`), if already known. When provided, the datasecurity endpoint is built directly, skipping the DataModel resolve call. When omitted, the DataModel is resolved by name first.

#### Returns:

* `list[dict] | dict`: The raw list of datasecurity rule objects from the API, or `{"ok": False, "error": "..."}` on failure (including when the DataModel cannot be resolved).

---

### `get_model_schema(self, datamodel_name)`

Retrieves the schema of a DataModel, including tables and columns.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve the schema for.

#### Returns:

* `list`: List of dictionaries, each containing schema details for a column. Fields include datamodel name, type, dataset name, table name, column name, and column type. Returns an error message if the schema is not found.

---

### `add_datamodel_shares(self, datamodel_name, shares)`

Adds share entries (users and groups) to a DataModel. Both EXTRACT (Elasticube) and LIVE DataModels are supported (EXTRACT support is new in 2.0 — the old "Cannot add shares to EXTRACT DataModels" error is retired).

Behavior (live-verified):

* Both model types key share entries by `partyId`, but use different endpoints and verbs: EXTRACT shares are merged with the existing raw permission list and written via `PUT /api/elasticubes/localhost/{title}/permissions`; LIVE shares are written via `PATCH /api/v1/elasticubes/live/{oid}/permissions` (unchanged).
* A party that already has a share gets its permission updated in place instead of a duplicate entry (EXTRACT path).
* Shares for INACTIVE users are skipped with a logged warning — Sisense accepts the write (HTTP 200) but silently drops such entries, so submitting them would report success for a share that never lands.
* When NO given share resolves (unknown or inactive parties), the method returns `{"ok": False, "error": "..."}` instead of writing the existing shares back unchanged.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to add shares to.

* `shares` (list): List of dictionaries. Each dictionary must contain:

  * `name`: Name of the user or group

  * `type`: Party type (`user` or `group`)

  * `permission`: Permission level (`EDIT`, `READ`, or `USE`)

#### Returns:

* `dict`: API response on success, or `{"ok": False, "error": "..."}` on failure (including when none of the given shares can be resolved — no changes are written in that case).

---

### `get_datamodel_permissions_extract(self, datamodel_title)`

Retrieves raw share entries for an EXTRACT (Elasticube) DataModel via `GET /api/elasticubes/localhost/{datamodel_title}/permissions`. Returns the raw `shares` list — each entry keyed by `partyId` and not resolved to a user/group name. Use `get_datamodel_shares` instead for a resolved, human-readable view.

#### Parameters:

* `datamodel_title` (str): Title of the EXTRACT DataModel.

#### Returns:

* `list[dict] | dict`: The raw list of share objects from the API, or `{"ok": False, "error": "..."}` on failure.

---

### `get_datamodel_permissions_live(self, datamodel_id)`

Retrieves raw share entries for a LIVE DataModel via `GET /api/v1/elasticubes/live/{datamodel_id}/permissions`. Returns the raw share list — each entry keyed by `partyId` and not resolved to a user/group name.

#### Parameters:

* `datamodel_id` (str): OID of the LIVE DataModel.

#### Returns:

* `list[dict] | dict`: The raw list of share objects from the API, or `{"ok": False, "error": "..."}` on failure.

---

### `update_datamodel_permissions_extract(self, datamodel_title, shares)`

Replaces share entries for an EXTRACT (Elasticube) DataModel via `PUT /api/elasticubes/localhost/{datamodel_title}/permissions`, sending the full raw share list. Use `add_datamodel_shares` instead for name/email-based share management.

Uses `PUT` because that's what the EXTRACT permissions endpoint requires — the LIVE counterpart uses `PATCH` (see below). This is an API difference between the two endpoints, not an inconsistency between the two methods.

#### Parameters:

* `datamodel_title` (str): Title of the EXTRACT DataModel.
* `shares` (list[dict]): Raw share objects, each with `partyId`, `type` (`"user"` or `"group"`), and `permission`.

#### Returns:

* `dict`: API response on success, or `{"ok": False, "error": "..."}` on failure.

---

### `update_datamodel_permissions_live(self, datamodel_id, shares)`

Replaces share entries for a LIVE DataModel via `PATCH /api/v1/elasticubes/live/{datamodel_id}/permissions`, sending the full raw share list. The LIVE model must already be published — publish it first with `deploy_datamodel` if it has never been built.

#### Parameters:

* `datamodel_id` (str): OID of the LIVE DataModel.
* `shares` (list[dict]): Raw share objects, each with `partyId`, `type` (`"user"` or `"group"`), and `permission`.

#### Returns:

* `dict`: API response on success, or `{"ok": False, "error": "..."}` on failure.

---

### `get_data(self, datamodel_name, table_name, query=None)`

Retrieves data from a specific table in a DataModel with optional custom SQL query support.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel.

* `table_name` (str): Name of the table to retrieve data from.

* `query` (str, optional): SQL query to apply as a filter on the data.

#### Returns:

* `list`: List of dictionaries where each dictionary represents a row of data. An empty list always means the query genuinely returned no rows. On failure, returns `{"ok": False, "error": "..."}` (no longer an empty list).

---

### `get_row_count(self, datamodel_name)`

Retrieves the row count for each table in a specific DataModel.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel.

#### Returns:

* `list`: List of dictionaries, each containing `table_name` and `row_count`. Includes an additional final row with the total row count. If the DataModel cannot be resolved, the `{"ok": False, "error": "..."}` dict is propagated; tables whose count query fails are skipped with a logged warning.

---

### `resolve_datamodel_reference(datamodel_ref)`

Resolves a data model reference (ID or name) to a concrete data model ID and title.

This helper accepts a single string that may be either:

- A Sisense data model ID, or  
- A data model title (name).

It first attempts to treat the reference as an ID using the underlying
“get by ID” logic. If that fails or the reference does not look like an
ID, it falls back to the “get by name” logic.

**Parameters:**

- `datamodel_ref` (str):  
  Data model reference to resolve. This can be either an ID or a name.

**Returns:**

- `dict`: Dictionary with the following keys:
  - `success` (bool): `True` if the reference was resolved to a data model; otherwise `False`.
  - `status_code` (int):  
    `200` if resolved successfully, `404` if not found, or `500` if an unexpected error occurred.
  - `datamodel_id` (str or None): Resolved data model ID (`oid`) if found, otherwise `None`.
  - `datamodel_title` (str or None): Resolved data model title if found, otherwise `None`.
  - `error` (str or None): Error message if `success` is `False`, otherwise `None`.

* * * * *

### `export_datamodel_schema(datamodel_id, dependencies=None)`

Exports a data model's full schema definition for re-import elsewhere (`GET /api/v2/datamodel-exports/schema`, or the legacy streaming export endpoint on Windows deployments). Returns the exported schema JSON as-is, ready to be passed to `import_datamodel_schema` — typically against a different Sisense environment.

**Parameters:**

-   `datamodel_id` (str): OID of the data model to export.
-   `dependencies` (list[str], optional): API dependency identifiers to include in the export (for example `"dataContext"`, `"scopeConfiguration"`, `"formulaManagement"`, `"drillHierarchies"`, `"perspectives"`). Windows: has no effect, the export endpoint used there accepts no dependencies parameter.

**Returns:**

-   `dict`: The exported schema object on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `import_datamodel_schema(schema, action=None, target_datamodel_id=None, new_title=None)`

Imports a data model schema (as produced by `export_datamodel_schema`) via `POST /api/v2/datamodel-imports/schema`. When `action="overwrite"` and `target_datamodel_id` is provided, targets that existing data model via the `datamodelId` query parameter; if not found (404), automatically retries as a plain create. When `action="duplicate"`, imports as a new data model titled `new_title` (or `"<title> (Duplicate)"` when omitted). Any other `action` value performs a plain create.

**Parameters:**

-   `schema` (dict): Schema object to import, typically produced by `export_datamodel_schema`.
-   `action` (str, optional): One of `"overwrite"` or `"duplicate"`. Any other value (including `None`) performs a plain create.
-   `target_datamodel_id` (str, optional): OID of the existing data model to overwrite. Required for `action="overwrite"` to take effect.
-   `new_title` (str, optional): Title for the duplicated data model. Used only when `action="duplicate"`.

**Returns:**

-   `dict`: `{"datamodel_id": <str or None>, "already_exists": False}` on success, or `{"ok": False, "error": "...", "already_exists": bool}` on failure. `already_exists` is `True` when the import failed because a data model with the same title already exists on the target under a different ID.

* * * * *

### `get_elasticubes()`

Lists all ElastiCubes using the legacy v1 endpoint (`GET /api/v1/elasticubes/getElasticubes`). Works on both Linux and Windows Sisense deployments. Returns basic metadata including `title`, `address`, and `fullname`. Prefer `get_all_datamodel` on Linux for richer metadata (build status, size, timestamps).

**Returns:**

-   `list[dict]`: List of ElastiCube objects on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `load_datamodel(title, server="LocalHost")`

Looks up a data model's OID by title using the GraphQL ECM endpoint (`POST /api/v2/ecm/` — `elasticubeByTitle` query). Use this to resolve a model title to its internal identifier when the OID is not already known.

**Parameters:**

-   `title` (str): Exact title of the data model to look up.
-   `server` (str, optional): Server name where the model is hosted. Defaults to `"LocalHost"`.

**Returns:**

-   `dict`: Contains `oid` and `__typename` on success, or `{"ok": False, "error": "..."}` on failure (including GraphQL-level errors that arrive as HTTP 200).

* * * * *

### `delete_datamodel(title, server)`

Permanently deletes a data model using the GraphQL ECM endpoint (`POST /api/v2/ecm/` — `removeElasticube` mutation).

**Parameters:**

-   `title` (str): Exact title of the data model to delete.
-   `server` (str): Server name where the model is hosted (e.g. `"LocalHost"`).

**Returns:**

-   `dict`: `{"success": True}` on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `update_datasecurity(datamodel_name, datasecurity)`

**Adds** row-level security rules to an extract (ElastiCube) data model. Sends `POST /api/elasticubes/localhost/{title}/datasecurity` (bulk add — existing rules are kept; use `delete_datasecurity` first to replace a column's rules). The Elasticube must be built and running.

Only supported for extract-type data models. For live models use `set_live_datasecurity_add_many`.

**Parameters:**

-   `datamodel_name` (str): Title of the extract data model.
-   `datasecurity` (list[dict]): Datasecurity rule list. Each rule must be a Sisense datasecurity object including `table`, `column`, `datatype`, `members` (list of strings), `exclusionary`, `shares`, and `allMembers`. Server-managed fields are stripped automatically.

**Returns:**

-   `dict`: API response body on success, or `{"ok": False, "error": "..."}` on failure.

* * * * *

### `set_live_datasecurity_add_many(datamodel_name, rules)`

Appends row-level security rules to a live data model. Sends `POST /api/v1/elasticubes/live/{title}/datasecurity/addMany`. Rules are added to the existing set rather than replacing it.

Only supported for live-type data models. For extract models use `update_datasecurity`.

**Parameters:**

-   `datamodel_name` (str): Title of the live data model.
-   `rules` (list[dict]): Datasecurity rules to append.

**Returns:**

-   `dict`: API response body on success, or `{"ok": False, "error": "..."}` on failure.
