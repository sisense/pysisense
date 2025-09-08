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

Creates a new DataModel in Sisense.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel.

* `datamodel_type` (str): Type of the DataModel. Should be either "extract" (for Elasticube) or "live" (for Live).

#### Returns:

* `dict`: Dictionary with the DataModel ID if created successfully, or an error message.

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
See [`examples/datamodel_examples.py`](../examples/datamodel_examples.py) for detailed examples of how to define `connection_params` for each supported datasource.

---

### `create_connections(self, connection_payload)`

Creates a new connection using the provided payload.

#### Parameters:

* `connection_payload` (dict): The configuration payload for the connection.

#### Returns:

* `dict or None`: JSON response with connection details if successful, otherwise `None`.

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

* `build_type` (str): Type of deployment. Required for EXTRACT only. Options:

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

* `list`: List of dictionaries containing DataModel name, DataModel ID, party name, party type (user or group), and assigned permission level.

---

### `get_datasecurity(self, datamodel_name)`

Retrieves datasecurity table and column entries for a given DataModel in flat row format.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve datasecurity for.

#### Returns:

* `list`: List of dictionaries containing datamodel name, table name, column name, and associated security type. If no rules exist, a single entry with the datamodel name and empty values is returned.

---

### `get_datasecurity_detail(self, datamodel_name)`

Retrieves detailed datasecurity rules for a specific DataModel, including visibility at the share level.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve datasecurity rules for.

#### Returns:

* `list`: List of dictionaries, where each dictionary represents a column-level rule repeated for each share. Includes datamodel name, table name, column name, data type, value, exclusionary flag, share type, share name, and a user-friendly rule description.

---

### `get_model_schema(self, datamodel_name)`

Retrieves the schema of a DataModel, including tables and columns.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to retrieve the schema for.

#### Returns:

* `list`: List of dictionaries, each containing schema details for a column. Fields include datamodel name, type, dataset name, table name, column name, and column type. Returns an error message if the schema is not found.

---

### `add_datamodel_shares(self, datamodel_name, shares)`

Adds share entries (users and groups) to a DataModel.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel to add shares to.

* `shares` (list): List of dictionaries. Each dictionary must contain:

  * `name`: Name of the user or group

  * `type`: Party type (`user` or `group`)

  * `permission`: Permission level (`EDIT`, `READ`, or `USE`)

#### Returns:

* `dict`: Result of the share addition operation.

---

### `get_data(self, datamodel_name, table_name, query=None)`

Retrieves data from a specific table in a DataModel with optional custom SQL query support.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel.

* `table_name` (str): Name of the table to retrieve data from.

* `query` (str, optional): SQL query to apply as a filter on the data.

#### Returns:

* `list`: List of dictionaries where each dictionary represents a row of data.

---

### `get_row_count(self, datamodel_name)`

Retrieves the row count for each table in a specific DataModel.

#### Parameters:

* `datamodel_name` (str): Name of the DataModel.

#### Returns:

* `list`: List of dictionaries, each containing `table_name` and `row_count`. Includes an additional final row with the total row count.