Queries Class Documentation
===========================

The `Queries` class runs JAQL queries against Sisense datasources and elasticubes.

* * * * *

Class: `Queries`
----------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `Queries` class.

* * * * *

### `elasticube_run_jaql_query(datasource_name, jaql_payload)`

Runs a JAQL query via `POST /api/datasources/{name}/jaql` (validation-tab style).

**Parameters:**

- `datasource_name` (str): Datasource / elasticube name.
- `jaql_payload` (dict): JAQL request body.

**Returns:**

- `dict`: Query result on success, or `{"error": "..."}` on failure.

* * * * *

### `elasticubes_run_jaql_csv(datasource_name, jaql_payload)`

Runs a JAQL query returning CSV via `POST /api/datasources/{name}/jaql/csv`.

The CSV endpoint reads the JAQL from a URL-encoded form field named `data`, not from a JSON
body (the same way the Sisense UI's CSV export submits it). The method handles that encoding;
pass the same JAQL dict you would give `elasticube_run_jaql_query`.

**Returns:**

- `dict` or `str`: JSON result, raw CSV text, or `{"error": "..."}` on failure.
