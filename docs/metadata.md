Metadata Class Documentation
============================

The `Metadata` class provides methods for Sisense datasource metadata: saved formulas (measures), saved filters (dimensions), datasource listing, and metadata queries.

* * * * *

Class: `Metadata`
-----------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `Metadata` class.

**Parameters:**

- `api_client` (SisenseClient, optional): An existing authenticated client.
- `debug` (bool): Enable debug logging when creating a new client.

* * * * *

### `get_datasource_measures(datasource=None, ds_full_name=None)`

Retrieves saved formula measures via `GET /api/metadata/measures`.

**Parameters:**

- `datasource` (str, optional): Datasource identifier.
- `ds_full_name` (str, optional): Full datasource name (maps to `dsFullName` query param).

**Returns:**

- `list` or `dict`: API payload on success, or `{"error": "..."}` on failure.

* * * * *

### `get_datasource_dimensions(datasource=None, ds_full_name=None)`

Retrieves saved filter dimensions via `GET /api/metadata/dimensions`.

**Parameters:**

- `datasource` (str, optional): Datasource identifier.
- `ds_full_name` (str, optional): Full datasource name (maps to `dsFullName` query param).

**Returns:**

- `list` or `dict`: API payload on success, or `{"error": "..."}` on failure.

* * * * *

### `get_datasources()`

Lists datasources via `GET /api/datasources`.

**Returns:**

- `list` or `dict`: Datasource list on success, or `{"error": "..."}` on failure.

* * * * *

### `add_datasource_measure(measure)`

Creates a saved formula measure via `POST /api/metadata/`.

**Parameters:**

- `measure` (dict): Measure definition in Sisense metadata format.

**Returns:**

- `dict`: Created measure on success (HTTP 200/201), or `{"error": "..."}` on failure.

* * * * *

### `post_metadata_query(query_payload)`

Executes a metadata query via `POST /api/metadata`.

**Parameters:**

- `query_payload` (dict): Query body required by the Sisense metadata API.

**Returns:**

- `dict`: Query result on success, or `{"error": "..."}` on failure.
