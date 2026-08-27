# Metadata operations reference

`from pysisense import Metadata` — `metadata = Metadata(api_client=api_client)`

Read-only(-ish) access to a datasource's semantic layer: saved formula measures, saved filter dimensions, the datasource list, and raw metadata queries. This class does **not** build or modify data model schemas (tables, columns, connections, builds) — that's `DataModel`. It also does not run JAQL/SQL queries against actual row data — that's `Queries` (`elasticube_run_jaql_query`, `elasticubes_run_jaql_csv`). `Metadata` is metadata *about* a datasource's schema/saved objects, not data retrieval.

## Listing and reading

| Method | Use when |
|---|---|
| `get_datasources()` | All datasources visible to the authenticated user. `GET /api/datasources`. |
| `get_datasource_measures(datasource=None, ds_full_name=None)` | Saved formula measures for a datasource. `GET /api/metadata/measures`. |
| `get_datasource_dimensions(datasource=None, ds_full_name=None)` | Saved filter dimensions for a datasource. `GET /api/metadata/dimensions`. |

Both `get_datasource_measures` and `get_datasource_dimensions` take the same two optional filters — pass either or both:
- `datasource` — datasource identifier (e.g. a datamodel title).
- `ds_full_name` — full datasource name (e.g. `"localhost/MyModel"`), sent as query param `dsFullName`.

Passing neither returns everything the token user can see across datasources.

```python
measures = metadata.get_datasource_measures(datasource="SalesModel", ds_full_name="localhost/SalesModel")
dimensions = metadata.get_datasource_dimensions(datasource="SalesModel", ds_full_name="localhost/SalesModel")
datasources = metadata.get_datasources()
```

## Return shape

All read methods return `list[dict] | dict`: the API payload (typically a list) on success, or `{"error": "..."}` on failure — check `"error" in result` before iterating, since a failure result is a dict, not a list.

## Adding a saved measure

`add_datasource_measure(measure)` — `POST /api/metadata/` (note the trailing slash — distinct from `post_metadata_query`'s endpoint). `measure` must be a dict describing the saved formula in Sisense metadata format (title, datasource reference, expression, etc.) — the SDK does not validate its internal shape beyond `isinstance(measure, dict)`.

```python
measure = {
    "title": "Revenue Sum",
    "datasource": {"title": "SalesModel", "fullname": "localhost/SalesModel"},
    # remaining fields per Sisense metadata format (expression, type, ...)
}
result = metadata.add_datasource_measure(measure)
```

Returns the created measure object on `200`/`201`, `{"success": True}` if the response body isn't JSON, or `{"error": "..."}` on failure.

## Raw metadata queries

`post_metadata_query(query_payload)` — `POST /api/metadata` (no trailing slash). Takes an arbitrary metadata query body as required by the Sisense metadata API (e.g. a `{"metadata": [...]}` JAQL-shaped structure describing measures/dimensions to resolve) and returns whatever the endpoint gives back — the SDK does not shape or validate the payload beyond `isinstance(query_payload, dict)`.

```python
query_payload = {
    "metadata": [
        # jaql / metadata query structure — schema-level, not row-level data
    ],
}
result = metadata.post_metadata_query(query_payload)
```

Use this only for schema/metadata-level lookups against the metadata API. To actually execute a JAQL query and get back result rows/CSV from an elasticube or live datasource, use `Queries.elasticube_run_jaql_query` / `Queries.elasticubes_run_jaql_csv` instead — different endpoint, different purpose.

Same failure convention as `add_datasource_measure`: `{"success": True}` if the body isn't JSON, `{"error": "..."}` on non-2xx.
