# Query execution reference

`from pysisense import Queries` — `queries = Queries(api_client=api_client)`

Runs caller-supplied JAQL query payloads directly against an elasticube or live datasource engine and returns actual result rows. This is not for schema discovery — the caller must already know the target datasource and the internal table/column names. Use `DataModel` for row counts per table, and `Metadata` for browsing the semantic layer (measures, dimensions) — see `references/metadata.md` for the distinction from `Metadata.post_metadata_query` (that one resolves schema/metadata-level queries against `/api/metadata`, not row data).

## Methods

| Method | Use when |
|---|---|
| `elasticube_run_jaql_query(datasource_name, jaql_payload)` | Run a JAQL query and get parsed JSON back — "validation-tab style" query, per the docstring. |
| `elasticubes_run_jaql_csv(datasource_name, jaql_payload)` | Same JAQL query, but returns CSV — prefer this for large result sets or when the caller wants to hand results straight to a file/dataframe pipeline. |

Both hit `POST /api/datasources/{datasource_name}/jaql` and `POST /api/datasources/{datasource_name}/jaql/csv` respectively, with the same `jaql_payload` shape.

## JAQL payload shape

`jaql_payload` needs a `datasource` reference and a `metadata` array of JAQL field definitions:

```python
jaql_payload = {
    "datasource": {"title": "SalesModel", "fullname": "localhost/SalesModel"},
    "metadata": [
        {
            "jaql": {
                "dim": "[Orders].[Amount]",
                "agg": "sum",
            }
        }
    ],
}
result = queries.elasticube_run_jaql_query("SalesModel", jaql_payload)
```

`datasource_name` (the method argument) and `jaql_payload["datasource"]` are both required — the former selects the URL path, the latter is part of the JAQL body itself. The SDK does not validate or shape the payload beyond passing it through — build the `metadata` array per standard Sisense JAQL field/dimension syntax (dims, aggs, filters, sorts, etc.) as needed for the query.

## CSV variant

```python
csv_result = queries.elasticubes_run_jaql_csv("SalesModel", jaql_payload)
if isinstance(csv_result, str):
    print(csv_result)          # raw CSV text
else:
    print(csv_result)          # parsed JSON (dict or list) — server didn't return CSV
```

`elasticubes_run_jaql_csv` returns `dict[str, Any] | str`: parsed JSON if the response is JSON, raw CSV text otherwise, or `{"error": "..."}` on failure. Check `isinstance(result, str)` before treating it as CSV text — a JSON-shaped error or result dict can still come back instead.

## Return shape / error convention

Both methods follow the standard SDK convention: the parsed response body on success, or `{"error": "..."}` on failure (no response received, or non-OK HTTP status). `elasticube_run_jaql_query` falls back to `{"success": True}` if a 2xx response has no JSON body; `elasticubes_run_jaql_csv` falls back to the raw response text (or `None`) under the same condition, per its plain-text/CSV nature.
