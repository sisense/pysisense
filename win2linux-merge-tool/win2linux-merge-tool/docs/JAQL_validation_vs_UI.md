# JAQL validation vs application UI

Validation runs the same JAQL API the Sisense UI uses when loading a widget.

## Endpoint

- **UI**: `POST https://<host>/api/datasources/<datasource_title>/jaql`
- **Validation**: Same. The REST client uses `server_base_url + "/datasources/{title}/jaql"` (base URL already includes `/api`).

## Request body (per widget)

We send **one request per widget**, with the **full metadata array** (all JAQL items from all panels: rows, measures, scope), so the payload matches what the UI sends.

| Field | UI | Validation |
|-------|----|------------|
| `datasource` | Full object (address, title, id, database, fullname, live) | From `widget.datasource` (same source as UI) |
| `metadata` | Array of all items (rows + measures + scope) | Same: we collect all `metadata.panels[].items[]` with `jaql` into one array |
| `widgetType` | e.g. `chart/bar` | From `widget.type` |
| `dashboard` | Dashboard OID | Dashboard ID we're validating |
| `widget` | Widget OID | From `widget.oid` |
| `format` | `json` | `json` |
| `offset` | `0` | `0` |
| `m2mThresholdFlag` | `0` | `0` |
| `isMaskedResult` | `true` | `true` |
| `by` | `widget` | `widget` |
| `count` | `20000` | `20000` |
| `queryGuid` | Optional (UI may send) | Omitted; not required for validation |

## Example (your test widget)

When the UI loads the bar chart widget it sends one POST with:

```json
{
  "datasource": { "address": "LocalHost", "title": "Sample ECommerce", ... },
  "metadata": [
    { "jaql": { "table": "Commerce", "column": "Brand ID", ... }, "panel": "rows", ... },
    { "jaql": { "table": "Brand", "column": "Brand", ... }, "panel": "rows", ... },
    { "jaql": { "table": "Commerce", "column": "Condition", "agg": "count", ... }, "panel": "measures", ... },
    { "jaql": { "table": "Country", "column": "Country", ... }, "panel": "scope", ... }
  ],
  "offset": 0,
  "m2mThresholdFlag": 0,
  "isMaskedResult": true,
  "format": "json",
  "widgetType": "chart/bar",
  "by": "widget",
  "dashboard": "699f0e398025ad063ef73699;davidh-test",
  "widget": "699f0e698025ad063ef7369c;",
  "count": 20000
}
```

Validation builds the same structure from `get_widgets()`: for that widget it collects all items from `metadata.panels[].items[]` that have a `jaql` property (in panel/item order) into one `metadata` array and sends one request with the same fields.

## Data source

Widget list and metadata come from:

- `GET /v1/dashboards/{dashboard_id}/widgets` → `api_client.get_widgets(dashboard_id)`

So the JAQL we run is exactly what the API returns for that widget; we do not modify or subset the metadata when validating.
