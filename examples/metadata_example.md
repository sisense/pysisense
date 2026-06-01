# Metadata Example Usage

This guide demonstrates how to use the `Metadata` class for Sisense datasource metadata (saved formulas, saved filters, and metadata queries).

---

## Prerequisites

```python
import os
import json
from pysisense import SisenseClient, Metadata

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

metadata = Metadata(api_client=api_client)
```

---

## Example 1: List Datasources

```python
response = metadata.get_datasources()
print(json.dumps(response, indent=4))
```

---

## Example 2: Get Saved Measures (Formulas)

```python
response = metadata.get_datasource_measures(
    datasource="SalesModel",
    ds_full_name="localhost/SalesModel",
)
print(json.dumps(response, indent=4))
```

---

## Example 3: Get Saved Dimensions (Filters)

```python
response = metadata.get_datasource_dimensions(
    datasource="SalesModel",
    ds_full_name="localhost/SalesModel",
)
print(json.dumps(response, indent=4))
```

---

## Example 4: Add a Saved Measure

```python
measure = {
    "title": "Revenue Sum",
    "datasource": {"title": "SalesModel", "fullname": "localhost/SalesModel"},
    # Additional Sisense metadata fields as required by your environment
}
response = metadata.add_datasource_measure(measure)
print(json.dumps(response, indent=4))
```

---

## Example 5: Post a Metadata Query

```python
query_payload = {
    "metadata": [
        # Jaql / metadata query structure for your use case
    ],
}
response = metadata.post_metadata_query(query_payload)
print(json.dumps(response, indent=4))
```

---

## Notes

- `add_datasource_measure` uses `POST /api/metadata/` (trailing slash).
- `post_metadata_query` uses `POST /api/metadata` (no trailing slash).
- Pass `datasource` and/or `ds_full_name` when scoping measures or dimensions to a specific datamodel.
