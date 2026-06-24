# Queries Example Usage

Run JAQL and SQL queries against Sisense datasources and elasticubes.

---

## Prerequisites

```python
import os
import json
from pysisense import SisenseClient, Queries

config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

queries = Queries(api_client=api_client)
```

---

## Example 1: Run a JAQL Query

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
print(json.dumps(result, indent=4))
```

---

## Example 2: Run JAQL and Get CSV

```python
csv_result = queries.elasticubes_run_jaql_csv("SalesModel", jaql_payload)
if isinstance(csv_result, str):
    print(csv_result)
else:
    print(json.dumps(csv_result, indent=4))
```

