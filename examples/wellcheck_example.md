# WellCheck Example Usage

This guide demonstrates how to use the `WellCheck` class from the `pysisense` package to run various health checks on Sisense dashboards and data models. Each example includes a short description and a code snippet you can copy into your own scripts.

---

## Prerequisites

- Ensure `config.yaml` is in the same folder as your script.
- Import required modules and initialize the API client and `WellCheck` class.

```python
import os
import json
from pysisense import SisenseClient, WellCheck

# Set the path to your config file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

# Initialize the API client
api_client = SisenseClient(config_file=config_path, debug=True)

# Initialize the WellCheck class using the shared APIClient
wellcheck = WellCheck(api_client=api_client, debug=True)
```

---

## Example 1: Check Dashboard Structure

Check dashboard structure (pivot, tabber, jump-to-dashboard, accordion usage) for one or more dashboards.

```python
dashboards = [
    "6893741265c9f5484dc999d7",   # dashboard ID
    "663b8f519ef48f00345bea45",   # dashboard ID
    "Academy AI Content",         # dashboard title
]

response = wellcheck.check_dashboard_structure(dashboards=dashboards)
print("Dashboard Structure Report:")
print(json.dumps(response, indent=4))

# Convert to DataFrame
dashboard_df = api_client.to_dataframe(response)
print(dashboard_df)
```

---

## Example 2: Check Dashboard Widget Counts

Compute the number of widgets per dashboard.

```python
dashboards = [
    "6893741265c9f5484dc999d7",
    "663b8f519ef48f00345bea45",
    "Academy AI Content",
]

rows = wellcheck.check_dashboard_widget_counts(dashboards=dashboards)
print(json.dumps(rows, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(rows)
print("Dashboard Widget Counts:")
print(df)

# Optional: Export to CSV
api_client.export_to_csv(rows, file_name="widget_counts.csv")
```

---

## Example 3: Check Pivot Widget Field Counts

Find pivot widgets that have many fields attached (for example, more than 20).

```python
dashboards = [
    "6893741265c9f5484dc999d7",
    "663b8f519ef48f00345bea45",
    "Academy AI Content",
]

fields = wellcheck.check_pivot_widget_fields(dashboards=dashboards, max_fields=20)
print(json.dumps(fields, indent=4))

# Convert to DataFrame
fields_df = api_client.to_dataframe(fields)
print("Pivot Widget Fields Counts:")
print(fields_df)

# Optional: Export to CSV
api_client.export_to_csv(fields, file_name="pivot_widget_fields_counts.csv")
```

---

## Example 4: Check Data Model Custom Tables (UNION Usage)

Inspect custom tables in a data model and detect whether their SQL expressions contain `UNION`.

```python
datamodels = "MyDataModel_ec"  # Can be ID or name

results = wellcheck.check_datamodel_custom_tables(datamodels=datamodels)
print("Data Model Custom Tables Report:")
print(json.dumps(results, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(results)
print("Data Model Custom Tables:")
print(df)

# Optional: Export to CSV
api_client.export_to_csv(results, file_name="datamodel_custom_tables_report.csv")
```

---

## Example 5: Check Data Model Island Tables

Find island tables (tables with no relationships) in a data model.

```python
datamodels = "MyDataModel_ec"  # Can be ID or name

results = wellcheck.check_datamodel_island_tables(datamodels=datamodels)
print("Data Model Island Tables Report:")
print(json.dumps(results, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(results)
print("Data Model Island Tables:")
print(df)
```

---

## Example 6: Check Data Model RLS Datatypes

Inspect Row Level Security (RLS) rules and see which columns and datatypes are used.

```python
datamodels = "MyDataModel_ec"  # Can be ID or name

results = wellcheck.check_datamodel_rls_datatypes(datamodels=datamodels)
print("Data Model Row Level Security Report:")
print(json.dumps(results, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(results)
print("Data Model Row Level Security:")
print(df)
```

---

## Example 7: Check Data Model Import Queries

Detect tables that use `importQuery` in their configuration.

```python
datamodels = "MyDataModel_ec"  # Can be ID or name

results = wellcheck.check_datamodel_import_queries(datamodels=datamodels)
print("Data Model Import Queries Report:")
print(json.dumps(results, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(results)
print("Data Model Import Queries:")
print(df)
```

---

## Example 8: Check Data Model Many-to-Many Relationships

Check for potential many-to-many (M2M) relationships between tables in a data model.

```python
datamodels = "MyDataModel_ec"  # Can be ID or name

results = wellcheck.check_datamodel_m2m_relationships(datamodels=datamodels)
print("Data Model Many-to-Many Relationships Report:")
print(json.dumps(results, indent=4))

# Convert to DataFrame
df = api_client.to_dataframe(results)
print("Data Model Many-to-Many Relationships:")
print(df)
```

---

## Example 9: Run Full WellCheck and Parse Results

Run the full suite of WellCheck checks for dashboards and data models, and then parse individual sections from the nested report.

```python
import json

dashboards = [
    "6893741265c9f5484dc999d7",   # dashboard ID
    "663b8f519ef48f00345bea45",   # dashboard ID
    "Academy AI Content",         # dashboard title
]

datamodels = (
    "60ca5fe3-dc7b-4db7-aaa4-7dff0ac30bcb",  # data model ID
    "MyDataModel_ec",                        # data model title
)

report = wellcheck.run_full_wellcheck(
    dashboards=dashboards,
    datamodels=datamodels,
    max_pivot_fields=20,
)

print("Full WellCheck report (nested JSON):")
print(json.dumps(report, indent=2))

# --------------------------------------------------------------
# Parsing individual checks (dashboards)
# --------------------------------------------------------------

# 1) Dashboard structure (pivot/tabber/JTD/accordion counts)
structure_rows = report.get("dashboards", {}).get("structure", [])
if structure_rows:
    df_structure = api_client.to_dataframe(structure_rows)
    print("
Dashboard structure:")
    print(df_structure)
    api_client.export_to_csv(structure_rows, file_name="dashboard_structure_report.csv")

# 2) Widget counts per dashboard
widget_count_rows = report.get("dashboards", {}).get("widget_counts", [])
if widget_count_rows:
    df_widget_counts = api_client.to_dataframe(widget_count_rows)
    print("
Widget counts per dashboard:")
    print(df_widget_counts)
    api_client.export_to_csv(widget_count_rows, file_name="widget_counts_report.csv")

# 3) Pivot widgets with many fields
pivot_field_rows = report.get("dashboards", {}).get("pivot_widget_fields", [])
if pivot_field_rows:
    df_pivot_fields = api_client.to_dataframe(pivot_field_rows)
    print("
Pivot widgets with more than 20 fields:")
    print(df_pivot_fields)
    api_client.export_to_csv(pivot_field_rows, file_name="pivot_widgets_report.csv")

# --------------------------------------------------------------
# Parsing individual checks (data models)
# --------------------------------------------------------------

datamodel_section = report.get("datamodels", {})

# 4) Custom tables and UNION usage
custom_table_rows = datamodel_section.get("custom_tables", [])
if custom_table_rows:
    df_custom_tables = api_client.to_dataframe(custom_table_rows)
    print("
Custom tables (with/without UNION):")
    print(df_custom_tables)
    api_client.export_to_csv(custom_table_rows, file_name="custom_tables_report.csv")

# 5) Island tables (no relationships)
island_table_rows = datamodel_section.get("island_tables", [])
if island_table_rows:
    df_island_tables = api_client.to_dataframe(island_table_rows)
    print("
Island tables (no relations):")
    print(df_island_tables)
    api_client.export_to_csv(island_table_rows, file_name="island_tables_report.csv")

# 6) RLS datatypes
rls_rows = datamodel_section.get("rls_datatypes", [])
if rls_rows:
    df_rls = api_client.to_dataframe(rls_rows)
    print("
RLS columns and datatypes:")
    print(df_rls)
    api_client.export_to_csv(rls_rows, file_name="rls_datatypes_report.csv")

# 7) Import queries on tables
import_query_rows = datamodel_section.get("import_queries", [])
if import_query_rows:
    df_import_queries = api_client.to_dataframe(import_query_rows)
    print("
Tables with import queries:")
    print(df_import_queries)
    api_client.export_to_csv(import_query_rows, file_name="import_queries_report.csv")

# 8) Many-to-many relationships
m2m_rows = datamodel_section.get("m2m_relationships", [])
if m2m_rows:
    df_m2m = api_client.to_dataframe(m2m_rows)
    print("
Many-to-many relationships:")
    print(df_m2m)
    api_client.export_to_csv(m2m_rows, file_name="m2m_relationships_report.csv")

# 9) Unused columns across all data models
unused_column_rows = datamodel_section.get("unused_columns", [])
if unused_column_rows:
    df_unused = api_client.to_dataframe(unused_column_rows)
    print("
Unused columns across all data models:")
    print(df_unused)
    api_client.export_to_csv(unused_column_rows, file_name="unused_columns_report.csv")
```

---

## Notes

- All methods accept either IDs or titles where indicated.
- You can pass a single string or a list/tuple of strings for `dashboards` and `datamodels`.
- Use `api_client.to_dataframe(...)` and `api_client.export_to_csv(...)` to move from JSON to tabular analysis and files.
