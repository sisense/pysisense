WellCheck Class Documentation
=============================

The `WellCheck` class provides a collection of health-check and complexity-check utilities for Sisense dashboards and data models. It helps you analyze structures, widget usage, data model tables, relationships, and security rules to surface potential issues or optimization opportunities.

* * * * *

Class: `WellCheck`
------------------

### `__init__(self, api_client=None, debug=False)`

Initializes the `WellCheck` class.

**Parameters:**

- `api_client` (SisenseClient, optional): Existing `SisenseClient` instance. If `None`, a new client may be created internally depending on the implementation.
- `debug` (bool, optional): Enable debug logging for the internal client and helpers.

* * * * *

Dashboard-level Checks
----------------------

### `check_dashboard_structure(dashboards=None)`

Analyze the structure of one or more dashboards.

This check counts key widget types and “jump-to-dashboard” (JTD) patterns commonly used in complex navigation flows.

**Parameters:**

- `dashboards` (list of str or str, optional):  
  One or more dashboard references to analyze. Each reference can be:
  - a Sisense dashboard ID, or  
  - a dashboard title (name).  

  If a single string is passed, it is normalized to a one-element list.  
  If `None`, an error is logged and an empty list is returned.

**Returns:**

- `list` of `dict`: One row per successfully processed dashboard, with keys such as:
  - `dashboard_id` (str): Resolved dashboard ID.  
  - `dashboard_title` (str): Resolved dashboard title.  
  - `pivot_count` (int): Number of pivot widgets.  
  - `tabber_count` (int): Number of tabber widgets.  
  - `jtd_count` (int): Number of jump-to-dashboard targets detected (options + script).  
  - `accordion_count` (int): Number of widgets configured as accordions.

If no dashboards are successfully processed, an empty list is returned and details are available in the logs.

* * * * *

### `check_dashboard_widget_counts(dashboards=None)`

Compute simple widget counts for one or more dashboards.

Use this when you just need “how many widgets are on each dashboard” without deeper structural analysis.

**Parameters:**

- `dashboards` (list of str or str, optional):  
  One or more dashboard references (ID or title).  
  If `None`, an error is logged and an empty list is returned.

**Returns:**

- `list` of `dict`: One row per dashboard, with keys:
  - `dashboard_id` (str): Resolved dashboard ID.  
  - `dashboard_title` (str): Resolved dashboard title.  
  - `widget_count` (int): Total number of widgets on the dashboard.

* * * * *

### `check_pivot_widget_fields(dashboards=None, max_fields=20)`

Analyze pivot widgets on one or more dashboards and report those that use “too many” fields.

This is useful for spotting overly wide/complex pivots that may impact performance or usability.

**Parameters:**

- `dashboards` (list of str or str, optional):  
  Dashboard references (ID or title). If `None`, logs an error and returns `[]`.

- `max_fields` (int, optional):  
  Threshold for the number of fields on a pivot widget.  
  Only pivot widgets with **more than** `max_fields` fields are included. Defaults to `20`.

**Returns:**

- `list` of `dict`: One row per pivot widget above the threshold, with keys:
  - `dashboard_id` (str): Dashboard ID.  
  - `dashboard_title` (str): Dashboard title.  
  - `widget_id` (str): Pivot widget ID.  
  - `has_more_fields` (bool): Always `True` for returned rows (explicit flag).  
  - `field_count` (int): Number of fields (panel items) attached to the widget.

If a dashboard has no pivot widgets, this is noted in the logs and no rows are returned for that dashboard.

* * * * *

Data Model-level Checks
-----------------------

### `check_datamodel_custom_tables(datamodels=None)`

Inspect custom tables in one or more data models and detect whether their SQL expressions contain `UNION`.

This is often used to find complex/custom logic that may need review or refactoring.

**Parameters:**

- `datamodels` (list of str or str, optional):  
  One or more data model references (ID or title).  
  If `None`, an error is logged and an empty list is returned.

**Returns:**

- `list` of `dict`: One row per custom table inspected, with keys:
  - `data_model` (str): Data model title.  
  - `table` (str): Custom table name.  
  - `has_union` (str): `"yes"` if the SQL expression contains `UNION` (case-insensitive), otherwise `"no"`.

Summary statistics (total tables, custom tables, and how many use `UNION`) are logged.

* * * * *

### `check_datamodel_island_tables(datamodels=None)`

Find “island” tables in one or more data models—tables that do **not** participate in any relationships.

These tables can cause confusion in dashboards and are often candidates for cleanup or documentation.

**Parameters:**

- `datamodels` (list of str or str, optional):  
  Data model references (ID or title). If `None`, logs an error and returns `[]`.

**Returns:**

- `list` of `dict`: One row per island table, with keys:
  - `datamodel` (str): Data model title.  
  - `datamodel_oid` (str): Data model ID.  
  - `table` (str): Table name.  
  - `table_oid` (str): Table ID.  
  - `type` (str): Table type (for example, `fact`, `dim`, `custom`).  
  - `relation` (str): `"no"` for island tables (no relationships).

The method logs, per data model, the total number of tables and how many are islands, plus overall totals.

* * * * *

### `check_datamodel_rls_datatypes(datamodels=None)`

Inspect data security (RLS) rules in one or more data models and report the datatypes of the columns used.

This is useful for identifying RLS configured on non-numeric columns (for example, string or date), which may not be recommended in your guidelines.

**Parameters:**

- `datamodels` (list of str or str, optional):  
  Data model references (ID or title). If `None`, logs an error and returns `[]`.

**Returns:**

- `list` of `dict`: One row per unique RLS column, with keys:
  - `datamodel` (str): Data model title.  
  - `table` (str): Table name.  
  - `column` (str): Column name that RLS is applied on.  
  - `datatype` (str): The RLS datatype reported by Sisense (for example, `numeric`, `string`, `datetime`).

The method logs, for each data model and overall:

- Total RLS rules.  
- Total non-numeric RLS rules.

* * * * *

### `check_datamodel_import_queries(datamodels=None)`

Check which tables in one or more data models use `importQuery` in their configuration.

Import queries can be powerful but may hide complex logic; this check surfaces all tables that use them.

**Parameters:**

- `datamodels` (list of str or str, optional):  
  Data model references (ID or title). If `None`, logs an error and returns `[]`.

**Returns:**

- `list` of `dict`: One row per table, with keys:
  - `data_model` (str): Data model title.  
  - `table` (str): Table name.  
  - `has_import_query` (str): `"yes"` if `configOptions.importQuery` is present, otherwise `"no"`.

The method logs how many tables were processed across all data models and how many had import queries.

* * * * *

### `check_datamodel_m2m_relationships(datamodels=None)`

Check for potential many-to-many (M2M) relationships between tables in one or more data models.

For each relation, it builds table/column pairs and runs aggregate SQL queries against the data source to detect duplicate keys on both sides.

**Parameters:**

- `datamodels` (list of str or str, optional):  
  Data model references (ID or title). If `None`, logs an error and returns `[]`.

**Returns:**

- `list` of `dict`: One row per relation field pair checked, with keys:
  - `data_model` (str): Data model title.  
  - `left_table` (str): Name of the left table.  
  - `left_column` (str): Name of the left column.  
  - `right_table` (str): Name of the right table.  
  - `right_column` (str): Name of the right column.  
  - `is_m2m` (bool):  
    - `True` if **both** sides have more than one occurrence of their key (detected via SQL).  
    - `False` otherwise.

The method logs:

- How many data models were processed.  
- How many relation column pairs were checked.  
- How many pairs were flagged as many-to-many.

* * * * *

Full WellCheck Orchestrator
---------------------------

### `run_full_wellcheck(dashboards=None, datamodels=None)`

Run the full suite of WellCheck analyses in one call.

This method is a convenience wrapper that orchestrates the individual checks:

- Dashboard checks:
  - `check_dashboard_structure`  
  - `check_dashboard_widget_counts`  
  - `check_pivot_widget_fields`
- Data model checks:
  - `check_datamodel_custom_tables`  
  - `check_datamodel_island_tables`  
  - `check_datamodel_rls_datatypes`  
  - `check_datamodel_import_queries`  
  - `check_datamodel_m2m_relationships`
- Unused-columns check via `AccessManagement` (if configured):
  - `get_unused_columns_bulk`

**Parameters:**

- `dashboards` (str or list of str, optional):  
  Dashboard references (IDs or titles) to include in dashboard-level checks.  
  If `None`, dashboard checks are skipped.

- `datamodels` (str or list of str, optional):  
  Data model references (IDs or titles) to include in data-model-level checks.  
  If `None`, data model checks are skipped.

**Returns:**

- `dict`: A dictionary of all results, keyed by check name. Typical structure:

  - `"dashboard_structure"`: list of rows from `check_dashboard_structure`.  
  - `"dashboard_widget_counts"`: list of rows from `check_dashboard_widget_counts`.  
  - `"pivot_widget_fields"`: list of rows from `check_pivot_widget_fields`.  
  - `"datamodel_custom_tables"`: list of rows from `check_datamodel_custom_tables`.  
  - `"datamodel_island_tables"`: list of rows from `check_datamodel_island_tables`.  
  - `"datamodel_rls_datatypes"`: list of rows from `check_datamodel_rls_datatypes`.  
  - `"datamodel_import_queries"`: list of rows from `check_datamodel_import_queries`.  
  - `"datamodel_m2m_relationships"`: list of rows from `check_datamodel_m2m_relationships`.  
  - `"unused_columns"`: list of rows from `AccessManagement.get_unused_columns_bulk`, if `access_mgmt` is configured; otherwise an empty list with a warning logged.

Each value is exactly the same structure as if the underlying method had been called directly, which makes it easy to:

- Export each section to CSV.  
- Convert each section to a DataFrame (for example, with `api_client.to_dataframe(report["datamodel_island_tables"])`).  
- Use only the pieces relevant to your analysis.
