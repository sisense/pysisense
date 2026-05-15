# Dashboard Example Usage

This guide demonstrates how to use the `Dashboard` class from the `pysisense` package. Each example below includes a description and a code snippet for easy reference and copying.

---

## Prerequisites

- Ensure `config.yaml` is in the same folder as your script.
- Import required modules and initialize the API client and `Dashboard` class.

```python
import os
import json
from pysisense import SisenseClient, Dashboard

# Set the path to your config file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")

# Initialize the API client
api_client = SisenseClient(config_file=config_path, debug=True)

# --- Initialize the Dashboard class using the shared APIClient ---
dashboard = Dashboard(api_client=api_client)
```

---

## Example 1: Get All Dashboards

Retrieve all dashboards in the system.

```python
response = dashboard.get_all_dashboards()
print(json.dumps(response, indent=4))
dashboard_df = api_client.to_dataframe(response)

# Optional: Convert the response to a DataFrame and print
print(dashboard_df)

# Optional: Export the response to a CSV file
api_client.export_to_csv(response, 'all_dashboard.csv')
```

---

## Example 2: Get Dashboard by ID

Fetch a dashboard using its ID.

```python
dashboard_id = "65d62c9wregfhg0e33bc64e8"
response = dashboard.get_dashboard_by_id(dashboard_id)
print(json.dumps(response, indent=4))
dashboard_df = api_client.to_dataframe(response)
print(dashboard_df)
api_client.export_to_csv(response, 'dashboard.csv')
```

---

## Example 3: Get dashboard widgets from export

Widget definitions come from the same admin export as `export_dashboard` (`GET /api/v1/dashboards/export` with `dashboardIds` and `adminAccess=true`): the `widgets` field on the exported dashboard object (list or object map of widget payloads).

```python
# Pass dashboard oid or title — same resolution as other dashboard helpers
widgets = dashboard.get_dashboard_widgets("Sample ECommerce")
if isinstance(widgets, list):
    print(f"Widget count: {len(widgets)}")
else:
    print(widgets.get("error"))
```

---

## Example 4: Get Dashboard by Name

Fetch a dashboard using its name.

```python
dashboard_name = "Sample ECommerce"
response = dashboard.get_dashboard_by_name(dashboard_name)
print(json.dumps(response, indent=4))
dashboard_df = api_client.to_dataframe(response)
print(dashboard_df)
```

---

## Example 5: Add a Custom Script to a Dashboard

Add a custom JavaScript script to a dashboard for UI customization.

The `script` argument may be a **raw JavaScript string** (as below) or a **JSON string** acceptable to the Sisense API. Strings that do not start with `{` are automatically wrapped as `{"script": "..."}`. Only the **dashboard owner** can save scripts; pass **`executing_user`** as the Sisense **username** of your API token user to temporarily take ownership (admin), apply the script, then restore the previous owner and shares.

```python
dashboard_id = "65d62c9574851800339cf49e"
script = """
dashboard.on('widgetready', function(d) {
    // Set background color for dashboard columns and layout
    $('.dashboard-layout-column').css('background-color', '#f0f0f0');
    $('.dashboard-layout').css('background-color', '#f0f0f0');

    // Remove horizontal dividers between layout cells
    $('.dashboard-layout-cell-horizontal-divider').remove();

    // Style individual dashboard cells
    $('.dashboard-layout-subcell-vertical')
        .css('background-color', 'white')
        .css('box-shadow', '4px 5px 12px #00000078');

    $('.dashboard-layout-subcell-host').css('padding', '10px');

    // Adjust overall dashboard padding
    $('.dashboard-layout')
        .css('padding-right', '20px')
        .css('padding-left', '20px');
});
"""
response = dashboard.add_dashboard_script(dashboard_id, script, executing_user='sisensepy@sisense.com')
print(response)
```

---

## Example 6: Add Widget Script

Add a custom script to a specific widget in a dashboard. On success, the SDK **republishes** the dashboard so changes take effect. If your API user is not the owner, pass **`executing_user`** (same pattern as dashboard-level scripts); a failed PUT with **403** often indicates an ownership issue.

```python
dashboard_id = "67dc928ae72ce30033bc6680"
widget_id = "67dc929be72ce30033bc6682"
script = """
widget.on('beforeviewloaded', function(se, ev){
    var legendAlign = 'left',
         verticalAlign= 'top',
        layout =  'horizontal',
        x = 0,
        y = 0
    /************************************/
    var legend = ev.options.legend;
    legend.align =   legendAlign
    legend.verticalAlign= verticalAlign
    legend.layout= layout
    legend.x=x
    legend.y=y
}) 
"""
response = dashboard.add_widget_script(dashboard_id, widget_id, script, executing_user='sisensepy@sisense.com')
print(response)
```

---

## Example 7: Add Dashboard Shares

Share a dashboard with users and groups, specifying permissions.

```python
dashboard_id = "6823c49365acb80033041c88"
shares = [
    {"name": "john.doe@sisense.com", "type": "user", "rule": "edit"},
    {"name": "viewer@sisense.com", "type": "user", "rule": "view"},
    {"name": "mig_test", "type": "group", "rule": "view"}
]
response = dashboard.add_dashboard_shares(dashboard_id, shares)
print(response)
```

---

## Example 8: Get Columns from a Dashboard

Retrieve all columns from a specific dashboard.

```python
dashboard_id = "pysense_databricks"
dashboard_columns = dashboard.get_dashboard_columns(dashboard_id)
print(json.dumps(dashboard_columns, indent=4))
df = api_client.to_dataframe(dashboard_columns)
print(df)
```

---

## Example 9: Get Dashboard Shares

Get sharing information for a dashboard by name.

```python
dashboard_name = "pysense_databricks"
share_info = dashboard.get_dashboard_share(dashboard_name)
print(json.dumps(share_info, indent=4))
df = api_client.to_dataframe(share_info)
print(df)
```

---

## Example 10: Resolve Dashboard Reference (ID or Name)

Resolve one or more Dashboard references that may be either IDs or names.

```python
# Mix of Dashboard IDs and titles
dashboard_refs = [
    "6893741265c9f5484dc999d7",  # Dashboard ID
    "Academy AI Content",        # Dashboard title
]

for ref in dashboard_refs:
    result = dashboard.resolve_dashboard_reference(ref)
    print(f"Input reference: {ref}")
    print(json.dumps(result, indent=4))
    print("-" * 60)

# Example of using the resolved ID/title for another call
resolved = dashboard.resolve_dashboard_reference("Academy AI Content")
if resolved.get("success"):
    dashboard_id = resolved.get("dashboard_id")
    dashboard_title = resolved.get("dashboard_title")
    print(f"Resolved Dashboard: ID={dashboard_id}, Title={dashboard_title}")
```

---

## Example 11: Get Dashboard Script

Retrieve the dashboard export, wrap the dashboard script in a **`SisenseScript`** helper, then render cleaned **jsbeautifier** output (Sisense boilerplate removed, metadata footer appended).

```python
dashboard_id = "65d62c9574851800339cf49e"
script_obj = dashboard.get_dashboard_script(dashboard_id)

if isinstance(script_obj, dict) and "error" in script_obj:
    print(script_obj["error"])
else:
    # Formatted JavaScript (boilerplate stripped + footer)
    print(script_obj.to_text())

    # Markdown title + fenced js block
    print(script_obj.to_md())

    # Write formatted script to disk
    script_obj.to_file("results/dashboard_script.js")
```

---

## Example 12: Get Widget Script and Save to File

`get_widget_script` uses the same export as Example 10; **`widget_id`** must match a key in the exported dashboard’s **`widgets`** map. The helper strips the standard Sisense widget header comment via regex, beautifies, and appends a widget-specific footer.

```python
dashboard_id = "67dc928ae72ce30033bc6680"
widget_id = "67dc929be72ce30033bc6682"
widget_script_obj = dashboard.get_widget_script(dashboard_id, widget_id)

if isinstance(widget_script_obj, dict) and "error" in widget_script_obj:
    print(widget_script_obj["error"])
else:
    print(widget_script_obj.to_text())
    widget_script_obj.to_file("results/widget_script.js")
```

---

## Notes

- Adjust parameters as needed for your environment.
- For more details, refer to the documentation in the `docs/` folder.

---
