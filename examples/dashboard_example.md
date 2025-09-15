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

## Example 3: Get Dashboard by Name

Fetch a dashboard using its name.

```python
dashboard_name = "Sample ECommerce"
response = dashboard.get_dashboard_by_name(dashboard_name)
print(json.dumps(response, indent=4))
dashboard_df = api_client.to_dataframe(response)
print(dashboard_df)
```

---

## Example 4: Add a Custom Script to a Dashboard

Add a custom JavaScript script to a dashboard for UI customization.

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

## Example 5: Add Widget Script

Add a custom script to a specific widget in a dashboard.

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

## Example 6: Add Dashboard Shares

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

## Example 7: Get Columns from a Dashboard

Retrieve all columns from a specific dashboard.

```python
dashboard_id = "pysense_databricks"
dashboard_columns = dashboard.get_dashboard_columns(dashboard_id)
print(json.dumps(dashboard_columns, indent=4))
df = api_client.to_dataframe(dashboard_columns)
print(df)
```

---

## Example 8: Get Dashboard Shares

Get sharing information for a dashboard by name.

```python
dashboard_name = "pysense_databricks"
share_info = dashboard.get_dashboard_share(dashboard_name)
print(json.dumps(share_info, indent=4))
df = api_client.to_dataframe(share_info)
print(df)
```

---

## Notes

- Adjust parameters as needed for your environment.
- For more details, refer to the documentation in the `docs/` folder.

---
