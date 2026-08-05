# Report Manager Example Usage

This guide demonstrates how to use the `ReportManager` class from the `pysisense` package to list, create, update, delete, and run scheduled reports. Each example includes a short description and a code snippet you can copy into your own scripts.

Report Manager is a **Marketplace feature**, if it is not installed or enabled on your Sisense instance, every method below returns `{"error": "..."}` instead of raising, so check for `"error"` in the response before using the result. In that case the underlying HTTP error is typically a 404 with an empty body (the `/report_manager` route isn't recognized at all), as opposed to a genuine "not found" 404 which carries Report Manager's own structured error JSON.

If Report Manager isn't available on your instance, the built-in dashboard **subscriptions** feature is the closest fallback for scheduled/emailed reports, `pysisense` does not yet support subscriptions, so that would need to be managed manually. See:

- [Report Manager (Sisense docs)](https://docs.sisense.com/main/SisenseLinux/report-manager.htm)
- [Configuring email settings / subscriptions (Sisense docs)](https://docs.sisense.com/main/SisenseLinux/configuring-email-settings.htm)

---

## Prerequisites

- Import required modules and initialize the API client and `ReportManager` class.

```python
import os
import json
from pysisense import SisenseClient, ReportManager

# Option A — YAML config file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
api_client = SisenseClient(config_file=config_path, debug=True)

# Option B — inline credentials (no config.yaml required)
api_client = SisenseClient(
    domain="https://your-domain.sisense.com",
    token="YOUR_API_TOKEN",
)

# Initialize the ReportManager class using the shared client
report_manager = ReportManager(api_client=api_client)
```

---

## Example 1: List All Reports

Retrieve every report configured in Report Manager, optionally filtered.

```python
response = report_manager.get_reports()
print(json.dumps(response, indent=4))

# Filter by name and only enabled reports
response = report_manager.get_reports(name="Weekly Sales", enabled=True)

# Convert to DataFrame
df = api_client.to_dataframe(response)
print(df)
```

---

## Example 2: Get a Single Report

Look up one report by id, optionally enriching the response with owner, recipient, or dashboard info.

```python
response = report_manager.get_report(
    "5A929ac648c9EcebAf0DE08e",
    owner_info=True,
    recipients_info=True,
)
print(json.dumps(response, indent=4))
```

---

## Example 3: Create a Report

Create a new scheduled report.

```python
new_report = {
    "name": "Weekly Sales Summary",
    "enabled": True,
    "priority": "normal",
    "schedule": {
        "timezone": "UTC",
        "time": {
            "frequency": "weekly",
            "days": {"mon": True},
            "at": "08:00",
        },
    },
    "dashboards": [
        {"dashboardOid": "f0aB356CeF6e8Cbc2E2b9add"},
    ],
    "recipients": [
        {"email": "team@example.com", "type": "user"},
    ],
    # PDF, CSV, and URL must all be present, even though only PDF is enabled here.
    "reportType": {"PDF": True, "CSV": False, "URL": False},
    # overwriteExisting is required even with no file-share destination configured.
    "runOnFinish": {"fileShare": {"overwriteExisting": False}},
}

response = report_manager.create_report(new_report)
print(json.dumps(response, indent=4))
```

---

## Example 4: Update a Report

Only the fields provided are sent to the API — omitted fields are left unmodified.

```python
response = report_manager.update_report(
    "5A929ac648c9EcebAf0DE08e",
    {"enabled": False, "priority": "high"},
)
print(json.dumps(response, indent=4))
```

---

## Example 5: Delete a Report

```python
response = report_manager.delete_report("5A929ac648c9EcebAf0DE08e")
print(response)
# {"success": True}
```

---

## Example 6: Run a Report Now

Trigger an immediate, on-demand run of a report. If the maximum number of concurrently running reports has been reached, the server queues it instead.

```python
response = report_manager.run_report("5A929ac648c9EcebAf0DE08e")
print(response)
# {"success": True}
```

---

## Example 7: Handling a Disabled Plugin

Since Report Manager is an on-demand plugin, always check for `"error"` before using a result.

```python
response = report_manager.get_reports()
if "error" in response:
    print(f"Report Manager is unavailable: {response['error']}")
else:
    print(f"Found {len(response)} report(s).")
```
