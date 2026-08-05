# Report Manager Module Documentation

The `ReportManager` class manages scheduled reports through the Sisense Report Manager plugin.

Report Manager is an **on-demand plugin** and is not guaranteed to be installed or enabled on every Sisense instance. Every method returns `{"error": "..."}` rather than raising when the plugin's endpoints are unavailable (for example, a 404 because the plugin is not enabled), so callers can detect and handle that case directly.

---

## Class: `ReportManager`

### `__init__(self, api_client=None, debug=False)`

Initializes the ReportManager class.

**Parameters:**

- `api_client` (SisenseClient, optional): An existing client. A new one is created if not provided.
- `debug` (bool, optional): Enable debug-level logging. Default `False`.

---

## Reports (`report_manager/core.py`)

### `get_reports(name=None, ids=None, enabled=None, statuses=None, priority=None, owner_ids=None, fields=None, sort=None, limit=100)`

Retrieves all reports configured in Report Manager. Sends paginated `GET /api/v1/report_manager/reports` requests and collects every page into a single flat list; filters are applied server-side.

**Parameters:**

- `name` (str, optional): Search reports by name.
- `ids` (list[str] | str, optional): One or more report ids to filter by.
- `enabled` (bool, optional): Filter to only enabled (`True`) or disabled (`False`) reports.
- `statuses` (list[str] | str, optional): One or more running statuses to filter by.
- `priority` (str, optional): `"high"` or `"normal"`.
- `owner_ids` (list[str] | str, optional): One or more owner user ids to filter by.
- `fields` (str, optional): Whitelist of fields to return. Prefix a field with `-` to exclude it.
- `sort` (str, optional): Field to sort by. Ascending by default, descending if prefixed with `-`.
- `limit` (int, optional): Page size for the underlying paginated requests. Default `100`.

**Returns:**

- `list | dict`: A flat list of report objects on success, or `{"error": "..."}` on failure.

---

### `get_report(report_id, owner_info=False, recipients_info=False, dashboards_info=False, fields=None)`

Retrieves a single report by id via `GET /api/v1/report_manager/reports/{id}`.

**Parameters:**

- `report_id` (str): The report id.
- `owner_info` (bool, optional): Include the report owner's information. Default `False`.
- `recipients_info` (bool, optional): Include recipient data. Default `False`.
- `dashboards_info` (bool, optional): Include dashboard names. Default `False`.
- `fields` (str, optional): Whitelist of fields to return.

**Returns:**

- `dict`: The report object on success, or `{"error": "..."}` on failure or when the report is not found.

---

### `create_report(report)`

Creates a new report via `POST /api/v1/report_manager/reports`.

**Parameters:**

- `report` (dict): The report definition. Supported fields (canonical Sisense payload names):

  | Field | Type | Notes |
  |---|---|---|
  | `name` | str | Required |
  | `reportType` | dict | Defaults to `{"PDF": False, "CSV": False, "URL": False}` when omitted. If provided, must include the `PDF`, `CSV`, and `URL` boolean flags — Report Manager's schema validation rejects the payload if any of the three is missing, even when only one format is enabled |
  | `runOnFinish` | dict | Defaults to `{"fileShare": {"overwriteExisting": False}}` when omitted. `fileShare.overwriteExisting` is required whenever `runOnFinish` is provided, even with no file-share destination configured; `url`, `SFTPServerName` are optional |
  | `events` | list[str] | Defaults to `[]` when omitted |
  | `enabled` | bool | |
  | `schedule` | dict | `cron`, `timezone`, `time` (frequency/day/time-of-day settings) |
  | `dashboards` | list[dict] | Each entry: `dashboardOid`, `filters`, `customFiltersEnabled` |
  | `recipients` | list[dict] | Each entry: `_id`, `email`, `firstName`, `lastName`, `name`, `type` |
  | `templateId` | str | |
  | `priority` | str | `"high"` or `"normal"` |
  | `errorEmails` | list[str] | |

**Returns:**

- `dict`: The API response body on success, or `{"error": "..."}` on failure (including payload validation errors).

---

### `update_report(report_id, report)`

Updates an existing report via `PATCH /api/v1/report_manager/reports/{id}`. Updates must be provided inside the `report` payload; only the fields provided are sent to the API, so omitted fields are left unmodified. Supports the same fields as `create_report`, all optional — except that if `reportType` is provided, it must still include all three of `PDF`, `CSV`, and `URL`, and if `runOnFinish` is provided, it must still include `fileShare.overwriteExisting`.

**Parameters:**

- `report_id` (str): The report id to update.
- `report` (dict): The fields to update.

**Returns:**

- `dict`: The API response body on success (or `{"success": True, "changed": False}` when no fields were provided), or `{"error": "..."}` on failure.

---

### `delete_report(report_id)`

Deletes a report by id via `DELETE /api/v1/report_manager/reports/{id}`.

**Parameters:**

- `report_id` (str): The report id to delete.

**Returns:**

- `dict`: `{"success": True}` on success, or `{"error": "..."}` on failure.

---

### `run_report(report_id)`

Triggers an immediate run of a report via `POST /api/v1/report_manager/reports/{id}/run`. If the maximum number of concurrently running reports has been reached, the server queues the report instead of running it immediately.

**Parameters:**

- `report_id` (str): The report id to run.

**Returns:**

- `dict`: `{"success": True}` on success, or `{"error": "..."}` on failure.
