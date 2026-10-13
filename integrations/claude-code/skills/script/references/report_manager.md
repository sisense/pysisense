# Report Manager reference

`from pysisense import ReportManager` — `report_manager = ReportManager(api_client=api_client)`

Report Manager is a **Marketplace/on-demand plugin**, not guaranteed to be installed or enabled on every instance. Every method below returns `{"ok": False, "error": "..."}` instead of raising when the plugin's endpoints are unavailable — always check for `"error"` before using the result. If it's not enabled on your instance, the built-in dashboard **subscriptions** feature is the closest fallback (`pysisense` does not wrap subscriptions).

## Methods

| Method | Use when |
|---|---|
| `get_reports(...)` | List/filter reports — paginates internally, returns one flat list. |
| `get_report(report_id, ...)` | Single report by id, optionally enriched with owner/recipient/dashboard info. |
| `create_report(report)` | Create a new scheduled report. |
| `update_report(report_id, report)` | Partial update (PATCH semantics) — only provided fields change. |
| `delete_report(report_id)` | Delete by id. |
| `run_report(report_id)` | Trigger an immediate, on-demand run. |

## Listing and fetching

```python
response = report_manager.get_reports(name="Weekly Sales", enabled=True)
df = api_client.to_dataframe(response)
```

`get_reports` filter kwargs: `name`, `ids` (list or bare str), `enabled` (bool), `statuses` (list or bare str), `priority` (`"high"`/`"normal"`), `owner_ids` (list or bare str), `fields` (whitelist string, prefix with `-` to exclude), `sort` (prefix with `-` for descending), `limit` (page size, default `100`). Returns a flat `list[dict]`, or `{"ok": False, "error": "..."}`.

```python
response = report_manager.get_report(
    "5A929ac648c9EcebAf0DE08e",
    owner_info=True,
    recipients_info=True,
    dashboards_info=True,
)
```

`owner_info` / `recipients_info` / `dashboards_info` are opt-in booleans (all default `False`) that enrich the single-report response.

## create_report / update_report — payload shape

Both validate a dict against a Pydantic model **before** any HTTP call and return `{"ok": False, "error": "Invalid report payload: ..."}` immediately on a Pydantic `ValidationError` — no request is sent. Both models use `extra="forbid"`, so an unknown/misspelled field key fails validation rather than being silently dropped.

`CreateReportPayload` fields (canonical Sisense payload names via aliases):

| Field (alias) | Required | Notes |
|---|---|---|
| `name` | yes | |
| `reportType` (`ReportTypeFlags`) | no | Defaults to `{"PDF": False, "CSV": False, "URL": False}` when omitted. **If you provide it, all three of `PDF`/`CSV`/`URL` must be present as booleans** — Report Manager's own schema validation requires all three even when only one format is enabled. Extra keys (e.g. `XLS`, a nested `pdf` settings object) are allowed. |
| `runOnFinish` (`RunOnFinishSettings`) | no | Defaults to `{"fileShare": {"overwriteExisting": False}}` when omitted. **If provided, `fileShare.overwriteExisting` is required**, even with no file-share destination configured; `url` and `SFTPServerName` are optional. |
| `events` | no | Defaults to `[]` when omitted. |
| `enabled`, `schedule`, `dashboards`, `recipients`, `templateId`, `priority`, `errorEmails` | no | Passed through as-is (`schedule`/`dashboards`/`recipients` are untyped dicts/lists). |

`UpdateReportPayload` accepts the same fields, all optional, with the same `reportType`/`runOnFinish` all-or-nothing sub-requirements when those keys are provided at all.

```python
new_report = {
    "name": "Weekly Sales Summary",
    "enabled": True,
    "priority": "normal",
    "schedule": {
        "timezone": "UTC",
        "time": {"frequency": "weekly", "days": {"mon": True}, "at": "08:00"},
    },
    "dashboards": [{"dashboardOid": "f0aB356CeF6e8Cbc2E2b9add"}],
    "recipients": [{"email": "team@example.com", "type": "user"}],
    "reportType": {"PDF": True, "CSV": False, "URL": False},  # all 3 keys required if present
    "runOnFinish": {"fileShare": {"overwriteExisting": False}},  # required if present
}
response = report_manager.create_report(new_report)
```

```python
# Only fields provided are sent — omitted fields are left unmodified
response = report_manager.update_report(
    "5A929ac648c9EcebAf0DE08e",
    {"enabled": False, "priority": "high"},
)
```

`update_report` with an empty payload (nothing to update after `exclude_unset`/`exclude_none`) short-circuits to `{"success": True, "changed": False}` without calling the API.

## delete_report / run_report

```python
report_manager.delete_report("5A929ac648c9EcebAf0DE08e")  # -> {"success": True} or {"ok": False, "error": "..."}
report_manager.run_report("5A929ac648c9EcebAf0DE08e")  # -> {"success": True} (or response body) or {"ok": False, "error": "..."}
```

`run_report` triggers an immediate on-demand run. If the max number of concurrently running reports has been reached, the server **queues** the report instead of running it right away — this is not surfaced as an error, the call still returns success.

## Plugin-unavailable / error-shape gotchas

`create_report`/`update_report`/`delete_report`/`run_report`/`get_report`/`get_reports` all funnel failures through the same internal error builder, which adds context to two specific status codes:

- **504**: message is annotated as a timeout — "the Report Manager service may be down or otherwise unavailable."
- **404 with an empty body**: annotated as "the Report Manager plugin is not installed or enabled on this Sisense instance." (A genuine "report not found" 404 from Report Manager itself always carries a JSON error body, so it does *not* get this annotation — that distinguishes "plugin missing" from "id wrong.")

```python
response = report_manager.get_reports()
if "error" in response:
    print(f"Report Manager unavailable: {response['error']}")
```
