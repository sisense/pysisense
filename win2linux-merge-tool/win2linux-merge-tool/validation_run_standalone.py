#!/usr/bin/env python3
"""
Run full validation (dashboards + widgets + optional JAQL) in a separate process.
Writes progress to a JSON file so the server can poll status without using threads.
Usage: python validation_run_standalone.py <config_json> <dashboards_json> <progress_json> [--include-jaql] [--jaql-mode=widget|item] [--show-jaql-in-results]
  --jaql-mode=widget: one JAQL request per widget with full metadata (like UI). Default.
  --jaql-mode=item:   one JAQL request per metadata item (separate JAQLs per panel item).
  --show-jaql-in-results: include the JAQL metadata in each result for display.
"""
import sys
import json
import os
import warnings

# Suppress pkg_resources deprecation warning (e.g. when run under PyInstaller bundle)
warnings.filterwarnings("ignore", message=".*pkg_resources is deprecated.*", category=UserWarning)

# Bootstrap: write to progress file immediately so server knows this process started.
# argv[0]=script, argv[1]=config, argv[2]=dashboards, argv[3]=progress [, --include-jaql] [, --jaql-mode=...]
_progress_path = sys.argv[3] if len(sys.argv) >= 4 else None
if _progress_path:
    try:
        _tmp = _progress_path + ".tmp"
        with open(_tmp, "w") as _f:
            json.dump({
                "status": "running",
                "log": ["Process started (bootstrap)."],
                "total_dashboards": 0,
                "dashboards_checked": 0,
                "widgets_checked": 0,
                "current_dashboard_name": "",
            }, _f)
            _f.flush()
            os.fsync(_f.fileno())
        # On Windows, rename fails (WinError 183) if destination exists; remove first.
        if os.path.exists(_progress_path):
            os.remove(_progress_path)
        os.rename(_tmp, _progress_path)
    except Exception as _e:
        print(f"Bootstrap write failed: {_e} path={_progress_path!r}", file=sys.stderr)
        sys.stderr.flush()

# Signal that the script started (visible in captured stderr)
print("validation_run_standalone: started", file=sys.stderr)
sys.stderr.flush()

def write_progress(path, data):
    """Write progress atomically (write to .tmp then rename) so readers never see partial content."""
    try:
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            if hasattr(f, "fileno"):
                os.fsync(f.fileno())
        # On Windows, rename fails (WinError 183) if destination exists; remove first.
        if os.path.exists(path):
            os.remove(path)
        os.rename(tmp, path)
    except Exception:
        pass


def read_control(control_path):
    """Return content of control file (e.g. 'pause', 'resume', 'stop') or ''."""
    try:
        if control_path and os.path.exists(control_path):
            with open(control_path, "r") as f:
                return (f.read() or "").strip().lower()
    except Exception:
        pass
    return ""


def check_pause_stop(control_path, progress_path, _progress_fn, log_line):
    """If control says 'stop', exit. If 'pause', wait until 'resume' or 'stop'."""
    cmd = read_control(control_path)
    if cmd == "stop":
        log_line("Validation stopped by user.")
        write_progress(progress_path, {**_progress_fn(), "status": "stopped", "log": _progress_fn().get("log", [])})
        sys.exit(0)
    if cmd == "pause":
        import time
        log_line("Validation paused.")
        while True:
            data = _progress_fn()
            data["paused"] = True
            write_progress(progress_path, data)
            time.sleep(1)
            c = read_control(control_path)
            if c == "stop":
                log_line("Validation stopped by user.")
                data = _progress_fn()
                data["status"] = "stopped"
                data["paused"] = False
                write_progress(progress_path, data)
                sys.exit(0)
            if c == "resume":
                log_line("Validation resumed.")
                break

def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    include_jaql = "--include-jaql" in sys.argv
    jaql_mode = "widget"
    show_jaql_in_results = "--show-jaql-in-results" in sys.argv
    for a in sys.argv[1:]:
        if a.startswith("--jaql-mode="):
            v = a.split("=", 1)[1].strip().lower()
            if v in ("widget", "item"):
                jaql_mode = v
            break
    if len(args) != 3:
        sys.exit(2)
    config_path, dashboards_path, progress_path = args

    def _jaql_result(status, error=None, metadata=None):
        r = {"status": status}
        if error is not None:
            r["error"] = error
        if show_jaql_in_results and metadata is not None:
            r["jaql"] = json.dumps(metadata, indent=2)
        return r

    with open(config_path, "r") as f:
        server_config = json.load(f)
    with open(dashboards_path, "r") as f:
        dashboards_list = json.load(f)

    # Prove the script is running: write progress before any imports or API calls
    write_progress(progress_path, {
        "status": "running",
        "total_dashboards": len(dashboards_list),
        "dashboards_checked": 0,
        "widgets_checked": 0,
        "current_dashboard_name": "",
        "log": ["Standalone script started.", f"Loaded {len(dashboards_list)} dashboards. Connecting..."],
    })

    log = []
    def log_line(msg):
        log.append(msg)
        write_progress(progress_path, _progress())

    dashboards_checked = 0
    widgets_checked = 0
    current_name = ""
    total_jaql = None
    jaql_checked = 0

    dashboard_results = []  # list of { dashboard_oid, dashboard_title, status, error?, widget_results: [...] }

    control_file = progress_path + ".control"

    def _progress():
        return {
            "status": "running",
            "total_dashboards": len(dashboards_list),
            "dashboards_checked": dashboards_checked,
            "widgets_checked": widgets_checked,
            "current_dashboard_name": current_name,
            "log": log.copy(),
            "total_jaql": total_jaql,
            "jaql_checked": jaql_checked,
            "dashboard_results": list(dashboard_results),
            "paused": False,
        }

    try:
        log_line("Importing API client...")
        from SisenseRESTAPIClientClass import SisenseRestApiClient, SisenseRestAPIError
        from urllib.parse import quote
        import logging
        log_line("Import done. Creating client...")
        logger = logging.getLogger(__name__)
        verify_ssl = server_config.get("verify", True)
        api_client = SisenseRestApiClient(
            server_domain=server_config["host"],
            port=server_config["port"],
            protocol=server_config["protocol"],
            operating_system=server_config["os"],
            api_token=server_config["api_token"],
            verify=verify_ssl,
            timeout=60,
            logger=logger,
        )
        log_line("Client created.")
        log_line(f"Checking {len(dashboards_list)} dashboard(s)...")
        dashboards_ok = 0
        dashboards_failed = 0
        widgets_ok = 0
        widgets_failed = 0
        jaql_ok = 0
        jaql_failed = 0
        errors = []

        for i, summary in enumerate(dashboards_list):
            check_pause_stop(control_file, progress_path, _progress, log_line)
            dashboards_checked = i + 1
            dash_id = summary.get("oid")
            dash_title = summary.get("title") or "Untitled"
            current_name = dash_title
            if not dash_id:
                continue
            log_line(f"Checking dashboard {dashboards_checked}/{len(dashboards_list)}: {dash_title}")
            write_progress(progress_path, _progress())
            if dashboards_checked == 1:
                log_line("Fetching widgets (first dashboard; timeout 60s)...")
                write_progress(progress_path, _progress())
            try:
                widgets = api_client.get_widgets(dashboard_id=dash_id)
                dashboards_ok += 1
                n_w = len(widgets) if isinstance(widgets, list) else 0
                widgets_ok += n_w
                widgets_checked += n_w

                # Per-widget status for this dashboard (oid -> { title, status, error_list, jaql_results: [{status, error?}] })
                widget_status = {}
                for w in (widgets or []):
                    oid = w.get("oid")
                    if oid is not None:
                        widget_status[str(oid)] = {"title": (w.get("title") or "Untitled"), "status": "ok", "error_list": [], "jaql_results": []}

                # Collect filter JAQL payloads from dashboard (if available)
                filter_jaql_payloads = []
                if include_jaql:
                    try:
                        dash_full = api_client.get_dashboard_by_id(dash_id)
                        for f in (dash_full.get("filters") or dash_full.get("globalFilters") or dash_full.get("filterGroups") or []):
                            item = f if isinstance(f, dict) else {}
                            if "jaql" in item:
                                ds = item.get("datasource") or dash_full.get("datasource")
                                if ds and isinstance(ds, dict) and ds.get("title"):
                                    filter_jaql_payloads.append({
                                        "datasource": ds,
                                        "metadata": [item],
                                        "dashboard": dash_id,
                                        "format": "json",
                                        "kind": "filter",
                                    })
                    except Exception:
                        pass
                filter_jaql_results = [] if include_jaql else []

                # Validate JAQL: either one request per widget (full metadata, like UI) or one per item.
                if include_jaql and widgets:
                    jaql_payloads_this = []
                    for widget in widgets:
                        if jaql_mode == "widget":
                            # One payload per widget, full metadata array (matches application UI request)
                            all_items = []
                            for panel in (widget.get("metadata", {}) or {}).get("panels", []):
                                for item in panel.get("items", []):
                                    if "jaql" in item:
                                        all_items.append(item)
                            if not all_items:
                                continue
                            jaql_payloads_this.append({
                                "datasource": widget.get("datasource"),
                                "metadata": all_items,
                                "widgetType": widget.get("type"),
                                "dashboard": dash_id,
                                "widget": str(widget.get("oid")),
                                "format": "json",
                                "offset": 0,
                                "m2mThresholdFlag": 0,
                                "isMaskedResult": True,
                                "by": "widget",
                                "count": 20000,
                                "kind": "widget",
                            })
                        else:
                            # One payload per metadata item (separate JAQLs)
                            for panel in (widget.get("metadata", {}) or {}).get("panels", []):
                                for item in panel.get("items", []):
                                    if "jaql" in item:
                                        jaql_payloads_this.append({
                                            "datasource": widget.get("datasource"),
                                            "metadata": [item],
                                            "widgetType": widget.get("type"),
                                            "dashboard": dash_id,
                                            "widget": str(widget.get("oid")),
                                            "format": "json",
                                            "kind": "widget",
                                        })
                    if jaql_payloads_this or filter_jaql_payloads:
                        log_line(f"  Validating {len(jaql_payloads_this)} widget JAQL(s) + {len(filter_jaql_payloads)} filter JAQL(s)...")
                        write_progress(progress_path, _progress())
                    for payload in jaql_payloads_this:
                        check_pause_stop(control_file, progress_path, _progress, log_line)
                        jaql_checked += 1
                        current_name = f"{dash_title} (JAQL {jaql_checked})"
                        write_progress(progress_path, _progress())
                        w_oid = payload.get("widget")
                        datasource = payload.get("datasource") or {}
                        ds_title = datasource.get("title") if isinstance(datasource, dict) else None
                        if not ds_title:
                            jaql_failed += 1
                            err_msg = "invalid datasource"
                            errors.append(f"Widget {w_oid} (dashboard {dash_id}): {err_msg}")
                            if w_oid and w_oid in widget_status:
                                widget_status[w_oid]["status"] = "jaql_failed"
                                if err_msg not in widget_status[w_oid]["error_list"]:
                                    widget_status[w_oid]["error_list"].append(err_msg)
                                widget_status[w_oid]["jaql_results"].append(_jaql_result("jaql_failed", err_msg, payload.get("metadata")))
                            continue
                        try:
                            encoded = quote(ds_title)
                            raw = api_client.elasticube_run_jaql_query(encoded, json.dumps(payload))
                            try:
                                resp = json.loads(raw) if isinstance(raw, str) else raw
                                if resp is not None and resp.get("error") is True:
                                    err_msg = resp.get("details") or resp.get("subType") or resp.get("type") or json.dumps(resp)[:200]
                                    jaql_failed += 1
                                    errors.append(f"Widget {w_oid} (dashboard {dash_id}) JAQL: {err_msg}")
                                    if w_oid and w_oid in widget_status:
                                        widget_status[w_oid]["status"] = "jaql_failed"
                                        if err_msg not in widget_status[w_oid]["error_list"]:
                                            widget_status[w_oid]["error_list"].append(err_msg)
                                        widget_status[w_oid]["jaql_results"].append(_jaql_result("jaql_failed", err_msg, payload.get("metadata")))
                                else:
                                    jaql_ok += 1
                                    if w_oid and w_oid in widget_status:
                                        widget_status[w_oid]["jaql_results"].append(_jaql_result("ok", metadata=payload.get("metadata")))
                            except (TypeError, json.JSONDecodeError):
                                jaql_ok += 1
                                if w_oid and w_oid in widget_status:
                                    widget_status[w_oid]["jaql_results"].append(_jaql_result("ok", metadata=payload.get("metadata")))
                        except Exception as e:
                            jaql_failed += 1
                            errors.append(f"Widget {w_oid} (dashboard {dash_id}) JAQL: {str(e)}")
                            if w_oid and w_oid in widget_status:
                                widget_status[w_oid]["status"] = "jaql_failed"
                                err_str = str(e)
                                if err_str not in widget_status[w_oid]["error_list"]:
                                    widget_status[w_oid]["error_list"].append(err_str)
                                widget_status[w_oid]["jaql_results"].append(_jaql_result("jaql_failed", err_str, payload.get("metadata")))
                    if jaql_payloads_this:
                        log_line(f"  Dashboard widget JAQL: {jaql_ok + jaql_failed} checked so far")
                        write_progress(progress_path, _progress())

                # Validate filter JAQLs for this dashboard (when include_jaql, even if no widgets)
                if include_jaql and filter_jaql_payloads:
                    for payload in filter_jaql_payloads:
                        check_pause_stop(control_file, progress_path, _progress, log_line)
                        jaql_checked += 1
                        current_name = f"{dash_title} (filter JAQL {jaql_checked})"
                        write_progress(progress_path, _progress())
                        datasource = payload.get("datasource") or {}
                        ds_title = datasource.get("title") if isinstance(datasource, dict) else None
                        if not ds_title:
                            jaql_failed += 1
                            filter_jaql_results.append(_jaql_result("jaql_failed", "invalid datasource", payload.get("metadata")))
                            continue
                        try:
                            encoded = quote(ds_title)
                            raw = api_client.elasticube_run_jaql_query(encoded, json.dumps(payload))
                            try:
                                resp = json.loads(raw) if isinstance(raw, str) else raw
                                if resp is not None and resp.get("error") is True:
                                    err_msg = resp.get("details") or resp.get("subType") or resp.get("type") or json.dumps(resp)[:200]
                                    jaql_failed += 1
                                    filter_jaql_results.append(_jaql_result("jaql_failed", err_msg, payload.get("metadata")))
                                else:
                                    jaql_ok += 1
                                    filter_jaql_results.append(_jaql_result("ok", metadata=payload.get("metadata")))
                            except (TypeError, json.JSONDecodeError):
                                jaql_ok += 1
                                filter_jaql_results.append(_jaql_result("ok", metadata=payload.get("metadata")))
                        except Exception as e:
                            jaql_failed += 1
                            filter_jaql_results.append(_jaql_result("jaql_failed", str(e), payload.get("metadata")))

                # Append this dashboard's results and write progress so UI can show it
                widget_results = [
                    {
                        "widget_oid": oid,
                        "widget_title": info["title"],
                        "status": info["status"],
                        "error": "; ".join(info["error_list"]) if info.get("error_list") else None,
                        "jaql_results": info.get("jaql_results") or [],
                    }
                    for oid, info in widget_status.items()
                ]
                dashboard_results.append({
                    "dashboard_oid": dash_id,
                    "dashboard_title": dash_title,
                    "status": "ok",
                    "widget_results": widget_results,
                    "filter_jaql_results": filter_jaql_results if include_jaql else [],
                })
                write_progress(progress_path, _progress())

            except SisenseRestAPIError as e:
                dashboards_failed += 1
                err_msg = getattr(e.result, "text", str(e))[:200] if hasattr(e, "result") else str(e)
                errors.append(f'Dashboard "{dash_title}" ({dash_id}): {err_msg}')
                log_line(f"  Failed: {err_msg[:100]}")
                dashboard_results.append({
                    "dashboard_oid": dash_id,
                    "dashboard_title": dash_title,
                    "status": "failed",
                    "error": err_msg,
                    "widget_results": [],
                    "filter_jaql_results": [],
                })
                write_progress(progress_path, _progress())
            except Exception as e:
                dashboards_failed += 1
                errors.append(f'Dashboard "{dash_title}" ({dash_id}): {str(e)}')
                log_line(f"  Failed: {str(e)[:100]}")
                dashboard_results.append({
                    "dashboard_oid": dash_id,
                    "dashboard_title": dash_title,
                    "status": "failed",
                    "error": str(e),
                    "widget_results": [],
                    "filter_jaql_results": [],
                })
                write_progress(progress_path, _progress())

        if include_jaql:
            total_jaql = jaql_checked
        else:
            jaql_ok = None
            jaql_failed = None

        current_name = ""
        log_line("Validation complete.")
        write_progress(progress_path, {
            "status": "completed",
            "total_dashboards": len(dashboards_list),
            "dashboards_checked": len(dashboards_list),
            "widgets_checked": widgets_checked,
            "current_dashboard_name": current_name,
            "log": log,
            "dashboardsOk": dashboards_ok,
            "dashboardsFailed": dashboards_failed,
            "widgetsOk": widgets_ok,
            "widgetsFailed": widgets_failed,
            "errors": errors[:100],
            "jaqlOk": jaql_ok,
            "jaqlFailed": jaql_failed,
            "dashboard_results": list(dashboard_results),
        })
    except Exception as e:
        write_progress(progress_path, {
            "status": "failed",
            "error": str(e),
            "log": log,
            "total_dashboards": len(dashboards_list),
            "dashboards_checked": dashboards_checked,
            "widgets_checked": widgets_checked,
            "current_dashboard_name": current_name,
        })
        raise
    sys.exit(0)

if __name__ == "__main__":
    main()
