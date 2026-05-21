"""
Migration MCP server: stdio MCP server exposing tools that call the migration Flask API over HTTP.
All tools use MIGRATION_SERVER_URL (default http://localhost:5001). The Flask app must be running.
"""
import os
import json
import requests
from typing import Optional
from mcp.server.fastmcp import FastMCP

BASE_URL = os.environ.get("MIGRATION_SERVER_URL", "http://localhost:5001").rstrip("/")
TIMEOUT = 120
# Compare fetches all dashboards from both servers (paginated); can take several minutes with 400+ dashboards.
COMPARE_DASHBOARDS_TIMEOUT = 900
# Dashboard count and full-list operations also fetch all dashboards.
LONG_TIMEOUT = 300


def _get(url: str, params: dict = None, timeout: int = TIMEOUT) -> dict:
    r = requests.get(f"{BASE_URL}{url}", params=params, timeout=timeout)
    r.raise_for_status()
    return r.json() if r.content else {}


def _post(url: str, json_data: dict = None) -> dict:
    r = requests.post(f"{BASE_URL}{url}", json=json_data or {}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() if r.content else {}


def _patch(url: str, json_data: dict) -> dict:
    r = requests.patch(f"{BASE_URL}{url}", json=json_data, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json() if r.content else {}


mcp = FastMCP(
    name="Migration MCP",
    instructions="Tools to configure and run Sisense migrations; list users, dashboards, datamodels, folders, groups, and roles on source and target; compare users; and read migration logs. All operations call the migration Flask server.",
)


@mcp.tool()
def get_settings() -> str:
    """Get the current migration settings (full YAML-backed config). Use to discover options and current values."""
    try:
        data = _get("/api/settings")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e), "hint": "Ensure the migration Flask server is running at MIGRATION_SERVER_URL."})


@mcp.tool()
def get_settings_preview(partial_settings: str) -> str:
    """Preview merged settings without saving. Pass a JSON object with the partial settings to merge (e.g. {\"migrate_users\": true}). Use when the user asks for a plan or what would you set."""
    try:
        data = json.loads(partial_settings) if isinstance(partial_settings, str) else partial_settings
        result = _post("/api/settings/preview", data)
        return json.dumps(result, indent=2)
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {e}"})
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def save_settings(settings_json: str) -> str:
    """Save the full settings (replace entire config). Pass the complete settings as JSON. Use sparingly; prefer update_settings for partial updates."""
    try:
        data = json.loads(settings_json) if isinstance(settings_json, str) else settings_json
        _post("/api/settings", data)
        return json.dumps({"message": "Settings saved successfully."})
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def update_settings(partial_settings: str) -> str:
    """Update settings with a partial JSON object (deep merge). E.g. {\"migrate_users\": true, \"migrate_dashboards\": true}. Use to configure migration options before running."""
    try:
        data = json.loads(partial_settings) if isinstance(partial_settings, str) else partial_settings
        _patch("/api/settings", data)
        return json.dumps({"message": "Settings updated successfully."})
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {e}"})
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def set_migrate_all_dashboards(import_mode: Optional[str] = None) -> str:
    """Set configuration to migrate ALL dashboards on the source (no owner or list filter). Call this when the user says 'migrate all dashboards', 'migrate every dashboard', or 'migrate everything' for dashboards without naming an owner or a specific list. Then call run_migration(). Do NOT use this for 'migrate all dashboards owned by X'—use prepare_dashboard_migration(owner_email=X) for that. Optional import_mode: 'overwrite' or 'skip' (default skip)."""
    try:
        patch = {
            "migrate_dashboards": True,
            "dashboard_migration_mode": "ALL",
            "dashboard_include_list": "",
        }
        if import_mode and import_mode.strip().lower() in ("overwrite", "skip"):
            patch["dashboard_import_mode"] = import_mode.strip().lower()
        _patch("/api/settings", patch)
        out = {
            "message": "Settings updated to migrate all dashboards. You can now call run_migration().",
            "migrate_dashboards": True,
            "dashboard_migration_mode": "ALL",
            "dashboard_include_list": "",
        }
        if "dashboard_import_mode" in patch:
            out["dashboard_import_mode"] = patch["dashboard_import_mode"]
        return json.dumps(out, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_servers() -> str:
    """Get the list of configured servers (source/target definitions)."""
    try:
        data = _get("/api/servers")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def add_or_update_server(server_name: str, host: str, port: int, protocol: str, os_name: str, api_token: str, verify: bool = False) -> str:
    """Add or update a single server in the servers config. Use for 'add a new target server' or similar. os_name is the Sisense OS type (e.g. linux)."""
    try:
        servers = _get("/api/servers") or {}
        servers[server_name] = {
            "host": host,
            "port": int(port),
            "protocol": protocol,
            "os": os_name,
            "api_token": api_token,
            "verify": bool(verify),
        }
        _post("/api/servers", servers)
        return json.dumps({"message": f"Server '{server_name}' saved successfully."})
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_current_source_server() -> str:
    """Get the currently selected source server info (name, host, port, etc.) from settings and servers."""
    try:
        settings = _get("/api/settings")
        servers = _get("/api/servers")
        name = settings.get("selected_source_server")
        if not name:
            return json.dumps({"error": "No source server selected."})
        if name not in (servers or {}):
            return json.dumps({"error": f"Source server '{name}' not found in servers."})
        out = {"name": name, **servers[name]}
        if "api_token" in out:
            out["api_token"] = "(redacted)"
        return json.dumps(out, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_current_target_server() -> str:
    """Get the currently selected target server info (name, host, port, etc.) from settings and servers."""
    try:
        settings = _get("/api/settings")
        servers = _get("/api/servers")
        name = settings.get("selected_target_server")
        if not name:
            return json.dumps({"error": "No target server selected."})
        if name not in (servers or {}):
            return json.dumps({"error": f"Target server '{name}' not found in servers."})
        out = {"name": name, **servers[name]}
        if "api_token" in out:
            out["api_token"] = "(redacted)"
        return json.dumps(out, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_source_users() -> str:
    """List users on the currently selected source server."""
    try:
        data = _get("/api/source/users")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_target_users() -> str:
    """List users on the currently selected target server."""
    try:
        data = _get("/api/target/users")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def users_missing_on_target() -> str:
    """List users that exist on the source server but not on the target (by email). Useful for planning user migration."""
    try:
        data = _get("/api/compare/users-missing-on-target")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def roles_missing_on_target() -> str:
    """List roles that exist on the source server but not on the target (by role name). Use when the user asks which roles are missing on the target."""
    try:
        data = _get("/api/compare/roles-missing-on-target")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


LIST_DASHBOARDS_DEFAULT_LIMIT = 200


@mcp.tool()
def list_source_dashboards(
    owner_id: Optional[str] = None,
    owner_email: Optional[str] = None,
    title: Optional[str] = None,
    oids: Optional[str] = None,
    limit: int = LIST_DASHBOARDS_DEFAULT_LIMIT,
    full_list: bool = False,
) -> str:
    """List dashboards on the source server. Optionally filter by owner_id, owner_email, title (search), or oids (comma-separated). Default returns up to 200; set full_list=True when the user wants the complete list (no cap). Response may be large for full_list."""
    try:
        params = {"limit": 0 if full_list else limit}
        if owner_id and owner_id.strip():
            params["owner_id"] = owner_id.strip()
        if owner_email and owner_email.strip():
            params["owner_email"] = owner_email.strip()
        if title and title.strip():
            params["title"] = title.strip()
        if oids and oids.strip():
            params["oids"] = oids.strip()
        data = _get("/api/source/dashboards", params)
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_target_dashboards(
    owner_id: Optional[str] = None,
    owner_email: Optional[str] = None,
    title: Optional[str] = None,
    oids: Optional[str] = None,
    limit: int = LIST_DASHBOARDS_DEFAULT_LIMIT,
    full_list: bool = False,
) -> str:
    """List dashboards on the target server. Optionally filter by owner_id, owner_email, title (search), or oids (comma-separated). Default returns up to 200; set full_list=True when the user wants the complete list (no cap). Response may be large for full_list."""
    try:
        params = {"limit": 0 if full_list else limit}
        if owner_id and owner_id.strip():
            params["owner_id"] = owner_id.strip()
        if owner_email and owner_email.strip():
            params["owner_email"] = owner_email.strip()
        if title and title.strip():
            params["title"] = title.strip()
        if oids and oids.strip():
            params["oids"] = oids.strip()
        data = _get("/api/target/dashboards", params)
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_source_dashboard_count() -> str:
    """Get the total number of dashboards on the source server. Use when the user asks how many dashboards are on the source (not the truncated list count)."""
    try:
        data = _get("/api/source/dashboards/count", timeout=LONG_TIMEOUT)
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_target_dashboard_count() -> str:
    """Get the total number of dashboards on the target server. Use when the user asks how many dashboards are on the target (not the truncated list count)."""
    try:
        data = _get("/api/target/dashboards/count", timeout=LONG_TIMEOUT)
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def compare_dashboards() -> str:
    """Compare all dashboards on source vs target; returns only_on_source, only_on_target, and on_both (by OID/title). Use when the user asks which dashboards from source are missing on target. May take 5–10 minutes with hundreds of dashboards; the agent SDK is configured to allow long-running tool calls. If this times out, the user can ask again and you can call get_compare_dashboards_result() to retrieve the result."""
    try:
        r = requests.post(f"{BASE_URL}/api/compare/dashboards/start", timeout=30)
        if r.status_code == 409:
            pass  # Already running; poll for result.
        elif r.status_code != 202:
            return json.dumps(r.json() if r.content else {"error": r.reason}, indent=2)
        # Poll for result (agent has client_session_timeout_seconds=900 so this can run ~15 min).
        poll_wait = 30
        max_polls = 20
        for _ in range(max_polls):
            data = _get("/api/compare/dashboards/result", {"wait": poll_wait}, timeout=poll_wait + 15)
            status = data.get("status")
            if status == "done":
                if data.get("error"):
                    return json.dumps({"error": data["error"]})
                return json.dumps(data, indent=2)
            if status == "idle":
                return json.dumps({"error": "Compare did not start."})
        return json.dumps({"error": "Compare timed out after polling."})
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_compare_dashboards_result() -> str:
    """Get the result of the dashboard compare (after compare_dashboards() was called and the user waited 2–3 minutes). Returns only_on_source, only_on_target, on_both. Call this when the user asks again for the compare result or which dashboards are missing. If status is still 'running', tell the user to ask again in a minute."""
    try:
        data = _get("/api/compare/dashboards/result", timeout=15)
        status = data.get("status")
        if status == "done":
            if data.get("error"):
                return json.dumps({"error": data["error"]})
            return json.dumps(data, indent=2)
        if status == "running":
            return json.dumps({
                "status": "running",
                "message": "Comparison still in progress. Ask the user to wait another minute and ask again, then call get_compare_dashboards_result() again.",
            }, indent=2)
        if status == "idle":
            return json.dumps({
                "status": "idle",
                "message": "No compare has been run. Call compare_dashboards() first, then have the user ask again in 2–3 minutes.",
            }, indent=2)
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def prepare_dashboard_migration(
    owner_email: Optional[str] = None,
    titles: Optional[str] = None,
    oids: Optional[str] = None,
    import_mode: Optional[str] = None,
) -> str:
    """Prepare dashboard migration by owner, by title list, or by OID list. Updates dashboard_migration_mode and dashboard_include_list (and migrate_dashboards). Use when the user asks to migrate dashboards owned by X or a specific list of dashboards. Then call run_migration(). If more than one of owner_email/titles/oids is provided: oids wins, then titles, then owner_email. When the user says 'these dashboards' or 'the list above', use the dashboard titles or OIDs from the list you just showed and pass them as titles= or oids= (comma-separated). Set import_mode to 'overwrite' when the user wants to overwrite existing dashboards on target, or 'skip' to skip existing (default is skip)."""
    try:
        include_list = ""
        mode = "By OID"
        if oids and oids.strip():
            include_list = ",".join(x.strip() for x in oids.split(",") if x.strip())
            mode = "By OID"
        elif titles and titles.strip():
            include_list = ",".join(x.strip() for x in titles.split(",") if x.strip())
            mode = "By Name"
        elif owner_email and owner_email.strip():
            data = _get("/api/source/dashboards", {"owner_email": owner_email.strip(), "limit": 1000})
            dashboards = data.get("dashboards") if isinstance(data, dict) else []
            if not dashboards:
                return json.dumps({"message": "No dashboards found for that owner.", "owner_email": owner_email})
            oid_list = []
            for d in dashboards:
                oid = d.get("oid") or d.get("_id") or d.get("id")
                if oid:
                    oid_list.append(oid)
            include_list = ",".join(oid_list)
            mode = "By OID"
        else:
            return json.dumps({"error": "Provide one of owner_email, titles, or oids."})
        if not include_list:
            return json.dumps({"error": "No dashboards to include (empty list)."})
        patch = {
            "dashboard_migration_mode": mode,
            "dashboard_include_list": include_list,
            "migrate_dashboards": True,
        }
        if import_mode and import_mode.strip().lower() in ("overwrite", "skip"):
            patch["dashboard_import_mode"] = import_mode.strip().lower()
        _patch("/api/settings", patch)
        out = {
            "message": "Settings updated; you can now run run_migration().",
            "dashboard_migration_mode": mode,
            "dashboard_include_list": include_list,
            "migrate_dashboards": True,
        }
        if "dashboard_import_mode" in patch:
            out["dashboard_import_mode"] = patch["dashboard_import_mode"]
        return json.dumps(out, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_source_datamodels() -> str:
    """List datamodels (elasticubes) on the currently selected source server. Use when the user asks about datamodels on the source."""
    try:
        data = _get("/api/source/datamodels")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_target_datamodels() -> str:
    """List datamodels (elasticubes) on the currently selected target server. Use when the user asks about datamodels on the target."""
    try:
        data = _get("/api/target/datamodels")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_source_folders() -> str:
    """List folders on the currently selected source server. Use when the user asks about folders on the source."""
    try:
        data = _get("/api/source/folders")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_target_folders() -> str:
    """List folders on the currently selected target server. Use when the user asks about folders on the target."""
    try:
        data = _get("/api/target/folders")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_source_groups() -> str:
    """List groups on the currently selected source server. Use when the user asks about groups on the source."""
    try:
        data = _get("/api/source/groups")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_target_groups() -> str:
    """List groups on the currently selected target server. Use when the user asks about groups on the target."""
    try:
        data = _get("/api/target/groups")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_source_roles() -> str:
    """List roles on the currently selected source server. Use when the user asks about roles on the source."""
    try:
        data = _get("/api/source/roles")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_target_roles() -> str:
    """List roles on the currently selected target server. Use when the user asks about roles on the target."""
    try:
        data = _get("/api/target/roles")
        return json.dumps(data, indent=2)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def run_migration() -> str:
    """Start the migration in the background. It runs whatever is currently enabled in settings (migrate_users, migrate_dashboards, migrate_folders, migrate_groups, migrate_datamodels, etc.). If the user asked for only one type (e.g. 'run user migration' or 'only migrate users'), you MUST call update_settings first to enable only that type and set others to false, then call run_migration. Returns status started or conflict if already running. Offer to show the migration log in plain language (e.g. 'Would you like me to show the log?') and provide a link to the migration output page so the user can follow progress: [Open migration output](/migration_output.html)."""
    try:
        r = requests.post(f"{BASE_URL}/api/run_migration", timeout=TIMEOUT)
        if r.status_code == 202:
            return json.dumps(r.json() or {"status": "started"})
        if r.status_code == 409:
            return json.dumps(r.json() or {"status": "conflict", "message": "Migration already in progress."})
        r.raise_for_status()
        return json.dumps(r.json() if r.content else {})
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def stop_migration() -> str:
    """Stop the currently running migration if any."""
    try:
        data = requests.post(f"{BASE_URL}/stop_migration", timeout=30).json()
        return json.dumps(data)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_migration_log(lines: int = 500) -> str:
    """Get the last N lines of the migration log. Use after run_migration to show progress or answer questions about the last run. For specific counts or facts (e.g. how many dashboards migrated successfully), use a large lines value (e.g. 2000 or 5000) to retrieve enough log to parse and report exact numbers. You can also offer a download link for the full log: [Download migration log](/api/migration-log/download)."""
    try:
        data = _get("/api/migration-log", {"lines": lines})
        return data.get("content", "") or data.get("message", json.dumps(data))
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def test_connection(server_name: str) -> str:
    """Test connection to a configured server by name. Returns success or error message. Use when the user gives an explicit server name (e.g. 'test connection to ps-sandbox')."""
    try:
        r = requests.post(f"{BASE_URL}/api/test-connection", json={"serverName": server_name}, timeout=30)
        data = r.json()
        return json.dumps(data)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def test_connection_to_source() -> str:
    """Test connection to the currently selected source server (from settings). Use when the user asks to validate or test connection to the 'source' or 'source server'."""
    try:
        settings = _get("/api/settings")
        server_name = (settings or {}).get("selected_source_server")
        if not server_name:
            return json.dumps({"error": "No source server selected. Please select a source server in settings first."})
        r = requests.post(f"{BASE_URL}/api/test-connection", json={"serverName": server_name}, timeout=30)
        data = r.json()
        return json.dumps(data)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def test_connection_to_target() -> str:
    """Test connection to the currently selected target server (from settings). Use when the user asks to validate or test connection to the 'target' or 'target server'."""
    try:
        settings = _get("/api/settings")
        server_name = (settings or {}).get("selected_target_server")
        if not server_name:
            return json.dumps({"error": "No target server selected. Please select a target server in settings first."})
        r = requests.post(f"{BASE_URL}/api/test-connection", json={"serverName": server_name}, timeout=30)
        data = r.json()
        return json.dumps(data)
    except requests.RequestException as e:
        return json.dumps({"error": str(e)})


def run() -> None:
    """Run the MCP server over stdio (default)."""
    mcp.run(transport="stdio")
