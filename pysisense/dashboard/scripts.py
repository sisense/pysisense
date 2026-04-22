from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import jsbeautifier as beautifier


class ScriptsMixin:
    # Configs for extract script method
    _DASHBOARD_SCRIPT_TEMPLATE = """\
    /*
    Welcome to your Dashboard's Script.

    To learn how you can access the Widget and Dashboard objects, see the online documentation at https://sisense.dev/guides/js/extensions
    */"""

    _WIDGET_TEMPLATE_REGEX = r"/\*.*?see the online documentation at.*?\*/"

    _JS_INDENT_SIZE = 4

    def add_dashboard_script(self, dashboard_id, script, executing_user=None):
        """
        Adds or overwrites a script to a dashboard, temporarily changing ownership if required.

        Parameters:
            dashboard_id (str): The ID of the dashboard where the script will be added.
            script (str): The JavaScript script as either:
                        - A properly formatted JSON string.
                        - A raw Python docstring (multi-line string).
            executing_user (str, optional): The username of the API user. This is used to temporarily change
                                        the owner of the dashboard, as only the owner can add scripts.
                                        If not provided, assumes the dashboard owner is the same as the API user.

        Returns:
            str: Success message or error details.
        """

        add_dashboard_script_endpoint = f"/api/dashboards/{dashboard_id}"

        # If executing_user is provided, temporarily change dashboard ownership
        if executing_user:
            self.logger.debug(f"API username '{executing_user}' provided. Fetching original owner of dashboard {dashboard_id}.")

            dashboard_response = self.api_client.get(f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}&asObject=false")
            if dashboard_response is None or dashboard_response.status_code != 200:
                self.logger.error(f"Dashboard with ID '{dashboard_id}' not found or failed to retrieve.")
                return f"Error: Dashboard '{dashboard_id}' not found."

            dashboard_data = dashboard_response.json()
            original_owner_id = dashboard_data[0].get("owner")

            # Fetch existing dashboard shares before changing ownership
            self.logger.debug(f"Retrieving existing shares of dashboard {dashboard_id} to restore later.")
            shares_response = self.api_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")

            if shares_response is None or shares_response.status_code != 200:
                error_message = shares_response.json() if shares_response else "No response received."
                self.logger.error(f"Failed to retrieve shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to retrieve shares for dashboard {dashboard_id}."

            shares = shares_response.json().get("sharesTo", [])

            # Change ownership to executing_user
            self.logger.info(f"Changing ownership of dashboard {dashboard_id} to '{executing_user}'.")
            api_user = self.access_mgmt.get_user(executing_user)
            api_user_id = api_user.get("USER_ID")

            if not api_user_id:
                self.logger.error(f"User '{executing_user}' not found.")
                return f"Error: User '{executing_user}' not found."

            ownership_response = self.api_client.post(f"/api/v1/dashboards/{dashboard_id}/change_owner?adminAccess=true", data={"ownerId": api_user_id, "originalOwnerRule": "edit"})

            if ownership_response is None or ownership_response.status_code != 200:
                error_message = ownership_response.json() if ownership_response else "No response received."
                self.logger.error(f"Failed to change ownership of dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to change ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully changed to '{executing_user}'.")
        else:
            self.logger.debug("No API username provided. Assuming the dashboard owner is the same as the API user.")

        # Convert script to JSON format if needed
        try:
            if isinstance(script, str) and not script.startswith("{"):
                self.logger.debug("Dashboard Script received as a Python docstring. Converting to JSON format.")
                script = json.dumps({"script": script}, ensure_ascii=False)

            script_dict = json.loads(script) if isinstance(script, str) else script
            self.logger.debug(f"Final dashboard script payload prepared: {script_dict}")
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON format for dashboard script.")
            return "Error: Dashboard Script must be a valid JSON string."

        # Add script to the dashboard
        # Add script to the dashboard
        script_response = self.api_client.put(add_dashboard_script_endpoint, data=script_dict)

        if script_response is None or script_response.status_code != 200:
            try:
                error_message = script_response.json()
            except Exception:
                error_message = getattr(script_response, "text", "No response text")

            self.logger.error(f"Failed to add script to dashboard {dashboard_id}. Error: {error_message}")

            if script_response.status_code == 404 and executing_user is None:
                return (
                    f"Error: Failed to add dashboard script to dashboard '{dashboard_id}'. "
                    f"This may be because the API token used does not belong to the dashboard owner, "
                    f"and no 'executing_user' was provided. Only the dashboard owner can modify scripts."
                    f" Please provide the 'executing_user' parameter to change ownership temporarily or "
                    f"set the API token user as dashboard owner."
                )

            return f"Error: Failed to add dashboard script to dashboard {dashboard_id}."

        self.logger.info(f"Dashboard Script successfully added to dashboard {dashboard_id}.")

        # Restore original ownership if changed
        if executing_user:
            self.logger.info(f"Restoring original ownership of dashboard {dashboard_id} to '{original_owner_id}'.")

            shares_payload = [{"shareId": s["shareId"], "type": s["type"], "rule": s.get("rule", "edit"), "subscribe": s.get("subscribe", False)} for s in shares]

            restore_shares_response = self.api_client.post(f"/api/shares/dashboard/{dashboard_id}", data={"sharesTo": shares_payload})

            if restore_shares_response is None or restore_shares_response.status_code != 200:
                error_message = restore_shares_response.json() if restore_shares_response else "No response received."
                self.logger.error(f"Failed to restore shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to restore shares for dashboard {dashboard_id}."

            ownership_restore_response = self.api_client.post(f"/api/v1/dashboards/{dashboard_id}/change_owner", data={"ownerId": original_owner_id, "originalOwnerRule": "edit"})

            if ownership_restore_response is None or ownership_restore_response.status_code != 200:
                error_message = ownership_restore_response.json() if ownership_restore_response else "No response received."
                self.logger.error(f"Failed to revert ownership of dashboard {dashboard_id} to original owner. Error: {error_message}")
                return f"Error: Failed to revert ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully restored to original owner.")

        return "Dashboard Script added successfully."

    def add_widget_script(self, dashboard_id, widget_id, script, executing_user=None):
        """
        Adds or overwrites a script for a specific widget within a dashboard.

        If required, temporarily changes the dashboard ownership, as only the owner can modify widget scripts.

        Parameters:
            dashboard_id (str): The ID of the dashboard containing the widget.
            widget_id (str): The ID of the widget where the script will be added.
            script (str): The JavaScript script as either:
                        - A properly formatted JSON string.
                        - A raw Python docstring (multi-line string).
            executing_user (str, optional): The username of the API user. This is used to temporarily change
                                        the owner of the dashboard, as only the owner can add scripts.
                                        If not provided, assumes the dashboard owner is the same as the API user.

        Returns:
            str: Success message or error details.
        """

        add_widget_script_endpoint = f"/api/dashboards/{dashboard_id}/widgets/{widget_id}"

        # If executing_user is provided, temporarily change dashboard ownership
        if executing_user:
            self.logger.debug(f"API username '{executing_user}' provided. Fetching original owner of dashboard {dashboard_id}.")

            dashboard_response = self.api_client.get(f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}&asObject=false")
            if dashboard_response is None or dashboard_response.status_code != 200:
                self.logger.error(f"Dashboard with ID '{dashboard_id}' not found or failed to retrieve.")
                return f"Error: Dashboard '{dashboard_id}' not found."

            dashboard_data = dashboard_response.json()
            original_owner_id = dashboard_data[0].get("owner")

            # Fetch existing dashboard shares before changing ownership
            self.logger.debug(f"Retrieving existing shares of dashboard {dashboard_id} to restore later.")
            shares_response = self.api_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")

            if shares_response is None or shares_response.status_code != 200:
                error_message = shares_response.json() if shares_response else "No response received."
                self.logger.error(f"Failed to retrieve shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to retrieve shares for dashboard {dashboard_id}."

            shares = shares_response.json().get("sharesTo", [])

            # Change ownership to executing_user
            self.logger.info(f"Changing ownership of dashboard {dashboard_id} to '{executing_user}'.")
            api_user = self.access_mgmt.get_user(executing_user)
            api_user_id = api_user.get("USER_ID")

            if not api_user_id:
                self.logger.error(f"User '{executing_user}' not found.")
                return f"Error: User '{executing_user}' not found."

            ownership_response = self.api_client.post(f"/api/v1/dashboards/{dashboard_id}/change_owner?adminAccess=true", data={"ownerId": api_user_id, "originalOwnerRule": "edit"})

            if ownership_response is None or ownership_response.status_code != 200:
                error_message = ownership_response.json() if ownership_response else "No response received."
                self.logger.error(f"Failed to change ownership of dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to change ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully changed to '{executing_user}'.")
        else:
            self.logger.debug("No API username provided. Assuming the dashboard owner is the same as the API user.")

        # Convert script to JSON format if needed
        try:
            if isinstance(script, str) and not script.startswith("{"):
                self.logger.debug("Widget Script received as a Python docstring. Converting to JSON format.")
                script = json.dumps({"script": script}, ensure_ascii=False)

            script_dict = json.loads(script) if isinstance(script, str) else script
            self.logger.debug(f"Final widget script payload prepared: {script_dict}")
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON format for widget script.")
            return "Error: Widget Script must be a valid JSON string."

        # Add script to the dashboard
        script_response = self.api_client.put(add_widget_script_endpoint, data=script_dict)

        if script_response is None or script_response.status_code != 200:
            try:
                error_message = script_response.json()
            except Exception:
                error_message = getattr(script_response, "text", "No response text")

            self.logger.error(f"Failed to add widget script to dashboard {dashboard_id} widget {widget_id}. Error: {error_message}")

            if script_response.status_code == 403 and executing_user is None:
                return (
                    f"Error: Failed to add widget script to dashboard '{dashboard_id}', widget '{widget_id}'. "
                    f"This may be because the API token used does not belong to the dashboard owner, "
                    f"and no 'executing_user' was provided. Only the dashboard owner can modify scripts."
                    f" Please provide the 'executing_user' parameter to change ownership temporarily or "
                    f"set the API token user as dashboard owner."
                )

            return f"Error: Failed to add widget script to dashboard {dashboard_id} widget {widget_id}."

        self.logger.info(f"Widget Script successfully added to dashboard {dashboard_id} widget {widget_id}.")

        # Republish the dashboard to apply changes
        self.logger.info(f"Republishing dashboard {dashboard_id} to apply changes.")
        republish_response = self.api_client.post(f"/api/v1/dashboards/{dashboard_id}/publish?force=true")
        if republish_response.status_code == 204:
            self.logger.info(f"Dashboard {dashboard_id} republished successfully.")
        else:
            error_message = republish_response.json() if republish_response else "No response received."
            self.logger.error(f"Failed to republish dashboard {dashboard_id}. Error: {error_message}")
            return f"Error: Failed to republish dashboard {dashboard_id}. Error: {error_message}"

        # Restore original ownership if changed
        if executing_user:
            self.logger.info(f"Restoring original ownership of dashboard {dashboard_id} to '{original_owner_id}'.")

            shares_payload = [{"shareId": s["shareId"], "type": s["type"], "rule": s.get("rule", "edit"), "subscribe": s.get("subscribe", False)} for s in shares]

            restore_shares_response = self.api_client.post(f"/api/shares/dashboard/{dashboard_id}", data={"sharesTo": shares_payload})

            if restore_shares_response is None or restore_shares_response.status_code != 200:
                error_message = restore_shares_response.json() if restore_shares_response else "No response received."
                self.logger.error(f"Failed to restore shares for dashboard {dashboard_id}. Error: {error_message}")
                return f"Error: Failed to restore shares for dashboard {dashboard_id}."

            ownership_restore_response = self.api_client.post(f"/api/v1/dashboards/{dashboard_id}/change_owner", data={"ownerId": original_owner_id, "originalOwnerRule": "edit"})

            if ownership_restore_response is None or ownership_restore_response.status_code != 200:
                error_message = ownership_restore_response.json() if ownership_restore_response else "No response received."
                self.logger.error(f"Failed to revert ownership of dashboard {dashboard_id} to original owner. Error: {error_message}")
                return f"Error: Failed to revert ownership of dashboard {dashboard_id}."

            self.logger.info(f"Ownership of dashboard {dashboard_id} successfully restored to original owner.")

        return "Widget Script added successfully."

    def extract_scripts(
        self,
        dashboard: str,
        output_dir: str | Path | None = None,
    ) -> list[dict[str, Any]]:
        """Extract and save JavaScript scripts from a Sisense dashboard via the API.

        Fetches the full dashboard export from Sisense, pulls the dashboard-level
        ``script`` field and every widget-level ``script`` field, removes default
        Sisense template boilerplate, beautifies the code with a 4-space indent,
        and writes each script to a structured output directory.

        Output layout::

            <output_dir>/<title>_<oid>/dashboard_script_1.js
            <output_dir>/<title>_<oid>/widgets/<widget_oid>_WidgetScript.js

        A JS footer comment is appended to every file with the dashboard's
        ``lastOpened`` timestamp and the Sisense URL path for that dashboard or
        widget.

        Parameters
        ----------
        dashboard : str
            Dashboard reference: either a 24-character dashboard ID or a
            dashboard title. Resolved automatically via
            :meth:`resolve_dashboard_reference`.
        output_dir : str or Path, optional
            Root directory where output folders are created. Defaults to
            a ``results/`` folder inside the current working directory
            (i.e. wherever the calling script is run from).

        Returns
        -------
        list[dict[str, Any]]
            One entry per written file. Each entry contains:

            - ``type`` (``"dashboard"`` or ``"widget"``): script source
            - ``oid``: dashboard OID
            - ``title``: dashboard title
            - ``widget_oid``: widget OID (``"widget"`` entries only)
            - ``widget_type``: Sisense widget type (``"widget"`` entries only)
            - ``path``: absolute path of the written ``.js`` file as a string

            Returns ``[{"error": "..."}]`` on failure.
        """
        output_dir = Path(output_dir) if output_dir is not None else Path.cwd() / "results"

        ref = self.resolve_dashboard_reference(dashboard)
        if not ref["success"]:
            self.logger.error(f"Could not resolve dashboard reference '{dashboard}': {ref['error']}")
            return [{"error": ref["error"]}]

        dashboard_id = ref["dashboard_id"]
        self.logger.debug(f"Fetching full export for dashboard '{dashboard_id}'")

        response = self.api_client.get(f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true")
        if response is None or response.status_code != 200:
            error_msg = f"Failed to export dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return [{"error": error_msg}]

        try:
            data = response.json()
        except Exception:
            error_msg = f"Failed to parse export response for dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return [{"error": error_msg}]

        if not data or not isinstance(data, list):
            error_msg = f"Unexpected export response structure for dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return [{"error": error_msg}]

        dashboard_data = data[0]
        title_safe = dashboard_data.get("title", "dashboard").strip().replace(" ", "_")
        oid = dashboard_data.get("oid", dashboard_id)
        dash_output_dir = output_dir / f"{title_safe}_{oid}"
        dash_output_dir.mkdir(parents=True, exist_ok=True)
        self.logger.debug(f"Output directory: {dash_output_dir}")

        results: list[dict[str, Any]] = []
        results.extend(self._extract_dashboard_level_script(dashboard_data, dash_output_dir))
        results.extend(self._extract_widget_level_scripts(dashboard_data, dash_output_dir))

        self.logger.info(f"Extracted {len(results)} script(s) from dashboard '{oid}'")
        return results

    def extract_scripts_from_all_dashboards(
        self,
        output_dir: str | Path | None = None,
    ) -> list[dict[str, Any]]:
        """Extract and save JavaScript scripts from every dashboard in the environment.

        Fetches the full list of dashboards via :meth:`get_all_dashboards` and
        calls :meth:`extract_scripts` on each one. Dashboards that have no
        scripts are silently skipped. Output is written under ``output_dir``
        with one sub-folder per dashboard.

        Parameters
        ----------
        output_dir : str or Path, optional
            Root directory where output folders are created. Defaults to
            a ``results/`` folder inside the current working directory
            (i.e. wherever the calling script is run from).

        Returns
        -------
        list[dict[str, Any]]
            Combined list of written-file entries from all processed dashboards.
            Returns ``[{"error": "..."}]`` if the dashboard list cannot be retrieved.
        """
        output_dir = Path(output_dir) if output_dir is not None else Path.cwd() / "results"

        all_dashboards = self.get_all_dashboards()
        if isinstance(all_dashboards, dict) and "error" in all_dashboards:
            self.logger.error(f"Failed to retrieve dashboard list: {all_dashboards['error']}")
            return [{"error": all_dashboards["error"]}]

        self.logger.info(f"Processing scripts for {len(all_dashboards)} dashboard(s)")

        all_results: list[dict[str, Any]] = []
        for dash in all_dashboards:
            oid = dash.get("oid")
            if not oid:
                continue
            all_results.extend(self.extract_scripts(oid, output_dir))

        self.logger.info(f"Total scripts extracted: {len(all_results)}")
        return all_results

    def _beautify_js_code(self, js_code: str) -> str:
        """Return ``js_code`` formatted with jsbeautifier using a 4-space indent."""
        opts = beautifier.default_options()
        opts.indent_size = self._JS_INDENT_SIZE
        return beautifier.beautify(js_code, opts)

    def _write_script_file(self, js_code: str, output_path: Path, footer_comment: str) -> dict[str, Any]:
        """Beautify ``js_code``, append ``footer_comment``, and write to ``output_path``."""
        try:
            content = self._beautify_js_code(js_code) + "\n\n" + footer_comment
            output_path.write_text(content, encoding="utf-8")
            self.logger.debug(f"Wrote script: {output_path}")
            return {"path": str(output_path)}
        except OSError as exc:
            error_msg = f"Failed to write '{output_path}': {exc}"
            self.logger.error(error_msg)
            return {"error": error_msg}

    def _extract_dashboard_level_script(self, dashboard: dict[str, Any], output_dir: Path) -> list[dict[str, Any]]:
        """Extract, clean, and write the dashboard-level script if present and non-empty."""
        script = dashboard.get("script")
        if not isinstance(script, str):
            return []

        cleaned = script.replace(self._DASHBOARD_SCRIPT_TEMPLATE, "").strip()
        if not cleaned:
            return []

        oid = dashboard.get("oid", "unknown")
        title = dashboard.get("title", "unknown")
        last_opened = dashboard.get("lastOpened", "unknown")
        footer = f"// Dashboard last opened on {last_opened}\n// To view dashboard URL Path is /app/main/dashboards/{oid}"

        result = self._write_script_file(cleaned, output_dir / "dashboard_script_1.js", footer)
        if "error" in result:
            return [result]

        return [{"type": "dashboard", "oid": oid, "title": title, "path": result["path"]}]

    def _extract_widget_level_scripts(self, dashboard: dict[str, Any], output_dir: Path) -> list[dict[str, Any]]:
        """Extract, clean, and write scripts for all widgets that carry one."""
        widgets = dashboard.get("widgets", [])
        if not isinstance(widgets, list) or not widgets:
            return []

        oid = dashboard.get("oid", "unknown")
        title = dashboard.get("title", "unknown")
        last_opened = dashboard.get("lastOpened", "unknown")
        widgets_dir = output_dir / "widgets"
        widgets_dir.mkdir(exist_ok=True)

        results: list[dict[str, Any]] = []
        for widget in widgets:
            script = widget.get("script")
            if not isinstance(script, str):
                continue

            cleaned = re.sub(self._WIDGET_TEMPLATE_REGEX, "", script, flags=re.DOTALL).strip()
            if not cleaned:
                continue

            widget_oid = widget.get("oid", "unknown")
            widget_type = widget.get("type", "unknown")
            footer = f"// Dashboard last opened on {last_opened}\n// Script is for widget type of {widget_type}\n// To view widget URL Path is /app/main/dashboards/{oid}/widgets/{widget_oid}"
            result = self._write_script_file(cleaned, widgets_dir / f"{widget_oid}_WidgetScript.js", footer)
            if "error" in result:
                results.append(result)
                continue

            results.append({"type": "widget", "oid": oid, "title": title, "widget_oid": widget_oid, "widget_type": widget_type, "path": result["path"]})

        return results
