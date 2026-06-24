from __future__ import annotations

import json
import re
from typing import Any

import jsbeautifier as beautifier


class ScriptsMixin:
    def add_dashboard_script(self, dashboard_id: str, script: str | dict[str, Any], executing_user: str | None = None) -> str:
        """Add or overwrite a script on a dashboard.

        Adds or overwrites a script on a dashboard, temporarily changing
        ownership to the executing user and restoring it (along with the
        original shares) afterwards when required, as only the dashboard owner
        can modify scripts.

        Parameters
        ----------
        dashboard_id : str
            The ID of the dashboard where the script will be added.
        script : str | dict[str, Any]
            The JavaScript script to add, provided as either a properly
            formatted JSON string, a raw multi-line script string, or a
            pre-built payload dictionary.
        executing_user : str | None, optional
            The username of the API user, used to temporarily change the owner
            of the dashboard, as only the owner can add scripts. If not
            provided, assumes the dashboard owner is the same as the API user.
            (format: email)

        Returns
        -------
        str
            A success message on success, or an error message string describing
            the failure.
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

    def add_widget_script(self, dashboard_id: str, widget_id: str, script: str | dict[str, Any], executing_user: str | None = None) -> str:
        """Add or overwrite a script for a specific widget within a dashboard.

        Adds or overwrites a script for a specific widget, republishing the
        dashboard to apply the changes. If required, it temporarily changes the
        dashboard ownership to the executing user and restores it (along with
        the original shares) afterwards, as only the owner can modify widget
        scripts.

        Parameters
        ----------
        dashboard_id : str
            The ID of the dashboard containing the widget.
        widget_id : str
            The ID of the widget where the script will be added.
        script : str | dict[str, Any]
            The JavaScript script to add, provided as either a properly
            formatted JSON string, a raw multi-line script string, or a
            pre-built payload dictionary.
        executing_user : str | None, optional
            The username of the API user, used to temporarily change the owner
            of the dashboard, as only the owner can add scripts. If not
            provided, assumes the dashboard owner is the same as the API user.
            (format: email)

        Returns
        -------
        str
            A success message on success, or an error message string describing
            the failure.
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

    def get_dashboard_script(self, dashboard_id: str) -> SisenseScript | dict[str, str]:
        """Build a formatted dashboard script helper object.

        Retrieves a dashboard export payload and wraps its script content in a
        :class:`SisenseScript` helper that can render plain text, markdown, or file output.

        Parameters
        ----------
        dashboard_id : str
            The dashboard identifier to export.

        Returns
        -------
        SisenseScript | dict[str, str]
            A :class:`SisenseScript` instance when the dashboard is retrieved successfully,
            or an ``{"error": "..."}`` dictionary from ``export_dashboard``.
        """
        dashboard_data = self.export_dashboard(dashboard_id)

        if "error" in dashboard_data:
            return dashboard_data

        DASHBOARD_SCRIPT_TEMPLATE = """\
        /*
        Welcome to your Dashboard's Script.

        To learn how you can access the Widget and Dashboard objects, see the online documentation at https://sisense.dev/guides/js/extensions
        */"""

        footer = "// Dashboard Title: {title}\n// To view dashboard URL Path is {url}"

        return SisenseScript(
            url=f"/app/main/dashboards/{dashboard_data.get('oid', 'unknown')}",
            title=dashboard_data.get("title", "unknown"),
            script=dashboard_data["script"],
            template=DASHBOARD_SCRIPT_TEMPLATE,
            type=None,
            footer=footer,
        )

    def get_widget_script(self, dashboard_id: str, widget_id: str) -> SisenseScript | dict[str, str]:
        """Build a formatted widget script helper object.

        Retrieves a dashboard export payload, selects a widget by index/key, and
        wraps its script content in a :class:`SisenseScript` helper for downstream rendering.

        Parameters
        ----------
        dashboard_id : str
            The dashboard identifier to export.
        widget_id : str
            The widget identifier or lookup key used in the exported widget mapping.

        Returns
        -------
        SisenseScript | dict[str, str]
            A :class:`SisenseScript` instance when the widget is found and has script data,
            or an ``{"error": "..."}`` dictionary on failure.
        """
        dashboard_data = self.export_dashboard(dashboard_id)

        if "error" in dashboard_data:
            return dashboard_data

        WIDGET_TEMPLATE_REGEX = r"/\*.*?see the online documentation at.*?\*/"

        widgets = dashboard_data["widgets"]
        widget_data = next((w for w in widgets if w["oid"] == widget_id), None)

        if not widget_data:
            return {"error": f"Widget with ID '{widget_id}' not found in dashboard '{dashboard_id}'"}

        footer = "// Widget Title: {title} \n// Script is for widget type of {widget_type}\n// To view widget URL Path is {url}"

        return SisenseScript(
            url=f"/app/main/dashboards/{dashboard_data.get('oid', 'unknown')}/widgets/{widget_data.get('oid', 'unknown')}",
            title=widget_data.get("title", "unknown"),
            type=widget_data.get("type", "unknown"),
            script=widget_data["script"],
            template=WIDGET_TEMPLATE_REGEX,
            footer=footer,
        )


class SisenseScript:
    def __init__(self, url: str, title: str, type: str | None, script: str, template: str, footer: str) -> None:
        """Initialize a script rendering container.

        Parameters
        ----------
        url : str
            Relative Sisense URL path for the dashboard or widget.
        title : str
            Display title used in rendered outputs.
        type : str | None
            Widget type metadata. ``None`` for dashboard-level scripts.
        script : str
            Raw script body returned by Sisense.
        last_opened : str
            Last opened timestamp associated with the source object.
        template : str
            Regex pattern used to remove Sisense boilerplate template text.
        footer : str
            Footer template appended after script cleanup and formatting.
        """
        self.url = url
        self.title = title
        self.type = type
        self.script = script
        self.template = template
        self.footer = footer

    def _beautify_js_code(self, js_code: str) -> str:
        """Return ``js_code`` formatted with jsbeautifier using a 4-space indent."""
        opts = beautifier.default_options()
        opts.indent_size = 4
        return beautifier.beautify(js_code, opts)

    def to_text(self) -> str:
        """Render the script as formatted JavaScript text.

        Removes template boilerplate, appends metadata footer lines, and applies
        jsbeautifier formatting.

        Returns
        -------
        str
            Formatted JavaScript text, or an empty string when no script content
            remains after cleanup.
        """
        cleaned = re.sub(self.template, "", self.script, flags=re.DOTALL).strip()

        if not cleaned:
            return ""

        mapping = {
            "title": self.title,
            "url": self.url,
            "widget_type": self.type if self.type is not None else "unknown",
        }
        keys = re.findall(r"\{(\w+)\}", self.footer)
        footer = self.footer.format(**{k: mapping[k] for k in keys})
        return self._beautify_js_code(f"{cleaned}\n{footer}")

    def to_file(self, path: str) -> None:
        """Write the rendered script text to a file.

        Parameters
        ----------
        path : str
            Destination file path.
        """
        with open(path, "w") as f:
            f.write(self.to_text())

    def to_md(self) -> str:
        """Render the script as a markdown code block.

        Returns
        -------
        str
            Markdown content containing the script title and JavaScript code block.
        """
        return f"# {self.title}\n\n```js\n{self.to_text()}\n```\n"
