from __future__ import annotations

import json
import re
from typing import Any

import jsbeautifier as beautifier

from ..utils import _dashboard_write_access, _finish_ownership, _user_id_for_email, _with_query, _write_dashboard_copies


class ScriptsMixin:
    def add_dashboard_script(self, dashboard_id: str, script: str | dict[str, Any], executing_user: str | None = None, act_as_owner: bool = False) -> dict[str, Any]:
        """Attach a JavaScript script to a dashboard.

        Sends ``PUT /api/dashboards/{dashboard_id}`` with ``{"script": ...}``. With Dashboard
        Co-Authoring on, a published dashboard's script lives on its shared copy (what
        viewers see) and on the owner's private copy; both are written, shared first with
        ``sharedMode=true``, and the dashboard is republished. Only the owner may write; a
        non-owner is refused with the owner named, unless ``act_as_owner`` is true and the
        token belongs to an administrator, in which case ownership is borrowed for the
        change and returned afterwards, together with the exact share list.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard.
        script : str | dict[str, Any]
            The script, as plain JavaScript, as a JSON string ``{"script": "..."}``, or as
            that dict.
        executing_user : str | None, optional
            Deprecated in favour of ``act_as_owner``: the email of the user to transfer
            ownership to for the change. When given, ownership is borrowed for that user
            exactly as ``act_as_owner`` does for the token's user. Default ``None``.
        act_as_owner : bool, optional
            Take ownership temporarily when the token's user is an administrator but not
            the owner. Default ``False``: refuse instead.

        Returns
        -------
        dict[str, Any]
            ``{"success": True, "message": "...", "copies_updated": [...], "published": bool | None}``
            (``published`` is ``None`` when no shared copy exists), plus
            ``ownership_transferred_temporarily`` / ``original_owner`` when ownership was
            borrowed. On failure the standard ``{"ok": False, "error": "...", ...}`` dict, with
            ``owner`` and ``co_owners`` when the token's user is not the owner.
        """
        try:
            if isinstance(script, str) and not script.startswith("{"):
                self.logger.debug("Dashboard Script received as a Python docstring. Converting to JSON format.")
                script = json.dumps({"script": script}, ensure_ascii=False)
            script_dict = json.loads(script) if isinstance(script, str) else script
            self.logger.debug(f"Final dashboard script payload prepared: {list(script_dict) if isinstance(script_dict, dict) else type(script_dict).__name__}")
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON format for dashboard script.")
            return {"ok": False, "error": "Dashboard Script must be a valid JSON string."}

        borrower = None
        if executing_user:
            borrower = _user_id_for_email(self.api_client, executing_user)
            if not borrower:
                return {"ok": False, "error": f"User '{executing_user}' not found."}
            act_as_owner = True
        access = _dashboard_write_access(self.api_client, self.logger, dashboard_id, act_as_owner, borrower=borrower, what="modify its script")
        if access.get("ok") is False:
            return access
        title, co_authoring, borrowed = access["title"], access["co_authoring"], access["borrowed"]
        result: dict[str, Any] = {"ok": False, "error": f"Adding a script to dashboard '{title}' failed unexpectedly."}
        try:
            outcome = _write_dashboard_copies(
                self.api_client,
                self.logger,
                dashboard_id,
                title,
                co_authoring,
                lambda query: self.api_client.put(_with_query(f"/api/dashboards/{dashboard_id}", query), data=script_dict),
                label="add dashboard script",
            )
            if outcome.get("ok") is False:
                result = outcome
                if outcome.get("status_code") == 404 and not act_as_owner:
                    result["error"] += " (the API token user may not be the dashboard owner — only the owner can modify scripts; pass act_as_owner=True to change ownership temporarily)"
            else:
                self.logger.info(f"Dashboard Script successfully added to dashboard {dashboard_id}.")
                result = {
                    "success": True,
                    "message": f"Dashboard Script added successfully to dashboard '{dashboard_id}'.",
                    "copies_updated": outcome["copies_updated"],
                    "published": outcome["published"],
                }
                if outcome.get("publish_error"):
                    result["publish_error"] = outcome["publish_error"]
        finally:
            result = _finish_ownership(self.api_client, self.logger, dashboard_id, title, borrowed, result)
        return result

    def add_widget_script(self, dashboard_id: str, widget_id: str, script: str | dict[str, Any], executing_user: str | None = None, act_as_owner: bool = False) -> dict[str, Any]:
        """Attach a JavaScript script to a widget.

        Sends ``PUT /api/dashboards/{dashboard_id}/widgets/{widget_id}`` with ``{"script": ...}``
        and republishes the dashboard (``POST .../publish?force=true``) so the change reaches
        viewers. With Dashboard Co-Authoring on, a published dashboard's widget lives on its
        shared copy and on the owner's private copy; both are written, shared first with
        ``sharedMode=true``. Only the owner may write; a non-owner is refused with the owner
        named, unless ``act_as_owner`` is true and the token belongs to an administrator, in
        which case ownership is borrowed for the change and returned afterwards, together
        with the exact share list.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard that contains the widget.
        widget_id : str
            The ``oid`` of the widget.
        script : str | dict[str, Any]
            The script, as plain JavaScript, as a JSON string ``{"script": "..."}``, or as
            that dict.
        executing_user : str | None, optional
            Deprecated in favour of ``act_as_owner``: the email of the user to transfer
            ownership to for the change. When given, ownership is borrowed for that user
            exactly as ``act_as_owner`` does for the token's user. Default ``None``.
        act_as_owner : bool, optional
            Take ownership temporarily when the token's user is an administrator but not
            the owner. Default ``False``: refuse instead.

        Returns
        -------
        dict[str, Any]
            ``{"success": True, "message": "...", "copies_updated": [...], "published": bool}``,
            plus ``ownership_transferred_temporarily`` / ``original_owner`` when ownership was
            borrowed. On failure the standard ``{"ok": False, "error": "...", ...}`` dict, with
            ``owner`` and ``co_owners`` when the token's user is not the owner.
        """
        try:
            if isinstance(script, str) and not script.startswith("{"):
                self.logger.debug("Widget Script received as a Python docstring. Converting to JSON format.")
                script = json.dumps({"script": script}, ensure_ascii=False)
            script_dict = json.loads(script) if isinstance(script, str) else script
            self.logger.debug(f"Final widget script payload prepared: {list(script_dict) if isinstance(script_dict, dict) else type(script_dict).__name__}")
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON format for widget script.")
            return {"ok": False, "error": "Widget Script must be a valid JSON string."}

        borrower = None
        if executing_user:
            borrower = _user_id_for_email(self.api_client, executing_user)
            if not borrower:
                return {"ok": False, "error": f"User '{executing_user}' not found."}
            act_as_owner = True
        access = _dashboard_write_access(self.api_client, self.logger, dashboard_id, act_as_owner, borrower=borrower, what="modify its widgets")
        if access.get("ok") is False:
            return access
        title, co_authoring, borrowed = access["title"], access["co_authoring"], access["borrowed"]
        result: dict[str, Any] = {"ok": False, "error": f"Adding a script to widget '{widget_id}' failed unexpectedly."}
        try:
            outcome = _write_dashboard_copies(
                self.api_client,
                self.logger,
                dashboard_id,
                title,
                co_authoring,
                lambda query: self.api_client.put(_with_query(f"/api/dashboards/{dashboard_id}/widgets/{widget_id}", query), data=script_dict),
                label=f"add widget script to widget '{widget_id}'",
                publish="always",
            )
            if outcome.get("ok") is False:
                result = outcome
                if outcome.get("status_code") == 403 and not act_as_owner:
                    result["error"] += " (the API token user may not be the dashboard owner — only the owner can modify scripts; pass act_as_owner=True to change ownership temporarily)"
            else:
                self.logger.info(f"Widget Script successfully added to dashboard {dashboard_id} widget {widget_id}.")
                result = {
                    "success": True,
                    "message": f"Widget Script added successfully to dashboard '{dashboard_id}', widget '{widget_id}'.",
                    "copies_updated": outcome["copies_updated"],
                    "published": outcome["published"],
                }
                if outcome.get("publish_error"):
                    result["publish_error"] = outcome["publish_error"]
        finally:
            result = _finish_ownership(self.api_client, self.logger, dashboard_id, title, borrowed, result)
        return result

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
            A :class:`SisenseScript` instance when the dashboard is retrieved and
            has a script. ``{"error": "..."}`` when the export fails (including
            ``status_code`` for HTTP failures such as missing access) or when the
            dashboard has no script — a normal state, reported explicitly.
        """
        dashboard_data = self.export_dashboard(dashboard_id)

        if "error" in dashboard_data:
            return dashboard_data

        script = dashboard_data.get("script")
        if not script:
            msg = f"Dashboard '{dashboard_data.get('title') or dashboard_id}' has no dashboard script."
            self.logger.info(msg)
            return {"ok": False, "error": msg}

        DASHBOARD_SCRIPT_TEMPLATE = """\
        /*
        Welcome to your Dashboard's Script.

        To learn how you can access the Widget and Dashboard objects, see the online documentation at https://sisense.dev/guides/js/extensions
        */"""

        footer = "// Dashboard Title: {title}\n// To view dashboard URL Path is {url}"

        return SisenseScript(
            url=f"/app/main/dashboards/{dashboard_data.get('oid', 'unknown')}",
            title=dashboard_data.get("title", "unknown"),
            script=script,
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
            A :class:`SisenseScript` instance when the widget is found and has a
            script. ``{"error": "..."}`` when the export fails (including
            ``status_code`` for HTTP failures such as missing access), the widget
            is not found, or the widget has no script — a normal state, reported
            explicitly.
        """
        dashboard_data = self.export_dashboard(dashboard_id)

        if "error" in dashboard_data:
            return dashboard_data

        WIDGET_TEMPLATE_REGEX = r"/\*.*?see the online documentation at.*?\*/"

        widgets = dashboard_data.get("widgets") or []
        widget_data = next((w for w in widgets if w.get("oid") == widget_id), None)

        if not widget_data:
            return {"ok": False, "error": f"Widget with ID '{widget_id}' not found in dashboard '{dashboard_id}'"}

        # Some Sisense versions omit the script (and title) from the export
        # payload's widget objects entirely — fetch the widget directly, which
        # carries the full object including its script.
        if "script" not in widget_data:
            self.logger.debug(f"Export payload omits widget script fields on this Sisense version; fetching widget {widget_id} directly.")
            direct_widget = self.get_widget_by_id(dashboard_id, widget_id)
            if isinstance(direct_widget, dict) and "error" not in direct_widget:
                widget_data = direct_widget

        script = widget_data.get("script")
        if not script:
            msg = f"Widget '{widget_data.get('title') or widget_id}' has no widget script."
            self.logger.info(msg)
            return {"ok": False, "error": msg}

        footer = "// Widget Title: {title} \n// Script is for widget type of {widget_type}\n// To view widget URL Path is {url}"

        return SisenseScript(
            url=f"/app/main/dashboards/{dashboard_data.get('oid', 'unknown')}/widgets/{widget_data.get('oid', 'unknown')}",
            title=widget_data.get("title", "unknown"),
            type=widget_data.get("type", "unknown"),
            script=script,
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
