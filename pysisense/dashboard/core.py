from __future__ import annotations

import json
import re
import time
from typing import Any
from urllib.parse import quote

from ..queries import Queries
from ..utils import (
    _build_schema_index,
    _column_name_variants,
    _datasource_title,
    _discover_dashboards_on_datasource,
    _extract_error_message,
    _iter_dim_nodes,
    _jaql_panel,
    _reference_from_jaql,
    _widget_query_metadata,
)


class DashboardCoreMixin:
    # replace_datasource: how long to wait for Sisense to show a datasource change (writes are asynchronous)
    _SWAP_POLL_ATTEMPTS = 5
    _SWAP_POLL_DELAY = 2

    def get_all_dashboards(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all dashboards from the Sisense server.

        Sends ``GET /api/v1/dashboards/admin?dashboardType=owner`` using the
        admin endpoint, which requires elevated access.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of dashboard objects on success, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/v1/dashboards/admin?dashboardType=owner"
        self.logger.debug(f"Fetching all dashboards from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to retrieve dashboards", self.api_client)
            self.logger.error(failure["error"])
            return failure

        dashboards = response.json()
        self.logger.info(f"Successfully retrieved {len(dashboards)} dashboards.")
        return dashboards

    def get_dashboard_by_id(self, dashboard_id: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve a specific dashboard by its ID.

        Sends ``GET /api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}``
        against the admin endpoint.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard to retrieve.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The matching dashboard objects on success, or ``{"error": "..."}``
            when the request fails or no dashboard is found.
        """
        endpoint = f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}"
        self.logger.debug(f"Fetching dashboard with ID {dashboard_id} from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to retrieve dashboard '{dashboard_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        dashboard_data = response.json()
        if not dashboard_data:
            self.logger.warning(f"No dashboard found with ID {dashboard_id}.")
            return {"ok": False, "error": f"No dashboard found with ID '{dashboard_id}'"}

        self.logger.info(f"Successfully retrieved dashboard with ID {dashboard_id}.")
        return dashboard_data

    def get_dashboard_by_name(self, dashboard_name: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve a specific dashboard by its name.

        Sends ``GET /api/v1/dashboards/admin?dashboardType=owner&name={dashboard_name}``
        against the admin endpoint.

        Parameters
        ----------
        dashboard_name : str
            Title of the dashboard to retrieve.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The matching dashboard objects on success, or ``{"error": "..."}``
            when the request fails or no dashboard is found.
        """
        endpoint = f"/api/v1/dashboards/admin?dashboardType=owner&name={dashboard_name}"
        self.logger.debug(f"Fetching dashboard with name {dashboard_name} from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to retrieve dashboard '{dashboard_name}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        dashboard_data = response.json()
        if not dashboard_data:
            warning_msg = f"No dashboard found with name {dashboard_name}."
            self.logger.warning(warning_msg)
            return {"ok": False, "error": warning_msg}

        self.logger.info(f"Successfully retrieved dashboard with name {dashboard_name}.")
        return dashboard_data

    def resolve_dashboard_reference(self, dashboard_ref: str) -> dict[str, Any]:
        """
        Resolve a dashboard reference (ID or name) to a concrete dashboard ID and title.

        This helper accepts a single string that may be either:
        - a Sisense dashboard ID (24-character ID), or
        - a dashboard title (name).

        It first attempts to treat the reference as an ID using
        `get_dashboard_by_id`. If that fails or the reference does not look
        like an ID, it falls back to `get_dashboard_by_name`. The underlying
        methods are reused as-is.

        Parameters
        ----------
        dashboard_ref : str
            Dashboard reference to resolve. This can be either an ID or a name.

        Returns
        -------
        dict
            A dictionary with the following keys:
            - success (bool): True if the reference was resolved to a dashboard.
            - status_code (int): 200 if resolved successfully, 404 if not found,
              or 500 if an unexpected error occurred.
            - dashboard_id (str or None): Resolved dashboard ID (oid) if found,
              otherwise None.
            - dashboard_title (str or None): Resolved dashboard title if found,
              otherwise None.
            - error (str or None): Error message if success is False, otherwise None.
        """
        self.logger.debug(f"Resolving dashboard reference: {dashboard_ref}")

        # Basic heuristic: check if the reference looks like a 24-char hex ID
        is_id_candidate = bool(re.fullmatch(r"[0-9a-fA-F]{24}", dashboard_ref))

        # Try resolving as ID first if it looks like one
        if is_id_candidate:
            try:
                result_by_id = self.get_dashboard_by_id(dashboard_ref)

                # On success, get_dashboard_by_id returns a list with a single dashboard
                if isinstance(result_by_id, list) and result_by_id:
                    dash = result_by_id[0]
                    dashboard_id = dash.get("oid") or dash.get("_id") or dash.get("id")
                    dashboard_title = dash.get("title") or dash.get("name")

                    if dashboard_id:
                        self.logger.info(f"Resolved dashboard reference '{dashboard_ref}' as ID '{dashboard_id}'.")
                        return {
                            "success": True,
                            "status_code": 200,
                            "dashboard_id": dashboard_id,
                            "dashboard_title": dashboard_title,
                            "error": None,
                        }

                # If it returns an error dict or empty list, fall through to name resolution
            except Exception as exc:
                self.logger.exception(f"Unexpected error while resolving dashboard reference '{dashboard_ref}' as ID: {exc}")

        # Try resolving as a name
        try:
            result_by_name = self.get_dashboard_by_name(dashboard_ref)

            if isinstance(result_by_name, list) and result_by_name:
                dash = result_by_name[0]
                dashboard_id = dash.get("oid") or dash.get("_id") or dash.get("id")
                dashboard_title = dash.get("title") or dash.get("name")

                if dashboard_id:
                    self.logger.info(f"Resolved dashboard reference '{dashboard_ref}' as name to ID '{dashboard_id}'.")
                    return {
                        "success": True,
                        "status_code": 200,
                        "dashboard_id": dashboard_id,
                        "dashboard_title": dashboard_title,
                        "error": None,
                    }

            # If we got an error dict or empty list, treat as not found
        except Exception as exc:
            self.logger.exception(f"Unexpected error while resolving dashboard reference '{dashboard_ref}' as name: {exc}")
            return {
                "success": False,
                "ok": False,
                "status_code": 500,
                "dashboard_id": None,
                "dashboard_title": None,
                "error": str(exc),
            }

        # If both ID and name paths failed
        error_msg = f"Dashboard reference '{dashboard_ref}' could not be resolved as ID or name."
        self.logger.error(error_msg)
        return {
            "success": False,
            "ok": False,
            "status_code": 404,
            "dashboard_id": None,
            "dashboard_title": None,
            "error": error_msg,
        }

    def export_dashboard(self, dashboard_id: str) -> dict[str, Any]:
        """Export a dashboard definition using the Sisense admin export endpoint.

        Sends a GET request to ``/api/v1/dashboards/export`` with
        ``dashboardIds`` and ``adminAccess=true``. The response is a JSON array;
        this method returns the first dashboard object, which includes fields such
        as ``title``, ``oid``, ``script``, ``widgets``, ``layout``, and ``filters``.
        Other features (for example ``get_dashboard_script``) use this payload
        internally.

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid`` to export.

        Returns
        -------
        dict[str, Any]
            The exported dashboard object on success, or ``{"error": "<message>"}``
            when the HTTP call fails, the body is not valid JSON, or the payload
            is not a non-empty list as expected.
        """
        response = self.api_client.get(f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true")
        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to export dashboard '{dashboard_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            data = response.json()
        except Exception:
            error_msg = f"Failed to parse export response for dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}

        if not data or not isinstance(data, list):
            error_msg = f"Unexpected export response structure for dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}

        return data[0]

    def get_dashboard_widgets(self, dashboard_ref: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve widget definitions from an admin export of the dashboard.

        Uses ``export_dashboard`` (``GET /api/v1/dashboards/export`` with
        ``dashboardIds`` and ``adminAccess=true``), then reads the ``widgets``
        field from the first exported dashboard object—the same payload shape
        used by ``get_dashboard_script`` and ``get_widget_script``. If
        ``widgets`` is missing or empty, an empty list is returned.

        ``dashboard_ref`` is resolved with ``resolve_dashboard_reference`` so it
        may be either a 24-character dashboard ``oid`` or a dashboard title.

        Parameters
        ----------
        dashboard_ref : str
            Dashboard ``oid`` or title.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of widget objects on success (possibly empty). On failure,
            ``{"error": "..."}`` when the reference cannot be resolved or
            ``export_dashboard`` fails. If ``widgets`` is present but neither a
            list nor a mapping of widget objects, returns an error dict.
        """
        resolved = self.resolve_dashboard_reference(dashboard_ref)
        if not resolved.get("success"):
            err = resolved.get("error") or "Dashboard reference could not be resolved."
            self.logger.error(f"get_dashboard_widgets: {err}")
            return {"ok": False, "error": err}

        dashboard_id = resolved.get("dashboard_id")
        if not dashboard_id:
            msg = "Resolved dashboard had no identifier."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        self.logger.debug(f"Loading widgets via export_dashboard for dashboard '{dashboard_id}'")

        exported = self.export_dashboard(dashboard_id)
        if "error" in exported:
            self.logger.error(f"get_dashboard_widgets: export failed for '{dashboard_id}': {exported['error']}")
            return exported

        raw_widgets = exported.get("widgets") or []
        if isinstance(raw_widgets, dict):
            widgets = [w for w in raw_widgets.values() if isinstance(w, dict)]
        elif isinstance(raw_widgets, list):
            widgets = [w for w in raw_widgets if isinstance(w, dict)]
        else:
            msg = f"Unexpected widgets type in export for dashboard '{dashboard_id}'"
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        self.logger.info(f"Successfully retrieved {len(widgets)} widgets for dashboard '{dashboard_id}' from export.")
        return widgets

    def get_dashboards(self, fields: list[str] | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve dashboards visible to the authenticated user.

        Sends ``GET /api/v1/dashboards``, which returns dashboards the current
        user owns or has been shared to — as opposed to
        ``get_all_dashboards``, which uses the admin endpoint and requires
        elevated access.

        Parameters
        ----------
        fields : list[str], optional
            Subset of fields to include in the response (for example
            ``["oid", "title", "owner"]``). When omitted, all fields are
            returned.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            List of dashboard objects on success, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/v1/dashboards"
        params: dict[str, str] = {}
        if fields:
            params["fields"] = ",".join(fields)

        self.logger.debug("Fetching dashboards from standard endpoint")
        response = self.api_client.get(endpoint, params=params if params else None)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to fetch dashboards", self.api_client)
            self.logger.error(failure["error"])
            return failure

        dashboards = response.json()
        count = len(dashboards) if isinstance(dashboards, list) else 1
        self.logger.info(f"Retrieved {count} dashboard(s)")
        return dashboards

    def publish_dashboard(
        self,
        dashboard_id: str,
        *,
        admin_access: bool = True,
        force: bool = False,
    ) -> dict[str, Any]:
        """Publish (republish) a dashboard.

        Sends ``POST /api/v1/dashboards/{dashboard_id}/publish`` as the caller
        first. If that is refused with 403 and ``admin_access`` is true, the call
        is retried with ``adminAccess=true``, which some Sisense versions honour
        for an admin token that is not the owner; versions that reject the flag
        (422) yield the original 403, so the caller learns that only the owner
        can publish. Pass ``force=True`` to append ``force=true`` (used after
        script updates).

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid`` to publish.
        admin_access : bool, optional
            Retry with ``adminAccess=true`` when the plain call is refused. Default ``True``.
        force : bool, optional
            When ``True``, request with ``force=true``. Default is ``False``.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` or the JSON response body on success. On failure,
            the standard ``{"ok": False, "error": "...", ...}`` dict.
        """
        base = f"/api/v1/dashboards/{dashboard_id}/publish"
        force_part = ["force=true"] if force else []

        def attempt(extra: list[str]):
            parts = force_part + extra
            endpoint = base + ("?" + "&".join(parts) if parts else "")
            self.logger.debug(f"Publishing dashboard {dashboard_id} via {endpoint}")
            return self.api_client.post(endpoint)

        response = attempt([])
        if response is not None and response.status_code == 403 and admin_access:
            retry = attempt(["adminAccess=true"])
            # A 422 here means this Sisense version does not accept the flag; the 403 is the truth.
            if retry is not None and retry.status_code != 422:
                response = retry
        if response is None:
            failure = _extract_error_message(response, f"Failed to publish dashboard '{dashboard_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure
        if response.status_code in (200, 204):
            self.logger.info(f"Successfully published dashboard {dashboard_id}.")
            if response.status_code == 204 or not response.content:
                return {"success": True}
            try:
                return response.json()
            except Exception:
                return {"success": True}
        failure = _extract_error_message(response, f"Failed to publish dashboard '{dashboard_id}'", self.api_client)
        self.logger.error(failure["error"])
        return failure

    def _patch_dashboard_field(self, dashboard_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Send a partial dashboard update and normalize the response.

        Shared by ``rename_dashboard`` and ``move_dashboard_to_folder``,
        which differ only in the field(s) being patched.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard to update.
        payload : dict[str, Any]
            The fields to patch, e.g. ``{"title": "..."}``.

        Returns
        -------
        dict[str, Any]
            The updated dashboard object on success, or ``{"success": True}``
            when the API responds 200 with an empty body. ``{"error": "..."}``
            on failure.
        """
        endpoint = f"/api/dashboards/{dashboard_id}"
        self.logger.debug(f"Patching dashboard {dashboard_id} — fields: {list(payload.keys())}")
        response = self.api_client.patch(endpoint, data=payload)

        if response is None:
            self.logger.error(f"PATCH request to update dashboard {dashboard_id} failed: No response received.")
            return {"ok": False, "error": f"No response received while updating dashboard ID '{dashboard_id}'"}

        if response.status_code != 200:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text or "No response text available."
            self.logger.error(f"Failed to update dashboard {dashboard_id}. Error: {error_message}")
            return {"ok": False, "error": f"Failed to update dashboard '{dashboard_id}'. {error_message}"}

        updated = response.json() if response.content else {"success": True}
        self.logger.info(f"Successfully updated dashboard {dashboard_id} — fields: {list(payload.keys())}")
        return updated

    def rename_dashboard(self, dashboard_id: str, title: str) -> dict[str, Any]:
        """Rename a dashboard.

        Sends ``PATCH /api/dashboards/{dashboard_id}`` with only ``title`` in
        the request body. Other dashboard fields are not modified.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard to rename.
        title : str
            The new dashboard title.

        Returns
        -------
        dict[str, Any]
            The updated dashboard object on success, or ``{"success": True}``
            when the API responds 200 with an empty body. ``{"error": "..."}``
            on failure.
        """
        return self._patch_dashboard_field(dashboard_id, {"title": title})

    def move_dashboard_to_folder(self, dashboard_id: str, folder_id: str) -> dict[str, Any]:
        """Move a dashboard into a folder.

        Sends ``PATCH /api/dashboards/{dashboard_id}`` with only ``parentFolder``
        in the request body. Other dashboard fields are not modified.

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid`` to move.
        folder_id : str
            The target folder ``oid`` (``parentFolder`` value).

        Returns
        -------
        dict[str, Any]
            The updated dashboard object from the API, or ``{"success": True}``
            when the API responds 200 with an empty body. ``{"error": "..."}``
            on failure.
        """
        return self._patch_dashboard_field(dashboard_id, {"parentFolder": folder_id})

    def can_be_owned(self, dashboard_id: str) -> dict[str, Any]:
        """Check whether a dashboard can be owned by the current user.

        Sends ``GET /api/v1/dashboards/{dashboard_id}/can_be_owned``.

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid`` to check.

        Returns
        -------
        dict[str, Any]
            The API response on success, or ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/v1/dashboards/{dashboard_id}/can_be_owned"
        self.logger.debug(f"Checking ownership eligibility for dashboard {dashboard_id}")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"GET request for can_be_owned on dashboard {dashboard_id} failed: No response received.")
            return {"ok": False, "error": f"No response received while checking dashboard ID '{dashboard_id}'"}

        if response.status_code != 200:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text or "No response text available."
            self.logger.error(f"Failed can_be_owned check for dashboard {dashboard_id}. Error: {error_message}")
            return {"ok": False, "error": f"Failed can_be_owned check for dashboard '{dashboard_id}'. {error_message}"}

        result = response.json()
        self.logger.info(f"Successfully checked can_be_owned for dashboard {dashboard_id}.")
        return result

    def import_dashboards_bulk(self, dashboards: list[dict[str, Any]], action: str = "skip") -> dict[str, Any]:
        """Import one or more dashboards via the bulk import endpoint.

        Sends ``POST /api/v1/dashboards/import/bulk`` with ``action`` as a
        query parameter and ``dashboards`` as the request body. Each
        dashboard object is typically the payload returned by
        ``export_dashboard`` on another environment. The server matches
        dashboards by their ``oid``: when a dashboard with the same ``oid``
        already exists, ``action`` controls whether it is left unchanged,
        replaced, or a new copy is created alongside it.

        Parameters
        ----------
        dashboards : list[dict[str, Any]]
            Dashboard objects to import.
        action : str, optional
            Conflict behavior for dashboards whose ``oid`` already exists.
            One of ``"skip"``, ``"overwrite"``, or ``"duplicate"``. Default
            is ``"skip"``.

        Returns
        -------
        dict[str, Any]
            The API response body — including ``succeded`` and ``failed``
            lists describing the outcome for each dashboard — or
            ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/v1/dashboards/import/bulk?action={action}"
        self.logger.debug(f"Importing {len(dashboards)} dashboard(s) with action={action}")
        response = self.api_client.post(endpoint, data=dashboards)

        if response is None:
            self.logger.error("POST request to import dashboards failed: No response received.")
            return {"ok": False, "error": "No response received while importing dashboards."}

        if response.status_code not in (200, 201):
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text or "No response text available."
            self.logger.error(f"Failed to import dashboards. Error: {error_message}")
            return {"ok": False, "error": f"Failed to import dashboards. {error_message}"}

        result = response.json() if response.content else {"succeded": [], "failed": []}
        self.logger.info(f"Dashboard import request completed for {len(dashboards)} dashboard(s).")
        return result

    def _resolve_datasource_title(self, datamodel: str) -> str:
        """Return the datasource title for a data model reference (title or model oid).

        Dashboards reference their datasource by title, so an oid is looked up via
        ``GET /api/v2/datamodels/{oid}/schema``; anything that does not resolve is
        used as a title as given.
        """
        response = self.api_client.get(f"/api/v2/datamodels/{datamodel}/schema")
        if response is not None and response.status_code == 200:
            try:
                title = response.json().get("title")
                if isinstance(title, str) and title.strip():
                    return title
            except Exception:
                self.logger.debug(f"Could not parse the schema response while resolving '{datamodel}' to a title.")
        return datamodel

    def get_dashboards_by_datasource(self, datamodel: str, deep: bool = False) -> list[dict[str, Any]] | dict[str, Any]:
        """Find every dashboard that uses a data model, including dashboards linked only through a widget.

        Reads the admin dashboard listing once and matches each dashboard two
        ways: its own ``datasource`` (match ``"dashboard"``), or any datasource in
        its ``widgetsDatasources`` summary (match ``"widget"``) — a dashboard built
        on model A with one widget on model B is found for both models. Titles
        are compared case-insensitively. With ``deep=True``, dashboards whose
        ``widgetsDatasources`` summary is empty are exported in batches and their
        widgets inspected directly, closing the one gap the listing leaves.

        Parameters
        ----------
        datamodel : str
            The data model, as an ID or title.
        deep : bool, optional
            Also export dashboards with an empty widget-datasource summary and inspect their
            widgets. Slower (one export call per 20 such dashboards). Default ``False``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            One row per matching dashboard: ``dashboard_id``, ``title``, ``owner`` (user id),
            ``owner_email``, ``datasource_title`` (the dashboard's own datasource),
            ``match`` (``"dashboard"`` or ``"widget"``), ``folder_id`` and ``last_updated``.
            An empty list means no dashboard uses the model. On failure, the standard
            ``{"ok": False, "error": "...", ...}`` dict.
        """
        if not isinstance(datamodel, str) or not datamodel.strip():
            failure = {"ok": False, "error": "datamodel is required (title or oid)."}
            self.logger.error(failure["error"])
            return failure
        wanted_title = self._resolve_datasource_title(datamodel.strip())
        self.logger.debug(f"Discovering dashboards on datasource '{wanted_title}' (deep={deep})")
        discovered = _discover_dashboards_on_datasource(self.api_client, self.logger, wanted_title, deep=deep)
        if discovered.get("ok") is False:
            self.logger.error(discovered["error"])
            return discovered
        matches: dict[str, str] = discovered["matches"]
        dashboards: dict[str, dict[str, Any]] = discovered["dashboards"]

        owner_emails: dict[str, str] = {}
        if matches:
            users = self.api_client.get("/api/v1/users")
            if users is not None and users.status_code == 200:
                try:
                    owner_emails = {u["_id"]: u.get("email") for u in users.json() if isinstance(u, dict) and u.get("_id")}
                except Exception:
                    self.logger.debug("Could not parse the user list while resolving dashboard owners.")

        rows = []
        for oid, match in matches.items():
            entry = dashboards.get(oid) or {}
            rows.append(
                {
                    "dashboard_id": oid,
                    "title": entry.get("title"),
                    "owner": entry.get("owner"),
                    "owner_email": owner_emails.get(entry.get("owner")),
                    "datasource_title": (entry.get("datasource") or {}).get("title") if isinstance(entry.get("datasource"), dict) else None,
                    "match": match,
                    "folder_id": entry.get("parentFolder"),
                    "last_updated": entry.get("lastUpdated"),
                }
            )
        rows.sort(key=lambda r: (r["match"] != "dashboard", (r["title"] or "").lower()))
        self.logger.info(f"Found {len(rows)} dashboards on datasource '{wanted_title}' ({sum(r['match'] == 'widget' for r in rows)} via widgets only)")
        return rows

    def duplicate_dashboard(self, dashboard: str) -> dict[str, Any]:
        """Create a copy of a dashboard, titled with a marker so copies are easy to find.

        Exports the dashboard and imports it with Sisense's ``duplicate`` action
        (``POST /api/v1/dashboards/import/bulk?action=duplicate``), which creates
        a new dashboard (new id) carrying the widgets, filters, hierarchies and
        shares of the original. The copy is titled ``<original title>_perspective_stage``
        so copies made for testing stand out in the dashboard list and are easy to
        find and remove later; rename it afterwards for a different name. The copy
        lands at the root folder. The original is not modified.

        Parameters
        ----------
        dashboard : str
            The dashboard to copy, as an ID or title.

        Returns
        -------
        dict[str, Any]
            ``{"success": True, "dashboard_id", "title", "source_dashboard_id", "source_title",
            "widget_count"}`` for the new copy. On failure (source not found, export or import
            failed, or the import reported the dashboard as failed), the standard
            ``{"ok": False, "error": "...", ...}`` dict.
        """
        ref = self.resolve_dashboard_reference(dashboard)
        if not ref.get("success"):
            failure = {"ok": False, "error": f"Dashboard '{dashboard}' could not be resolved: {ref.get('error') or 'not found'}", "status_code": ref.get("status_code")}
            self.logger.error(failure["error"])
            return failure
        source_id = ref["dashboard_id"]

        exported = self.export_dashboard(source_id)
        if not isinstance(exported, dict) or exported.get("ok") is False or ("error" in exported and "title" not in exported):
            return exported if isinstance(exported, dict) else {"ok": False, "error": f"Unexpected export result for dashboard '{source_id}'."}
        source_title = exported.get("title") or ref.get("dashboard_title") or ""
        copy_title = f"{source_title}_perspective_stage"

        copy = dict(exported)
        copy["title"] = copy_title
        self.logger.debug(f"Duplicating dashboard '{source_title}' ({source_id}) as '{copy_title}'")
        result = self.import_dashboards_bulk([copy], action="duplicate")
        if not isinstance(result, dict) or result.get("ok") is False:
            return result if isinstance(result, dict) else {"ok": False, "error": "Unexpected import result while duplicating the dashboard."}
        succeeded = [d for d in (result.get("succeded") or result.get("succeeded") or []) if isinstance(d, dict)]
        created = next((d for d in succeeded if isinstance(d.get("oid"), str) and d["oid"] != source_id), None)
        if created is None:
            failed = result.get("failed") or []
            failure = {"ok": False, "error": f"Duplicating dashboard '{source_title}' produced no new dashboard." + (f" Import reported: {failed}" if failed else "")}
            self.logger.error(failure["error"])
            return failure

        widget_count = len(exported.get("widgets") or [])  # the import response lists widgets separately
        self.logger.info(f"Duplicated dashboard '{source_title}' ({source_id}) as '{copy_title}' ({created['oid']}) with {widget_count} widgets")
        return {
            "success": True,
            "dashboard_id": created["oid"],
            "title": created.get("title") or copy_title,
            "source_dashboard_id": source_id,
            "source_title": source_title,
            "widget_count": widget_count,
        }

    def delete_dashboard(self, dashboard_id: str, title: str) -> dict[str, Any]:
        """Delete a dashboard, but only if both its ID and its title match.

        Sends ``DELETE /api/v1/dashboards/{dashboard_id}`` after reading the
        dashboard and checking that its stored title equals ``title`` exactly.
        Requiring both is a deliberate safety catch: a wrong or stale id, or a
        dashboard renamed since it was listed, is refused instead of deleted.

        Parameters
        ----------
        dashboard_id : str
            The dashboard's 24-character ``oid``.
        title : str
            The dashboard's exact current title.

        Returns
        -------
        dict[str, Any]
            ``{"success": True, "message": "...", "dashboard_id", "title", "owner"}`` on success. On
            failure (not found, title mismatch, or an API error), the standard
            ``{"ok": False, "error": "...", ...}`` dict.
        """
        if not isinstance(dashboard_id, str) or not re.fullmatch(r"[0-9a-fA-F]{24}", dashboard_id.strip()):
            return self._fail("dashboard_id must be the dashboard's 24-character oid.")
        if not isinstance(title, str) or not title.strip():
            return self._fail("title is required and must match the dashboard's current title exactly.")
        dashboard_id = dashboard_id.strip()
        doc = self._dashboard_document(dashboard_id)
        if doc is None:
            return self._fail(f"Dashboard '{dashboard_id}' not found.", status_code=404)
        stored = doc.get("title") if isinstance(doc.get("title"), str) else ""
        if stored.strip() != title.strip():
            return self._fail(f"Refusing to delete dashboard '{dashboard_id}': its title is '{stored}', not '{title}'.")

        self.logger.debug(f"Deleting dashboard '{stored}' ({dashboard_id})")
        response = self.api_client.delete(f"/api/v1/dashboards/{dashboard_id}")
        if response is None or response.status_code not in (200, 204):
            failure = _extract_error_message(response, f"Failed to delete dashboard '{stored}'", self.api_client)
            self.logger.error(failure["error"])
            return failure
        owner = self._owner_email(doc.get("owner")) or doc.get("owner")
        self.logger.info(f"Deleted dashboard '{stored}' ({dashboard_id}), owner {owner}")
        return {"success": True, "message": f"Dashboard '{stored}' deleted.", "dashboard_id": dashboard_id, "title": stored, "owner": owner}

    _NON_QUERY_WIDGET_TYPES = {"richtexteditor", "widgetstabber", "filter"}

    @staticmethod
    def _jaql_panel(panel_name: str, jaql: dict[str, Any]) -> str:
        """Map a widget slot name to the panel name the JAQL endpoint understands."""
        return _jaql_panel(panel_name, jaql)

    def _widget_query(self, widget: dict[str, Any], dashboard: dict[str, Any], datasource: dict[str, Any], replaced_title: str | None) -> list[dict[str, Any]]:
        """Build the metadata list a widget's query needs: its own items plus the dashboard filters that apply to it."""
        return _widget_query_metadata(widget, dashboard, datasource, replaced_title)

    def _available_fields(self, datasource_title: str) -> set[tuple[str, str]] | None:
        """Lower-cased ``(table, column)`` pairs a datasource exposes: a perspective's kept columns, or all of a model's.

        Returns ``None`` when the datasource cannot be resolved to a model schema.
        """
        wanted = _datasource_title(datasource_title)
        perspective = None
        response = self.api_client.get("/api/v2/perspectives")
        if response is not None and response.status_code == 200:
            try:
                perspective = next((p for p in response.json() if isinstance(p, dict) and isinstance(p.get("name"), str) and p["name"].strip().lower() == wanted and p.get("parentOid")), None)
            except Exception:
                perspective = None
        model_oid = perspective.get("datamodelOid") if perspective else None
        if model_oid is None:
            listing = self.api_client.get("/api/v2/datamodels/schema", params={"title": datasource_title})
            if listing is None or listing.status_code != 200:
                return None
            try:
                body = listing.json()
                model = body if isinstance(body, dict) else next((m for m in body if isinstance(m, dict) and m.get("oid")), None)
            except Exception:
                return None
            model_oid = model.get("oid") if isinstance(model, dict) else None
        if not isinstance(model_oid, str):
            return None
        schema = self.api_client.get(f"/api/v2/datamodels/{model_oid}/schema")
        if schema is None or schema.status_code != 200:
            return None
        try:
            index = _build_schema_index(schema.json())
        except Exception:
            return None
        fields: set[tuple[str, str]] = set()
        if perspective is None:
            for table in index["tables"].values():
                for column in table["columns"].values():
                    if isinstance(table.get("name"), str) and isinstance(column.get("name"), str):
                        fields.add((table["name"].strip().lower(), column["name"].strip().lower()))
            return fields
        for entry in perspective.get("tables") or []:
            if not isinstance(entry, dict) or entry.get("diffType") == "exclude":
                continue
            table = index["tables"].get(entry.get("oid"))
            if table is None or not isinstance(table.get("name"), str):
                continue
            for diff in entry.get("columnsDiff") or []:
                if isinstance(diff, dict) and diff.get("enabled", True):
                    column = table["columns"].get(diff.get("oid"))
                    if column is not None and isinstance(column.get("name"), str):
                        fields.add((table["name"].strip().lower(), column["name"].strip().lower()))
        return fields

    @staticmethod
    def _missing_fields(metadata: list[dict[str, Any]], available: set[tuple[str, str]]) -> list[str]:
        """Dims in a widget query whose (table, column) is not among ``available``."""
        missing: list[str] = []
        for entry in metadata:
            for node, _ds, _path in _iter_dim_nodes(entry.get("jaql"), None, "$"):
                reference = _reference_from_jaql(node)
                if reference is None:
                    continue
                table, column = reference
                # date dims carry a " (Calendar)" suffix the column itself does not have
                if not any((table.strip().lower(), variant.strip().lower()) in available for variant in _column_name_variants(column)):
                    dim = node.get("dim") if isinstance(node.get("dim"), str) else f"[{table}.{column}]"
                    if dim not in missing:
                        missing.append(dim)
        return missing

    def validate_dashboard_queries(self, dashboard: str, datasource: str | None = None) -> dict[str, Any]:
        """Run every widget's query and report which widgets answer, fail, or cannot be queried.

        Reads the dashboard's widgets and filters, builds each widget's query the
        way the widget itself does — its own fields plus the dashboard filters that
        apply to it, honouring a widget's "ignore dashboard filters" settings — and
        runs it through ``POST /api/datasources/{name}/jaql`` with a row count of
        one. Nothing on the dashboard is modified. With ``datasource`` given, widgets
        and filters that use the dashboard's own datasource are run against that
        datasource instead, which answers "would this dashboard still work on that
        model or perspective" without changing anything. Before running, each
        widget's fields are checked against what that datasource exposes — a
        perspective's kept columns, or a model's columns — and a widget that
        references a missing field is reported ``"failed"`` with the missing dims,
        since the query engine does not answer for such a query.

        Parameters
        ----------
        dashboard : str
            The dashboard, as an ID or title.
        datasource : str | None, optional
            Title of a data model or perspective to run the queries against in place of the
            dashboard's own datasource. Default: each widget runs against its own datasource.

        Returns
        -------
        dict[str, Any]
            ``{"dashboard_id", "title", "datasource", "all_passed", "counts": {"ok", "failed",
            "unreachable", "skipped"}, "widgets": [...]}``. Each widget entry carries ``widget_id``,
            ``title``, ``type``, ``datasource``, ``status`` — ``"ok"`` (answered), ``"failed"``
            (Sisense returned an error, in ``error``), ``"unreachable"`` (no answer within the client's
            timeout, in ``error``) or ``"skipped"`` (nothing to query, reason in ``error``) — and
            ``seconds``. ``all_passed`` is true when no widget failed or was unreachable. On failure
            to read the dashboard or resolve ``datasource``, the standard ``{"ok": False, "error": "...",
            ...}`` dict.
        """
        ref = self.resolve_dashboard_reference(dashboard)
        if not ref.get("success"):
            return self._fail(f"Dashboard '{dashboard}' could not be resolved: {ref.get('error') or 'not found'}", status_code=ref.get("status_code"))
        dashboard_id = ref["dashboard_id"]
        exported = self.export_dashboard(dashboard_id)
        if not isinstance(exported, dict) or exported.get("ok") is False or ("error" in exported and "title" not in exported):
            return exported if isinstance(exported, dict) else {"ok": False, "error": f"Unexpected export result for dashboard '{dashboard_id}'."}
        title = exported.get("title")
        own_ds = exported.get("datasource") if isinstance(exported.get("datasource"), dict) else {}
        target: dict[str, Any] | None = None
        replaced_title: str | None = None
        if datasource is not None:
            target = self._datasource_object(datasource)
            if target is None:
                return self._fail(f"Datasource '{datasource}' not found: it is neither a data model nor a perspective on this instance.")
            replaced_title = _datasource_title(own_ds)

        available = self._available_fields(datasource) if datasource is not None else None
        if datasource is not None and available is None:
            self.logger.debug(f"Could not read the fields of '{datasource}'; widgets will be validated by running their queries only.")

        queries = Queries(api_client=self.api_client)
        results: list[dict[str, Any]] = []
        for widget in exported.get("widgets") or []:
            if not isinstance(widget, dict):
                continue
            widget_ds = widget.get("datasource") if isinstance(widget.get("datasource"), dict) else own_ds
            run_ds = target if (target is not None and _datasource_title(widget_ds) == replaced_title) else widget_ds
            entry: dict[str, Any] = {
                "widget_id": widget.get("oid"),
                "title": widget.get("title") or "",
                "type": widget.get("type"),
                "datasource": (run_ds or {}).get("title"),
                "status": "skipped",
                "error": None,
                "seconds": 0.0,
            }
            widget_type = str(widget.get("type") or "").lower()
            metadata = [] if widget_type in self._NON_QUERY_WIDGET_TYPES else self._widget_query(widget, exported, run_ds or {}, replaced_title if run_ds is target else None)
            if not any(m["panel"] != "scope" for m in metadata):
                entry["error"] = "widget has no fields to query" if widget_type not in self._NON_QUERY_WIDGET_TYPES else f"{widget.get('type')} widgets do not query data"
                results.append(entry)
                continue
            if available is not None and run_ds is target:
                missing = self._missing_fields(metadata, available)
                if missing:
                    # Asking Sisense would only stall: the query engine does not answer for a field the
                    # perspective does not expose. Report the gap the way the dashboard would show it.
                    entry["status"] = "failed"
                    entry["error"] = f"not found in '{datasource}': " + ", ".join(missing)
                    results.append(entry)
                    continue
            body = {"datasource": run_ds, "metadata": metadata, "count": 1, "offset": 0, "format": "json"}
            started = time.time()
            response = queries.elasticube_run_jaql_query((run_ds or {}).get("title") or "", body)
            entry["seconds"] = round(time.time() - started, 1)
            if isinstance(response, dict) and (response.get("ok") is False or "error" in response):
                entry["status"] = "failed" if response.get("status_code") is not None else "unreachable"
                entry["error"] = response.get("error")
            else:
                entry["status"] = "ok"
            self.logger.debug(f"validate_dashboard_queries: widget {entry['widget_id']} ({entry['type']}) -> {entry['status']} in {entry['seconds']}s")
            results.append(entry)

        counts = {status: sum(1 for r in results if r["status"] == status) for status in ("ok", "failed", "unreachable", "skipped")}
        all_passed = counts["failed"] == 0 and counts["unreachable"] == 0
        self.logger.info(f"Validated dashboard '{title}' ({dashboard_id}) against '{(target or own_ds).get('title')}': {counts}")
        return {"dashboard_id": dashboard_id, "title": title, "datasource": (target or own_ds).get("title"), "all_passed": all_passed, "counts": counts, "widgets": results}

    _COMPARE_ROW_LIMIT = 1000

    def compare_dashboard_values(self, dashboard: str, datasource_a: str, datasource_b: str) -> dict[str, Any]:
        """Run every widget's query against two datasources and report whether the values match.

        Reads the dashboard's widgets and filters, builds each widget's query the
        way the widget itself does — its own fields plus the dashboard filters that
        apply to it, honouring a widget's "ignore dashboard filters" settings — and
        runs it through ``POST /api/datasources/{name}/jaql`` once against
        ``datasource_a`` and once against ``datasource_b``, up to 1000 rows each,
        then compares the two result sets row for row, ignoring row order. This
        answers "does the dashboard show the same numbers on both": a perspective
        against its root model, or one model against another. Nothing on the
        dashboard is modified. Before querying, each widget's fields are checked
        against what each datasource exposes — a perspective's kept columns, or a
        model's columns — and a widget that references a missing field is reported
        ``"error"`` with the missing dims, since the query engine does not answer
        for such a query. Widgets on another datasource than the dashboard's own,
        BloX widgets and widgets with nothing to query are ``"skipped"``.

        Parameters
        ----------
        dashboard : str
            The dashboard, as an ID or title.
        datasource_a : str
            Title of the first data model or perspective, the reference.
        datasource_b : str
            Title of the second data model or perspective, the one under test.

        Returns
        -------
        dict[str, Any]
            ``{"dashboard_id", "title", "datasource_a", "datasource_b", "all_match", "compared",
            "skipped", "counts": {"match", "mismatch", "error", "skipped"}, "widgets": [...]}``. Each
            widget entry carries ``widget_id``, ``title``, ``type``, ``status`` — ``"match"``,
            ``"mismatch"``, ``"error"`` (a query failed, stalled or names a field a datasource lacks;
            detail in ``error``) or ``"skipped"`` (reason in ``error``) — ``rows_a``, ``rows_b`` (row
            counts returned by each datasource, ``None`` when not queried) and ``seconds``. ``all_match``
            is true only when at least one widget was compared and none is ``"mismatch"`` or
            ``"error"``. On failure to read the dashboard or resolve a datasource, the standard
            ``{"ok": False, "error": "...", ...}`` dict.
        """
        ref = self.resolve_dashboard_reference(dashboard)
        if not ref.get("success"):
            return self._fail(f"Dashboard '{dashboard}' could not be resolved: {ref.get('error') or 'not found'}", status_code=ref.get("status_code"))
        dashboard_id = ref["dashboard_id"]
        exported = self.export_dashboard(dashboard_id)
        if not isinstance(exported, dict) or exported.get("ok") is False or ("error" in exported and "title" not in exported):
            return exported if isinstance(exported, dict) else {"ok": False, "error": f"Unexpected export result for dashboard '{dashboard_id}'."}
        title = exported.get("title")
        own_ds = exported.get("datasource") if isinstance(exported.get("datasource"), dict) else {}
        own_title = _datasource_title(own_ds)

        sides: list[tuple[str, dict[str, Any], set[tuple[str, str]] | None]] = []
        for wanted in (datasource_a, datasource_b):
            resolved = self._datasource_object(wanted)
            if resolved is None:
                return self._fail(f"Datasource '{wanted}' not found: it is neither a data model nor a perspective on this instance.")
            available = self._available_fields(wanted)
            if available is None:
                self.logger.debug(f"Could not read the fields of '{wanted}'; widgets will be compared by running their queries only.")
            sides.append((wanted, resolved, available))

        queries = Queries(api_client=self.api_client)

        def run(ds: dict[str, Any], metadata: list[dict[str, Any]]) -> tuple[Any, str | None]:
            body = {"datasource": ds, "metadata": metadata, "count": self._COMPARE_ROW_LIMIT, "offset": 0, "format": "json"}
            response = queries.elasticube_run_jaql_query(ds.get("title") or "", body)
            if isinstance(response, dict) and (response.get("ok") is False or "error" in response):
                return None, f"query on '{ds.get('title')}' failed: {response.get('error')}"
            values = response.get("values") if isinstance(response, dict) else None
            if not isinstance(values, list):
                return None, f"query on '{ds.get('title')}' returned no values"
            return values, None

        results: list[dict[str, Any]] = []
        for widget in exported.get("widgets") or []:
            if not isinstance(widget, dict):
                continue
            entry: dict[str, Any] = {
                "widget_id": widget.get("oid"),
                "title": widget.get("title") or "",
                "type": widget.get("type"),
                "status": "skipped",
                "rows_a": None,
                "rows_b": None,
                "error": None,
                "seconds": 0.0,
            }
            widget_type = str(widget.get("type") or "").lower()
            widget_ds = widget.get("datasource") if isinstance(widget.get("datasource"), dict) else own_ds
            if widget_type in self._NON_QUERY_WIDGET_TYPES:
                entry["error"] = f"{widget.get('type')} widgets do not query data"
            elif widget_type == "blox":
                entry["error"] = "BloX widgets query through their own actions and templates; not compared"
            elif _datasource_title(widget_ds) != own_title:
                entry["error"] = f"widget queries '{(widget_ds or {}).get('title')}', not the dashboard's datasource"
            if entry["error"] is not None:
                results.append(entry)
                continue
            started = time.time()
            values_by_side: list[Any] = []
            for label, (wanted, ds, available) in zip(("a", "b"), sides, strict=True):
                metadata = self._widget_query(widget, exported, ds, own_title)
                if not any(m["panel"] != "scope" for m in metadata):
                    entry["error"] = "widget has no fields to query"
                    break
                if available is not None:
                    missing = self._missing_fields(metadata, available)
                    if missing:
                        entry["status"] = "error"
                        entry["error"] = f"not found in '{wanted}': " + ", ".join(missing)
                        break
                values, error = run(ds, metadata)
                if error is not None:
                    entry["status"] = "error"
                    entry["error"] = error
                    break
                entry[f"rows_{label}"] = len(values)
                values_by_side.append(values)
            entry["seconds"] = round(time.time() - started, 1)
            if entry["error"] is None:
                normalised = [sorted(json.dumps(row, sort_keys=True, default=str) for row in values) for values in values_by_side]
                entry["status"] = "match" if normalised[0] == normalised[1] else "mismatch"
            self.logger.debug(f"compare_dashboard_values: widget {entry['widget_id']} ({entry['type']}) -> {entry['status']} in {entry['seconds']}s")
            results.append(entry)

        counts = {status: sum(1 for r in results if r["status"] == status) for status in ("match", "mismatch", "error", "skipped")}
        compared = counts["match"] + counts["mismatch"]
        all_match = compared > 0 and counts["mismatch"] == 0 and counts["error"] == 0
        self.logger.info(f"Compared dashboard '{title}' ({dashboard_id}) on '{datasource_a}' and '{datasource_b}': {counts}, all_match={all_match}")
        return {
            "dashboard_id": dashboard_id,
            "title": title,
            "datasource_a": sides[0][1].get("title"),
            "datasource_b": sides[1][1].get("title"),
            "all_match": all_match,
            "compared": compared,
            "skipped": counts["skipped"],
            "counts": counts,
            "widgets": results,
        }

    def _datasource_object(self, title: str) -> dict[str, Any] | None:
        """Build the datasource object Sisense expects for a perspective or data model title.

        Checks the perspectives list first: a perspective is addressed through its
        root model's catalogue entry — live parents give ``{"title", "id": "live:<name>",
        "fullname": "live:<name>", "live": True}``; ElastiCube parents keep the parent's
        ``id``, ``database`` and ``address`` with the perspective's own title and
        ``<address>/<name>`` fullname. Anything else is looked up in
        ``GET /api/datasources`` and used as listed. Returns ``None`` when nothing matches.
        """
        wanted = _datasource_title(title)
        catalogue: dict[str, dict[str, Any]] = {}
        response = self.api_client.get("/api/datasources")
        if response is not None and response.status_code == 200:
            try:
                for entry in response.json() or []:
                    if isinstance(entry, dict) and isinstance(entry.get("title"), str):
                        catalogue.setdefault(entry["title"].strip().lower(), entry)
            except Exception:
                self.logger.debug("Could not parse the datasource catalogue.")

        perspective = None
        perspectives = self.api_client.get("/api/v2/perspectives")
        if perspectives is not None and perspectives.status_code == 200:
            try:
                perspective = next((p for p in perspectives.json() if isinstance(p, dict) and isinstance(p.get("name"), str) and p["name"].strip().lower() == wanted and p.get("parentOid")), None)
            except Exception:
                self.logger.debug("Could not parse the perspectives list.")
        if perspective is not None:
            parent_title = self._resolve_datasource_title(perspective["datamodelOid"]) if isinstance(perspective.get("datamodelOid"), str) else None
            parent = catalogue.get(_datasource_title(parent_title) or "") if parent_title else None
            if parent is None:
                return None
            name = perspective["name"]
            if parent.get("live"):
                return {"title": name, "id": f"live:{name}", "fullname": f"live:{name}", "live": True}
            address = parent.get("address") or "LocalHost"
            return {"fullname": f"{address}/{name}", "id": parent.get("id"), "address": address, "database": parent.get("database"), "live": False, "title": name}
        return dict(catalogue[wanted]) if wanted in catalogue else None

    def _dashboard_document(self, dashboard_id: str) -> dict[str, Any] | None:
        """Return the dashboard document (dict) for an id, or ``None``."""
        doc = self.get_dashboard_by_id(dashboard_id)
        if isinstance(doc, list):
            doc = doc[0] if doc and isinstance(doc[0], dict) else None
        return doc if isinstance(doc, dict) and "error" not in doc else None

    def replace_datasource(self, dashboard: str, datasource: str, from_datasource: str | None = None, publish: bool = True, act_as_owner: bool = False) -> dict[str, Any]:
        """Change the datasource a dashboard queries — for example from a data model to a perspective built over it.

        Sends ``POST /api/v1/dashboards/{server}/{old title}/replace_datasource?dashboardId=...``
        with the new datasource object; Sisense rewrites the dashboard and every widget
        and filter that used the old datasource, leaving widgets on other datasources
        alone. The old datasource defaults to the dashboard's own; pass ``from_datasource``
        to change one that only some widgets use.

        With Dashboard Co-Authoring enabled (system setting ``dashboardCoAuthoring``), a
        published dashboard exists as a shared copy — what viewers see, read with
        ``sharedMode=true`` — and a private copy per owner, and publishing flows from the
        shared copy to the private ones. The method therefore compares against and writes
        the shared copy first (``sharedMode=true``), verifies that the shared dashboard and
        every widget carry the new datasource, then writes the owner's private copy so the
        owner's own view matches, publishes, and verifies the shared copy again. A dashboard
        that was never published has a single copy and is written once. With the feature
        off, the single copy is written and published as before.

        Only the dashboard's owner can read or write the shared copy and publish. When the
        token's user is not the owner the method refuses before writing anything and names
        the owner and co-owners — unless ``act_as_owner`` is true and the token belongs to an
        administrator, in which case ownership is transferred to the token's user for the
        duration of the change (``POST /api/v1/dashboards/{id}/change_owner``), the change is
        made as owner, and ownership and the exact share list are restored afterwards, even
        when the change or the publish fails. The private copy written in that case is the
        token user's; the original owner's private copy keeps the old datasource until they
        open or restore the dashboard, while the shared copy viewers see carries the change.
        A temporary co-owner share was tried first and is not available to a non-owner: the
        share endpoint refuses it without ``adminAccess=true``, and with it a share of rule
        ``"owner"`` makes that user the owner outright and drops the previous owner, which is
        why the transfer-and-restore route is used instead.

        Parameters
        ----------
        dashboard : str
            The dashboard, as an ID or title.
        datasource : str
            Title of the new datasource: a data model or a perspective.
        from_datasource : str | None, optional
            Title of the datasource being replaced. Default: the dashboard's own datasource.
        publish : bool, optional
            Republish the dashboard after the change so shared viewers see it. Default ``True``.
        act_as_owner : bool, optional
            When the token's user is an administrator but not the owner, take ownership
            temporarily to make the change, then hand it back. Default ``False``: refuse instead.

        Returns
        -------
        dict[str, Any]
            ``{"success": True, "dashboard_id", "title", "previous_datasource",
            "previous_datasource_title", "new_datasource", "widgets_updated", "widgets_unchanged",
            "published", "co_authoring", "shared_copy_updated", "private_copy_updated",
            "ownership_transferred_temporarily"}`` — returned only once the read-back shows the new
            datasource on every copy written. ``previous_datasource`` is the full old object and
            ``previous_datasource_title`` its title, so the change can be reverted with another call.
            ``shared_copy_updated`` is ``None`` when the dashboard has no shared copy (co-authoring
            off, or never published). ``published`` is true only when the publish succeeded and, where
            a shared copy exists, it was read back with the new datasource; otherwise ``False`` with
            ``publish_error``. When ownership was borrowed, ``original_owner`` (email, or id) is set
            and ``ownership_restore_error`` reports a failed hand-back. On failure — unknown
            dashboard or datasource, a token that is not the owner (``owner`` and ``co_owners`` name
            who is), a write that did not apply to a copy, or an API error — the standard
            ``{"ok": False, "error": "...", ...}`` dict, carrying ``shared_copy_updated`` and
            ``private_copy_updated`` when any write was attempted.
        """
        ref = self.resolve_dashboard_reference(dashboard)
        if not ref.get("success"):
            return self._fail(f"Dashboard '{dashboard}' could not be resolved: {ref.get('error') or 'not found'}", status_code=ref.get("status_code"))
        dashboard_id = ref["dashboard_id"]
        doc = self._dashboard_document(dashboard_id)
        if doc is None:
            return self._fail(f"Dashboard '{dashboard}' ({dashboard_id}) could not be read.")
        title = doc.get("title")
        target = self._datasource_object(datasource)
        if target is None:
            return self._fail(f"Datasource '{datasource}' not found: it is neither a data model nor a perspective on this instance.")
        co_authoring = self._co_authoring_enabled(dashboard_id)

        # Only the owner can write the shared copy and publish: settle ownership before touching anything.
        me = self._current_user()
        my_id = me.get("_id") if isinstance(me, dict) else None
        owner_id = doc.get("owner") if isinstance(doc.get("owner"), str) else None
        borrowed: dict[str, Any] | None = None
        if my_id and owner_id and my_id != owner_id:
            owner_label = self._owner_email(owner_id) or owner_id
            co_owners = self._co_owner_names(doc)
            if not act_as_owner:
                return self._fail_with(
                    f"Dashboard '{title}' is owned by {owner_label}; only the owner can change its datasource. Pass act_as_owner=True with an administrator token to take ownership temporarily.",
                    owner=owner_label,
                    co_owners=co_owners,
                )
            if not self._is_admin_user(me):
                return self._fail_with(f"Dashboard '{title}' is owned by {owner_label}; act_as_owner requires an administrator token.", owner=owner_label, co_owners=co_owners)
            borrowed = self._borrow_ownership(dashboard_id, title, doc, my_id)
            if borrowed.get("ok") is False:
                return borrowed
            doc = self._dashboard_document(dashboard_id) or doc

        restored: dict[str, Any] | None = None
        try:
            result = self._replace_datasource_as_owner(dashboard_id, title, doc, datasource, target, from_datasource, publish, co_authoring)
        finally:
            if borrowed is not None:
                restored = self._return_ownership(dashboard_id, title, borrowed)
        if borrowed is not None:
            result["ownership_transferred_temporarily"] = True
            result["original_owner"] = borrowed.get("owner_email") or borrowed.get("owner_id")
            if restored is not None and restored.get("ok") is False:
                result["ownership_restore_error"] = restored.get("error")
        elif result.get("success"):
            result["ownership_transferred_temporarily"] = False
        return result

    def _replace_datasource_as_owner(
        self,
        dashboard_id: str,
        title: Any,
        doc: dict[str, Any],
        datasource: str,
        target: dict[str, Any],
        from_datasource: str | None,
        publish: bool,
        co_authoring: bool,
    ) -> dict[str, Any]:
        """The datasource change proper, run with owner rights; see ``replace_datasource``."""
        updated: dict[str, bool | None] = {"shared": None, "private": None}

        def fail(message: str, **extra: Any) -> dict[str, Any]:
            failure = self._fail(message)
            failure.update({"co_authoring": co_authoring, "shared_copy_updated": updated["shared"], "private_copy_updated": updated["private"]})
            failure.update({k: v for k, v in extra.items() if v is not None})
            return failure

        # Under co-authoring the shared copy is what viewers see: compare against it, not the private copy.
        shared_doc: dict[str, Any] | None = None
        if co_authoring:
            status, shared_doc = self._shared_copy(dashboard_id)
            if shared_doc is None:
                owner_label = self._owner_email(doc.get("owner")) or doc.get("owner")
                return fail(f"The shared copy of dashboard '{title}' could not be read (HTTP {status}); only its owner ({owner_label}) can change it.", owner=owner_label)
        has_shared_copy = bool(shared_doc and shared_doc.get("lastPublish"))
        source = shared_doc if has_shared_copy else doc

        old = source.get("datasource") if isinstance(source.get("datasource"), dict) else {}
        if from_datasource is not None and _datasource_title(from_datasource) != _datasource_title(old):
            widgets = self._shared_widgets(dashboard_id) if has_shared_copy else self.get_dashboard_widgets(dashboard_id)
            old = (
                next(
                    (
                        w.get("datasource")
                        for w in (widgets if isinstance(widgets, list) else [])
                        if isinstance(w, dict) and _datasource_title(w.get("datasource")) == _datasource_title(from_datasource)
                    ),
                    None,
                )
                or {}
            )
            if not old:
                return fail(f"Dashboard '{title}' has no widget on datasource '{from_datasource}'.")
        old_title = old.get("title") if isinstance(old.get("title"), str) else from_datasource
        if not old_title:
            return fail(f"Dashboard '{title}' has no datasource to replace.")
        if _datasource_title(old_title) == _datasource_title(datasource):
            return fail(f"Dashboard '{title}' already uses datasource '{datasource}'.")

        server = "live" if old.get("live") else (old.get("address") or "LocalHost")
        endpoint = f"/api/v1/dashboards/{quote(str(server), safe='')}/{quote(old_title, safe='')}/replace_datasource?dashboardId={dashboard_id}"
        copies = [("shared", "&sharedMode=true"), ("private", "")] if has_shared_copy else [("private", "")]
        self.logger.debug(f"Replacing datasource '{old_title}' with '{datasource}' on dashboard '{title}' ({dashboard_id}); copies={[c for c, _ in copies]} co_authoring={co_authoring}")

        widgets: list[dict[str, Any]] = []
        for copy, suffix in copies:
            response = self.api_client.post(endpoint + suffix, data=target)
            if response is None or response.status_code not in (200, 201, 204):
                failure = _extract_error_message(response, f"Failed to replace datasource on the {copy} copy of dashboard '{title}'", self.api_client)
                failure.update({"co_authoring": co_authoring, "shared_copy_updated": updated["shared"], "private_copy_updated": updated["private"]})
                if response is not None and response.status_code == 403:
                    failure["owner"] = self._owner_email(doc.get("owner")) or doc.get("owner")
                self.logger.error(failure["error"])
                return failure
            applied, widgets = self._wait_for_datasource(dashboard_id, datasource, old_title, from_datasource is not None, shared=(copy == "shared"))
            updated[copy] = applied
            if not applied:
                return fail(
                    f"Sisense accepted the request but the {copy} copy of dashboard '{title}' still shows datasource '{old_title}'.", owner=self._owner_email(doc.get("owner")) or doc.get("owner")
                )

        widgets_updated = sum(1 for w in widgets if _datasource_title(w.get("datasource")) == _datasource_title(datasource))
        unchanged = sorted(
            {(w.get("datasource") or {}).get("title") for w in widgets if isinstance(w.get("datasource"), dict) and _datasource_title(w.get("datasource")) != _datasource_title(datasource)}
        )
        self.logger.info(f"Dashboard '{title}' ({dashboard_id}) now uses '{datasource}' instead of '{old_title}' ({widgets_updated} widgets; copies={[c for c, _ in copies]})")
        result: dict[str, Any] = {
            "success": True,
            "dashboard_id": dashboard_id,
            "title": title,
            "previous_datasource": old,
            "previous_datasource_title": old_title,
            "new_datasource": target,
            "widgets_updated": widgets_updated,
            "widgets_unchanged": [u for u in unchanged if u],
            "published": False,
            "co_authoring": co_authoring,
            "shared_copy_updated": updated["shared"],
            "private_copy_updated": updated["private"],
        }
        if publish:
            published = self.publish_dashboard(dashboard_id, admin_access=True)
            if isinstance(published, dict) and published.get("ok") is False:
                result["publish_error"] = published.get("error")
                if published.get("status_code") == 403:
                    result["owner"] = self._owner_email(doc.get("owner")) or doc.get("owner")
                self.logger.warning(f"Dashboard '{title}' was switched to '{datasource}' but could not be republished: {published.get('error')}")
            elif has_shared_copy:
                still_applied, _ = self._wait_for_datasource(dashboard_id, datasource, old_title, from_datasource is not None, shared=True)
                if not still_applied:
                    updated["shared"] = False
                    return fail(f"Dashboard '{title}' was published, but its shared copy no longer shows datasource '{datasource}'.")
                result["published"] = True
            else:
                result["published"] = True
        return result

    # ------------------------------------------------------------------ co-authoring and ownership helpers

    _ADMIN_ROLE_NAMES = {"admin", "super"}

    def _fail_with(self, message: str, **extra: Any) -> dict[str, Any]:
        failure = self._fail(message)
        failure.update({k: v for k, v in extra.items() if v is not None})
        return failure

    def _co_authoring_enabled(self, dashboard_id: str | None = None) -> bool:
        """Whether Dashboard Co-Authoring is on: the system setting, or — when unreadable — a shared-copy probe."""
        response = self.api_client.get("/api/v1/settings/system")
        if response is not None and response.status_code == 200:
            try:
                flag = ((response.json() or {}).get("dashboardCoAuthoring") or {}).get("enabled")
            except Exception:
                flag = None
            if isinstance(flag, bool):
                return flag
        if dashboard_id is None:
            return False
        # Unknown query parameters are rejected with a 4xx on some versions, so only a 200 counts as "on".
        probe = self.api_client.get(f"/api/v1/dashboards/{dashboard_id}?sharedMode=true")
        return probe is not None and probe.status_code == 200

    def _shared_copy(self, dashboard_id: str) -> tuple[int | None, dict[str, Any] | None]:
        """Read the shared copy (``GET /api/v1/dashboards/{id}?sharedMode=true``): (status, document or None)."""
        response = self.api_client.get(f"/api/v1/dashboards/{dashboard_id}?sharedMode=true")
        if response is None:
            return None, None
        if response.status_code != 200:
            return response.status_code, None
        try:
            body = response.json()
        except Exception:
            return response.status_code, None
        return 200, body if isinstance(body, dict) else None

    def _shared_widgets(self, dashboard_id: str) -> list[dict[str, Any]]:
        """The widgets of the shared copy (``GET /api/v1/dashboards/{id}/widgets?sharedMode=true``), or ``[]``."""
        response = self.api_client.get(f"/api/v1/dashboards/{dashboard_id}/widgets?sharedMode=true")
        if response is None or response.status_code != 200:
            return []
        try:
            body = response.json()
        except Exception:
            return []
        return [w for w in body if isinstance(w, dict)] if isinstance(body, list) else []

    def _current_user(self) -> dict[str, Any] | None:
        """The token's user (``GET /api/users/loggedin``), or ``None``."""
        response = self.api_client.get("/api/users/loggedin")
        if response is None or response.status_code != 200:
            return None
        try:
            body = response.json()
        except Exception:
            return None
        return body if isinstance(body, dict) and body.get("_id") else None

    def _is_admin_user(self, user: dict[str, Any] | None) -> bool:
        """Whether the user's role is an administrator role (``admin`` or ``super``)."""
        role_id = (user or {}).get("roleId")
        if not isinstance(role_id, str):
            return False
        response = self.api_client.get("/api/roles")
        if response is None or response.status_code != 200:
            return False
        try:
            return any(isinstance(r, dict) and r.get("_id") == role_id and str(r.get("name") or "").lower() in self._ADMIN_ROLE_NAMES for r in response.json())
        except Exception:
            return False

    def _co_owner_names(self, doc: dict[str, Any]) -> list[str]:
        """Emails of users and names of groups holding an edit share on the dashboard."""
        entries = [s for s in (doc.get("shares") or []) if isinstance(s, dict) and str(s.get("rule") or "").lower() in ("edit", "owner")]
        names: list[str] = []
        if any(s.get("type") == "user" for s in entries):
            users = self.api_client.get("/api/v1/users")
            emails: dict[str, str] = {}
            if users is not None and users.status_code == 200:
                try:
                    emails = {u["_id"]: u.get("email") for u in users.json() if isinstance(u, dict) and u.get("_id")}
                except Exception:
                    emails = {}
            names += [emails.get(s.get("shareId")) or str(s.get("shareId")) for s in entries if s.get("type") == "user"]
        if any(s.get("type") == "group" for s in entries):
            groups = self.api_client.get("/api/v1/groups")
            group_names: dict[str, str] = {}
            if groups is not None and groups.status_code == 200:
                try:
                    group_names = {g["_id"]: g.get("name") for g in groups.json() if isinstance(g, dict) and g.get("_id")}
                except Exception:
                    group_names = {}
            names += [f"group:{group_names.get(s.get('shareId')) or s.get('shareId')}" for s in entries if s.get("type") == "group"]
        return names

    def _borrow_ownership(self, dashboard_id: str, title: Any, doc: dict[str, Any], my_id: str) -> dict[str, Any]:
        """Take ownership of a dashboard temporarily; returns what is needed to give it back, or the error dict."""
        owner_id = doc.get("owner")
        shares = [
            {"shareId": s.get("shareId"), "type": s.get("type"), "rule": s.get("rule"), "subscribe": bool(s.get("subscribe"))}
            for s in (doc.get("shares") or [])
            if isinstance(s, dict) and s.get("shareId") and s.get("rule")  # the owner's own entry carries no rule
        ]
        self.logger.info(f"Taking ownership of dashboard '{title}' ({dashboard_id}) temporarily from {owner_id}")
        changed = self.change_dashboard_owner(dashboard_id, my_id, original_owner_rule="edit")
        if isinstance(changed, dict) and changed.get("ok") is False:
            return self._fail_with(f"Could not take ownership of dashboard '{title}' temporarily: {changed.get('error')}", owner=self._owner_email(owner_id) or owner_id)
        return {"owner_id": owner_id, "owner_email": self._owner_email(owner_id), "shares": shares}

    def _return_ownership(self, dashboard_id: str, title: Any, borrowed: dict[str, Any]) -> dict[str, Any]:
        """Give ownership back and restore the exact share list captured by ``_borrow_ownership``."""
        owner_id = borrowed.get("owner_id")
        changed = self.change_dashboard_owner(dashboard_id, owner_id, original_owner_rule="view")
        if isinstance(changed, dict) and changed.get("ok") is False:
            message = f"Could not return ownership of dashboard '{title}' to {borrowed.get('owner_email') or owner_id}: {changed.get('error')}"
            self.logger.error(message)
            return {"ok": False, "error": message}
        # change_owner leaves the caller with a view share; posting the original list (owner included, as rule
        # "owner") replaces the whole share list and removes it.
        body = {"sharesTo": [{"shareId": owner_id, "type": "user", "rule": "owner", "subscribe": False}] + list(borrowed.get("shares") or [])}
        response = self.api_client.post(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true", data=body)
        if response is None or response.status_code != 200:
            failure = _extract_error_message(
                response,
                f"Ownership of dashboard '{title}' was returned to {borrowed.get('owner_email') or owner_id}, but its share list could not be restored (the token's user keeps a view share)",
                self.api_client,
            )
            self.logger.error(failure["error"])
            return failure
        self.logger.info(f"Returned ownership of dashboard '{title}' ({dashboard_id}) to {borrowed.get('owner_email') or owner_id} and restored its shares")
        return {"success": True}

    def _wait_for_datasource(self, dashboard_id: str, datasource: str, old_title: str, widget_level: bool, shared: bool = False) -> tuple[bool, list[dict[str, Any]]]:
        """Poll one copy of the dashboard until it shows ``datasource`` and no widget is left on ``old_title``.

        For a dashboard-level change the dashboard's own datasource must have changed; for a
        widget-level change (``widget_level``) some widget must. Either way every widget that
        used ``old_title`` must have moved. ``shared`` reads the shared copy (``sharedMode=true``).
        """
        widgets: list[dict[str, Any]] = []
        for _ in range(self._SWAP_POLL_ATTEMPTS):
            time.sleep(self._SWAP_POLL_DELAY)
            if shared:
                _status, after = self._shared_copy(dashboard_id)
                widgets = self._shared_widgets(dashboard_id)
            else:
                after = self._dashboard_document(dashboard_id)
                fetched = self.get_dashboard_widgets(dashboard_id)
                widgets = [w for w in (fetched if isinstance(fetched, list) else []) if isinstance(w, dict)]
            on_widget = any(_datasource_title(w.get("datasource")) == _datasource_title(datasource) for w in widgets)
            on_dashboard = after is not None and _datasource_title(after.get("datasource")) == _datasource_title(datasource)
            left_behind = any(_datasource_title(w.get("datasource")) == _datasource_title(old_title) for w in widgets)
            if (on_widget if widget_level else on_dashboard) and not left_behind:
                return True, widgets
        return False, widgets

    def _owner_email(self, owner_id: Any) -> str | None:
        """Resolve a user id to an email via the user list, or ``None``."""
        if not isinstance(owner_id, str):
            return None
        response = self.api_client.get("/api/v1/users")
        if response is None or response.status_code != 200:
            return None
        try:
            return next((u.get("email") for u in response.json() if isinstance(u, dict) and u.get("_id") == owner_id), None)
        except Exception:
            return None

    def _fail(self, message: str, status_code: int | None = None) -> dict[str, Any]:
        """Log and return a standard failure dict."""
        failure: dict[str, Any] = {"ok": False, "error": message}
        if status_code is not None:
            failure["status_code"] = status_code
        self.logger.error(message)
        return failure
