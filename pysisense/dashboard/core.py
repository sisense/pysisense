from __future__ import annotations

import re
from typing import Any


class DashboardCoreMixin:
    def get_all_dashboards(self):
        """
        Retrieves all dashboards from the Sisense server.

        Returns:
            list or dict: A list of dashboards if successful,
                        or a dict containing an error message.
        """
        endpoint = "/api/v1/dashboards/admin?dashboardType=owner"
        self.logger.debug(f"Fetching all dashboards from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error("GET request to retrieve dashboards failed: No response received.")
            return {"error": "No response received from the server."}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve dashboards. Error: {error_message}")
            return {"error": f"Failed to retrieve dashboards. {error_message}"}

        dashboards = response.json()
        self.logger.info(f"Successfully retrieved {len(dashboards)} dashboards.")
        return dashboards

    def get_dashboard_by_id(self, dashboard_id):
        """
        Retrieves a specific dashboard by its ID.

        Parameters:
            dashboard_id (str): The ID of the dashboard to retrieve.

        Returns:
            dict: A dictionary containing dashboard details if found,
                or a dict with an error message if the request fails.
        """
        endpoint = f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}"
        self.logger.debug(f"Fetching dashboard with ID {dashboard_id} from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"GET request to retrieve dashboard {dashboard_id} failed: No response received.")
            return {"error": f"No response received while retrieving dashboard ID '{dashboard_id}'"}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve dashboard {dashboard_id}. Error: {error_message}")
            return {"error": f"Failed to retrieve dashboard '{dashboard_id}'. {error_message}"}

        dashboard_data = response.json()
        if not dashboard_data:
            self.logger.warning(f"No dashboard found with ID {dashboard_id}.")
            return {"error": f"No dashboard found with ID '{dashboard_id}'"}

        self.logger.info(f"Successfully retrieved dashboard with ID {dashboard_id}.")
        return dashboard_data

    def get_dashboard_by_name(self, dashboard_name):
        """
        Retrieves a specific dashboard by its name.

        Parameters:
            dashboard_name (str): The name of the dashboard to retrieve.

        Returns:
            dict or list: A dictionary containing dashboard details if found,
                        or {'error': 'message'} if not found or failed.
        """
        endpoint = f"/api/v1/dashboards/admin?dashboardType=owner&name={dashboard_name}"
        self.logger.debug(f"Fetching dashboard with name {dashboard_name} from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None:
            error_msg = f"GET request to retrieve dashboard {dashboard_name} failed: No response received."
            self.logger.error(error_msg)
            return {"error": error_msg}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve dashboard {dashboard_name}. Error: {error_message}")
            return {"error": f"Failed to retrieve dashboard '{dashboard_name}'. {error_message}"}

        dashboard_data = response.json()
        if not dashboard_data:
            warning_msg = f"No dashboard found with name {dashboard_name}."
            self.logger.warning(warning_msg)
            return {"error": warning_msg}

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
            error_msg = f"Failed to export dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return {"error": error_msg}

        try:
            data = response.json()
        except Exception:
            error_msg = f"Failed to parse export response for dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return {"error": error_msg}

        if not data or not isinstance(data, list):
            error_msg = f"Unexpected export response structure for dashboard '{dashboard_id}'"
            self.logger.error(error_msg)
            return {"error": error_msg}

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
            return {"error": err}

        dashboard_id = resolved.get("dashboard_id")
        if not dashboard_id:
            msg = "Resolved dashboard had no identifier."
            self.logger.error(msg)
            return {"error": msg}

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
            return {"error": msg}

        self.logger.info(f"Successfully retrieved {len(widgets)} widgets for dashboard '{dashboard_id}' from export.")
        return widgets

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
            The updated dashboard object from the API, or ``{"error": "..."}`` on
            failure.
        """
        return self._patch_dashboard(dashboard_id, {"parentFolder": folder_id})

    def rename_dashboard(self, dashboard_id: str, title: str) -> dict[str, Any]:
        """Rename a dashboard.

        Sends ``PATCH /api/dashboards/{dashboard_id}`` with only ``title`` in
        the request body. Other dashboard fields are not modified.

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid`` to rename.
        title : str
            The new dashboard title.

        Returns
        -------
        dict[str, Any]
            The updated dashboard object from the API, or ``{"error": "..."}`` on
            failure.
        """
        return self._patch_dashboard(dashboard_id, {"title": title})

    def publish_dashboard(
        self,
        dashboard_id: str,
        *,
        admin_access: bool = True,
        force: bool = False,
    ) -> dict[str, Any]:
        """Publish (republish) a dashboard.

        Sends ``POST /api/v1/dashboards/{dashboard_id}/publish``. By default
        ``adminAccess=true`` is appended so an admin token can republish when the
        caller already has access but is not the owner. Pass ``force=True`` to
        append ``force=true`` (used after script updates).

        Parameters
        ----------
        dashboard_id : str
            The dashboard ``oid`` to publish.
        admin_access : bool, optional
            When ``True`` (default), request with ``adminAccess=true``.
        force : bool, optional
            When ``True``, request with ``force=true``. Default is ``False``.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` or the JSON response body on success. On failure,
            ``{"error": "..."}``.
        """
        query_parts: list[str] = []
        if admin_access:
            query_parts.append("adminAccess=true")
        if force:
            query_parts.append("force=true")

        endpoint = f"/api/v1/dashboards/{dashboard_id}/publish"
        if query_parts:
            endpoint += "?" + "&".join(query_parts)

        self.logger.debug(f"Publishing dashboard {dashboard_id}")
        response = self.api_client.post(endpoint)

        if response is None:
            self.logger.error(f"POST request to publish dashboard {dashboard_id} failed: No response received.")
            return {"error": f"No response received while publishing dashboard ID '{dashboard_id}'"}

        if response.status_code in (200, 204):
            self.logger.info(f"Successfully published dashboard {dashboard_id}.")
            if response.status_code == 204 or not response.content:
                return {"success": True}
            try:
                return response.json()
            except Exception:
                return {"success": True}

        error_message = response.json() if response else "No response text available."
        self.logger.error(f"Failed to publish dashboard {dashboard_id}. Error: {error_message}")
        return {"error": f"Failed to publish dashboard '{dashboard_id}'. {error_message}"}

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
            return {"error": f"No response received while checking dashboard ID '{dashboard_id}'"}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed can_be_owned check for dashboard {dashboard_id}. Error: {error_message}")
            return {"error": f"Failed can_be_owned check for dashboard '{dashboard_id}'. {error_message}"}

        result = response.json()
        self.logger.info(f"Successfully checked can_be_owned for dashboard {dashboard_id}.")
        return result

    def _patch_dashboard(self, dashboard_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        endpoint = f"/api/dashboards/{dashboard_id}"
        self.logger.debug(f"Patching dashboard {dashboard_id} — fields: {list(payload.keys())}")
        response = self.api_client.patch(endpoint, data=payload)

        if response is None:
            self.logger.error(f"PATCH request to update dashboard {dashboard_id} failed: No response received.")
            return {"error": f"No response received while updating dashboard ID '{dashboard_id}'"}

        if response.status_code != 200:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to update dashboard {dashboard_id}. Error: {error_message}")
            return {"error": f"Failed to update dashboard '{dashboard_id}'. {error_message}"}

        updated_dashboard = response.json()
        self.logger.info(f"Successfully updated dashboard {dashboard_id} — fields: {list(payload.keys())}")
        return updated_dashboard
