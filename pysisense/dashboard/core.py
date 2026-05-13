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
        """Retrieve widget metadata for a dashboard.

        Sends a GET request to ``/api/v1/dashboards/{dashboardId}/widgets``.
        The response is a JSON array of widget objects (fields such as ``type``,
        ``oid``, ``title``, and ``created`` depend on the Sisense version).

        ``dashboard_ref`` is resolved with ``resolve_dashboard_reference`` so it
        may be either a 24-character dashboard ``oid`` or a dashboard title.

        Parameters
        ----------
        dashboard_ref : str
            Dashboard ``oid`` or title.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The widget list on success. On failure, ``{"error": "..."}`` when
            the reference cannot be resolved, the HTTP layer returns no
            response, the status code is not 200, the body is not valid JSON,
            or the parsed body is not a list.
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

        endpoint = f"/api/v1/dashboards/{dashboard_id}/widgets"
        self.logger.debug(f"Fetching widgets from: {endpoint}")

        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"GET request for dashboard widgets failed (no response): {dashboard_id}")
            return {"error": f"No response received while retrieving widgets for dashboard '{dashboard_id}'"}

        if response.status_code != 200:
            try:
                error_message = response.json()
            except Exception:
                error_message = getattr(response, "text", None) or "No response text available."
            self.logger.error(f"Failed to retrieve widgets for dashboard {dashboard_id}. Error: {error_message}")
            return {"error": f"Failed to retrieve widgets for dashboard '{dashboard_id}'. {error_message}"}

        try:
            widgets = response.json()
        except Exception:
            self.logger.error(f"Failed to parse widgets JSON for dashboard '{dashboard_id}'")
            return {"error": f"Failed to parse widgets response for dashboard '{dashboard_id}'"}

        if not isinstance(widgets, list):
            self.logger.error(f"Unexpected widgets response structure for dashboard '{dashboard_id}'")
            return {"error": f"Unexpected widgets response structure for dashboard '{dashboard_id}'"}

        self.logger.info(f"Successfully retrieved {len(widgets)} widgets for dashboard '{dashboard_id}'.")
        return widgets
