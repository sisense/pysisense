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
