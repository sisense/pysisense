from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message

# Fields that Sisense manages server-side and must be stripped before a PUT write.
_SERVER_MANAGED_FIELDS = frozenset({"oid", "_id", "owner", "userId", "created", "lastUpdated", "instanceType", "dashboardid"})


class DashboardWidgetsMixin:
    def get_widget_by_id(self, dashboard_id: str, widget_id: str, *, admin_access: bool = True) -> dict[str, Any]:
        """Retrieve a single widget by its dashboard and widget IDs.

        Sends ``GET /api/v1/dashboards/{dashboard_id}/widgets/{widget_id}``.
        Returns the full widget object as returned by the Sisense API.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard that contains the widget.
        widget_id : str
            The ``oid`` of the widget to retrieve.
        admin_access : bool, optional
            When ``True`` (default), appends ``?adminAccess=true`` to the request,
            allowing access to dashboards the API token user does not own.

        Returns
        -------
        dict[str, Any]
            The widget object returned by the API, or ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/v1/dashboards/{dashboard_id}/widgets/{widget_id}"
        if admin_access:
            endpoint += "?adminAccess=true"

        self.logger.debug(f"Fetching widget {widget_id} from dashboard {dashboard_id} (admin_access={admin_access})")
        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to fetch widget '{widget_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Widget {widget_id} retrieved from dashboard {dashboard_id}.")
        return response.json()

    def update_widget(self, dashboard_id: str, widget_id: str, widget_data: dict[str, Any]) -> dict[str, Any]:
        """Write updated widget data back to Sisense.

        Sends ``PUT /api/dashboards/{dashboard_id}/widgets/{widget_id}``.
        Server-managed fields (``oid``, ``_id``, ``owner``, ``userId``,
        ``created``, ``lastUpdated``, ``instanceType``, ``dashboardid``) are
        stripped from ``widget_data`` before the request is sent.

        The caller is responsible for obtaining the current widget via
        :meth:`get_widget_by_id`, modifying the desired fields, and passing
        the result here.

        Only the dashboard owner can write widgets. Pair with
        :meth:`change_dashboard_owner` if the API token user is not the owner.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard that contains the widget.
        widget_id : str
            The ``oid`` of the widget to update.
        widget_data : dict[str, Any]
            The full widget payload with the desired changes applied. Server-managed
            fields are removed automatically before the PUT request.

        Returns
        -------
        dict[str, Any]
            The API response body on success, or ``{"error": "..."}`` on failure.
        """
        stripped = _SERVER_MANAGED_FIELDS & widget_data.keys()
        clean_payload = {k: v for k, v in widget_data.items() if k not in _SERVER_MANAGED_FIELDS}
        self.logger.debug(f"Updating widget {widget_id} on dashboard {dashboard_id} — stripped server-managed fields: {stripped}")

        endpoint = f"/api/dashboards/{dashboard_id}/widgets/{widget_id}"
        response = self.api_client.put(endpoint, data=clean_payload)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to update widget '{widget_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Widget {widget_id} on dashboard {dashboard_id} updated successfully.")
        return response.json() if response.content else {"success": True}

    def find_widgets_by_type(
        self,
        widget_type: str,
        dashboards: list[str] | str | None = None,
        *,
        admin_access: bool = True,
        max_results: int | None = None,
    ) -> list[dict[str, Any]]:
        """Find all widgets matching a given type across one or more dashboards.

        Iterates over the specified dashboards (or all dashboards when
        ``dashboards`` is ``None``) and returns every widget whose ``type``
        field matches ``widget_type``.

        Parameters
        ----------
        widget_type : str
            The widget type to match (for example ``"BloX"``, ``"chart"``,
            ``"pivot"``). Comparison is case-sensitive.
        dashboards : list[str] | str | None, optional
            One or more dashboard IDs or titles to search. A bare string is
            treated as a single-item list. When ``None`` (default), all
            dashboards on the instance are searched.
        admin_access : bool, optional
            When ``True`` (default), enumerates all dashboards on the instance
            via the admin endpoint and fetches widgets using ``adminAccess=true``,
            so dashboards owned by other users are included. When ``False``,
            only dashboards visible to the API token user are scanned.
        max_results : int | None, optional
            Stop after collecting this many matching widgets. ``None`` (default)
            means no limit.

        Returns
        -------
        list[dict[str, Any]]
            A list of match records. Each record contains:

            - ``dashboard_id`` (str): The ``oid`` of the containing dashboard.
            - ``dashboard_title`` (str): The title of the containing dashboard.
            - ``widget_id`` (str): The ``oid`` of the matching widget.
            - ``widget_title`` (str): The title of the matching widget.
            - ``widget_type`` (str): The type of the matching widget.

            Returns an empty list when no matches are found or all lookups fail.
        """
        if isinstance(dashboards, str):
            dashboards = [dashboards]

        # Build the list of (id, title) pairs to scan
        targets: list[tuple[str, str]] = []

        if dashboards is None:
            list_endpoint = "/api/v1/dashboards/admin?dashboardType=owner" if admin_access else "/api/v1/dashboards"
            self.logger.debug(f"No dashboards specified — fetching full dashboard list (admin_access={admin_access}).")
            all_dash_response = self.api_client.get(list_endpoint)
            if all_dash_response is None or all_dash_response.status_code != 200:
                self.logger.error("Failed to retrieve dashboard list for find_widgets_by_type.")
                return []
            targets = [(d["oid"], d.get("title", "")) for d in all_dash_response.json() if "oid" in d]
        else:
            for dashboard_ref in dashboards:
                ref = self.resolve_dashboard_reference(dashboard_ref)
                if not ref.get("success"):
                    self.logger.warning(f"Could not resolve dashboard '{dashboard_ref}': {ref.get('error')}")
                    continue
                targets.append((ref["dashboard_id"], ref.get("dashboard_title") or ""))

        self.logger.debug(f"Scanning {len(targets)} dashboard(s) for widgets of type '{widget_type}'.")

        results: list[dict[str, Any]] = []

        for dashboard_id, dashboard_title in targets:
            if max_results is not None and len(results) >= max_results:
                break

            widgets_endpoint = f"/api/v1/dashboards/{dashboard_id}/widgets"
            if admin_access:
                widgets_endpoint += "?adminAccess=true"

            widgets_response = self.api_client.get(widgets_endpoint)
            if widgets_response is None or widgets_response.status_code != 200:
                self.logger.warning(f"Could not fetch widgets for dashboard {dashboard_id}, skipping.")
                continue

            widgets_data = widgets_response.json()
            widgets = widgets_data if isinstance(widgets_data, list) else widgets_data.get("widgets", [])

            for widget in widgets:
                if widget.get("type") == widget_type:
                    results.append(
                        {
                            "dashboard_id": dashboard_id,
                            "dashboard_title": dashboard_title,
                            "widget_id": widget.get("oid", ""),
                            "widget_title": widget.get("title", ""),
                            "widget_type": widget.get("type", ""),
                        }
                    )
                    if max_results is not None and len(results) >= max_results:
                        break

        self.logger.info(f"Found {len(results)} widget(s) of type '{widget_type}'.")
        return results
