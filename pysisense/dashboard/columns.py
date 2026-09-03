from __future__ import annotations

from typing import Any

from ..utils import _extract_dashboard_columns, _extract_error_message


class ColumnsMixin:
    def get_dashboard_columns(self, dashboard_name: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve columns referenced by a dashboard, including widget and filter columns.

        Resolves the dashboard by title with ``get_dashboard_by_name``, exports
        its full metadata, then extracts column references from dashboard
        filters (plain and dependent) and from every widget panel item,
        including the columns a formula references through its ``context``.
        Both ``[Table.Column]`` and ``[Table].[Column]`` references are
        understood, and table or column names may contain any character.
        The final list is deduplicated by ``table`` and ``column``.

        Parameters
        ----------
        dashboard_name : str
            Title of the dashboard to retrieve columns from.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of distinct column entries. Each entry contains
            ``dashboard_name``, ``source`` (``"filter"`` or ``"widget"``),
            ``widget_id`` (the widget's own ``oid``, or ``"N/A"`` for dashboard
            filters), ``table``, and ``column`` — an empty list means the
            dashboard genuinely references no columns. On failure (dashboard
            not found, or its metadata cannot be retrieved or parsed), returns
            the standard ``{"ok": False, "error": "...", ...}`` dict.
        """
        self.logger.info(f"Starting column retrieval for dashboard: {dashboard_name}")

        # Step 1: Get dashboard details using existing method
        dashboard = self.get_dashboard_by_name(dashboard_name)
        if not dashboard or "error" in dashboard:
            error_msg = f"Dashboard '{dashboard_name}' not found."
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}
        dashboard_id = dashboard[0].get("oid")
        self.logger.info(f"Dashboard '{dashboard_name}' found with ID: {dashboard_id}")

        # Step 2: Export full dashboard metadata
        dashboard_url = f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true"
        dashboard_response = self.api_client.get(dashboard_url)

        if dashboard_response is None or dashboard_response.status_code != 200:
            failure = _extract_error_message(dashboard_response, f"Failed to export dashboard with ID '{dashboard_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            dashboard_data = dashboard_response.json()
        except Exception:
            self.logger.exception(f"Failed to parse dashboard export response for ID '{dashboard_id}'")
            return {"ok": False, "error": f"Failed to parse dashboard export response for ID '{dashboard_id}'."}

        if not dashboard_data or not isinstance(dashboard_data, list) or not isinstance(dashboard_data[0], dict):
            self.logger.error(f"Unexpected dashboard data structure for ID '{dashboard_id}'")
            return {"ok": False, "error": f"Unexpected dashboard export structure for ID '{dashboard_id}'."}

        dashboard = dashboard_data[0]
        self.logger.debug(f"Analyzing dashboard '{dashboard.get('title', dashboard_name)}' (ID: {dashboard_id})")

        # Step 3: Extract every column reference from filters and widgets (shared walk)
        dashboard_columns = _extract_dashboard_columns(dashboard, dashboard_name, logger=self.logger)
        self.logger.info(
            f"Processed {len(dashboard.get('filters') or [])} filters and {len(dashboard.get('widgets') or [])} widgets, "
            f"extracted {len(dashboard_columns)} column references for dashboard '{dashboard_name}'"
        )

        # Step 4: Deduplicate columns based on 'table' and 'column'
        distinct_columns_set = set()
        distinct_dashboard_columns = []

        for entry in dashboard_columns:
            table = entry["table"]
            column = entry["column"]

            # Remove (Calendar) from column names if present
            if column.endswith(" (Calendar)"):
                column = column.replace(" (Calendar)", "").strip()

            key = (table, column)
            if key not in distinct_columns_set:
                distinct_dashboard_columns.append(entry)
                distinct_columns_set.add(key)

        self.logger.info(f"Retrieved {len(distinct_dashboard_columns)} distinct columns from dashboard '{dashboard_name}'")

        return distinct_dashboard_columns
