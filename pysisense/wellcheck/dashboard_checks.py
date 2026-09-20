from __future__ import annotations

import re
from typing import Any


class DashboardChecksMixin:
    def check_dashboard_structure(
        self,
        dashboards: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Analyze the structure of one or more dashboards.

        This method:
          - Counts pivot widgets (pivot / pivot2)
          - Counts tabber widgets (WidgetsTabber)
          - Counts accordion widgets via accordionConfig on widgets
          - Counts jump-to-dashboard (JTD) instances, treated as child dashboards

        A "child dashboard" is treated as any dashboard referenced as a
        jump-to-dashboard target from the parent dashboard, either via
        widget options (drillTarget) or via dashboard script
        (prism.jumpToDashboard calls).

        This method is intended to back wellcheck tasks and agentic tools that
        respond to prompts such as:
          - "check child dashboards for this dashboard"
          - "check jump-to dashboards on XYZ"
          - "analyze dashboard structure / complexity"

        Parameters
        ----------
        dashboards : list of str, optional
            One or more dashboard references to analyze. Each reference can be:
              - a Sisense dashboard ID, or
              - a dashboard title (name).
            At least one dashboard reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.

        Returns
        -------
        list of dict
            A list with one entry per successfully processed dashboard. Each
            entry has the keys:
              - dashboard_id (str): Resolved dashboard ID.
              - dashboard_title (str): Resolved dashboard title.
              - pivot_count (int): Number of pivot/pivot2 widgets.
              - tabber_count (int): Number of WidgetsTabber widgets.
              - accordion_count (int): Number of accordion widgets detected
                via accordionConfig on widgets.
              - jtd_count (int): Number of jump-to-dashboard (JTD) instances
                (child dashboards) detected in widget options and scripts.
            If no dashboards are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting dashboard structure check.")
        self.logger.debug(f"Input dashboards parameter: {dashboards}")

        # Validate input
        if dashboards is None:
            error_msg = "At least one dashboard reference (ID or name) is required."
            self.logger.error(error_msg)
            return []

        # Normalize to list of strings
        dashboard_refs = [dashboards] if isinstance(dashboards, str) else [ref for ref in dashboards if isinstance(ref, str)]

        if not dashboard_refs:
            error_msg = "No valid dashboard references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified dashboards: {dashboard_refs}")

        results: list[dict[str, Any]] = []
        total_dashboards = 0
        total_pivot_count = 0
        total_tabber_count = 0
        total_jtd_count = 0
        total_accordion_count = 0

        for ref in dashboard_refs:
            self.logger.info(f"Processing dashboard reference: {ref}")

            # Resolve ID and title using the Dashboard helper
            resolved = self.dashboard.resolve_dashboard_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(f"Skipping dashboard reference '{ref}': {resolved.get('error')}")
                continue

            dashboard_id = resolved.get("dashboard_id")
            dashboard_title = resolved.get("dashboard_title") or ref

            if not dashboard_id:
                self.logger.warning(f"Resolved dashboard reference '{ref}' has no dashboard_id. Skipping.")
                continue

            # Fetch full dashboard definition (widgets, scripts, etc.)
            endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"
            self.logger.debug(f"Fetching full dashboard definition from: {endpoint}")

            response = self.api_client.get(endpoint)

            if response is None:
                self.logger.warning(f"Failed to retrieve dashboard data for dashboard OID: {dashboard_id} (Title: {dashboard_title})")
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(f"Failed to retrieve dashboard data for dashboard OID: {dashboard_id} (Title: {dashboard_title}). Status: {response.status_code}, Error: {error_body}")
                continue

            try:
                dashboard_data = response.json()
            except Exception as exc:
                self.logger.exception(f"Failed to parse dashboard JSON for '{dashboard_id}': {exc}")
                continue

            row = self._compute_dashboard_structure_counts(
                dashboard_data=dashboard_data,
                resolved_title=dashboard_title,
            )

            if row is not None:
                results.append(row)
                total_dashboards += 1
                total_pivot_count += row.get("pivot_count", 0)
                total_tabber_count += row.get("tabber_count", 0)
                total_jtd_count += row.get("jtd_count", 0)
                total_accordion_count += row.get("accordion_count", 0)

        if total_dashboards == 0:
            self.logger.warning("No dashboards were successfully processed for structure check.")
            return []

        # Summary logs
        self.logger.info(f"Total dashboards processed: {total_dashboards}")
        self.logger.info(f"Total pivot widgets: {total_pivot_count}")
        self.logger.info(f"Total tabber widgets: {total_tabber_count}")
        self.logger.info(f"Total JTD (Jump to Dashboard) instances: {total_jtd_count}")
        self.logger.info(f"Total accordion widgets: {total_accordion_count}")
        self.logger.info("Completed dashboard structure check.")

        return results

    def _compute_dashboard_structure_counts(
        self,
        dashboard_data: dict[str, Any],
        resolved_title: str,
    ) -> dict[str, Any] | None:
        """
        Compute pivot/tabber/accordion/JTD counts for a single dashboard payload.
        """
        dashboard_oid = dashboard_data.get("oid")
        dashboard_title = dashboard_data.get("title", resolved_title)

        if dashboard_oid is None:
            self.logger.warning(f"Dashboard missing OID: {dashboard_title}")
            return None

        widgets = dashboard_data.get("widgets")
        if not widgets or not isinstance(widgets, list):
            self.logger.warning(f"Failed to retrieve data for dashboard OID: {dashboard_oid} (Title: {dashboard_title})")
            return None

        pivot_count = 0
        tabber_count = 0
        accordion_count = 0
        jtd_count = 0
        jtd_ids: set[str] = set()

        for widget in widgets:
            (
                pivot_count,
                tabber_count,
                jtd_count,
                accordion_count,
                jtd_ids,
            ) = self._process_widget_for_structure(
                widget=widget,
                pivot_count=pivot_count,
                tabber_count=tabber_count,
                jtd_count=jtd_count,
                accordion_count=accordion_count,
                jtd_ids=jtd_ids,
                dashboard_title=dashboard_title,
            )

        return {
            "dashboard_id": dashboard_oid,
            "dashboard_title": dashboard_title,
            "pivot_count": pivot_count,
            "tabber_count": tabber_count,
            "accordion_count": accordion_count,
            "jtd_count": jtd_count,
        }

    def _process_widget_for_structure(
        self,
        widget: dict[str, Any],
        pivot_count: int,
        tabber_count: int,
        jtd_count: int,
        accordion_count: int,
        jtd_ids: set[str],
        dashboard_title: str,
    ) -> tuple[int, int, int, int, set[str]]:
        """
        Process a single widget and update counts for pivots, tabbers,
        jump-to dashboards (JTD), and accordions.

        Accordion widgets are detected via accordionConfig on the widget:
          - accordionConfig.isEnabled is True, and
          - accordionConfig.dashboardName is non-empty.
        """
        widget_type = widget.get("type", "")

        # Pivot detection (pivot / pivot2)
        if "pivot2" in widget_type or "pivot" in widget_type:
            pivot_count += 1

        # Tabber detection
        if "WidgetsTabber" in widget_type:
            tabber_count += 1

        # Accordion detection via accordionConfig
        accordion_config = widget.get("accordionConfig")
        if isinstance(accordion_config, dict):
            is_enabled = bool(accordion_config.get("isEnabled", False))
            dashboard_name = (accordion_config.get("dashboardName") or "").strip()
            if is_enabled and dashboard_name:
                accordion_count += 1

        # JTD via widget.options.drillTarget.oid
        options = widget.get("options", {})
        if isinstance(options, dict) and "drillTarget" in options:
            drill_target = options["drillTarget"]
            if isinstance(drill_target, dict):
                jtd_oid = drill_target.get("oid")
                if isinstance(jtd_oid, str) and jtd_oid not in jtd_ids:
                    jtd_ids.add(jtd_oid)
                    jtd_count += 1

        # Script-based JTDs
        if "script" in widget:
            value = widget["script"]
            if isinstance(value, str):
                cleaned = self._clean_script_comments(value)
                jtd_count, accordion_count = self._process_script_for_structure(
                    script=cleaned,
                    jtd_count=jtd_count,
                    accordion_count=accordion_count,
                    jtd_ids=jtd_ids,
                )
            else:
                self.logger.warning(f"Expected string for 'script' in widget, but got {type(value)} in dashboard: {dashboard_title} in widget: {widget.get('oid')}")

        return pivot_count, tabber_count, jtd_count, accordion_count, jtd_ids

    @staticmethod
    def _clean_script_comments(script: str) -> str:
        """
        Remove block and line comments from a JavaScript script string.
        """
        script = re.sub(r"/\*.*?\*/", "", script, flags=re.DOTALL)
        script = re.sub(r"//.*", "", script)
        return script

    def _process_script_for_structure(
        self,
        script: str,
        jtd_count: int,
        accordion_count: int,
        jtd_ids: set[str],
    ) -> tuple[int, int]:
        """
        Process dashboard script content and update JTD counts.

        Accordion detection based on script has been deprecated and removed.
        """
        # Find all prism.jumpToDashboard(...) blocks
        jtd_block_pattern = r"prism\.jumpToDashboard\(widget,\s*\{[\s\S]*?\}\s*\);"
        jtd_blocks = re.findall(jtd_block_pattern, script, re.DOTALL)

        if jtd_blocks:
            for block in jtd_blocks:
                jtd_count = self._count_non_pivot_jtds(block, jtd_count, jtd_ids)
                jtd_count = self._count_pivot_jtds(block, jtd_count, jtd_ids)

        return jtd_count, accordion_count

    @staticmethod
    def _count_non_pivot_jtds(block: str, jtd_count: int, jtd_ids: set[str]) -> int:
        """
        Count jump-to-dashboard references in non-pivot widgets.
        """
        # dashboardId: "24-char-id"
        dashboard_id_matches = re.findall(r'dashboardId\s*:\s*"\w{24}"', block)
        for match in dashboard_id_matches:
            id_value_match = re.search(r'"\w{24}"', match)
            if id_value_match:
                id_value = id_value_match.group().strip('"')
                if id_value not in jtd_ids:
                    jtd_ids.add(id_value)
                    jtd_count += 1

        # dashboardIds: [{ id: "..." }, ...]
        dashboard_ids_list_pattern = r"dashboardIds\s*:\s*\[\s*(\{[^\}]*\}\s*,?\s*)+\]"
        if re.search(dashboard_ids_list_pattern, block, re.DOTALL):
            id_matches = re.findall(r'id\s*:\s*"\w{24}"', block)
            for match in id_matches:
                id_value_match = re.search(r'"\w{24}"', match)
                if id_value_match:
                    id_value = id_value_match.group().strip('"')
                    if id_value not in jtd_ids:
                        jtd_ids.add(id_value)
                        jtd_count += 1

        return jtd_count

    @staticmethod
    def _count_pivot_jtds(block: str, jtd_count: int, jtd_ids: set[str]) -> int:
        """
        Count jump-to-dashboard references in pivot widget configurations.
        """
        # targetDashboards: [ { dashboardId: "..." }, ... ]
        target_dashboards_list_pattern = r"targetDashboards\s*:\s*\[.*?\]"
        if re.search(target_dashboards_list_pattern, block, re.DOTALL):
            target_dashboard_matches = re.findall(r'dashboardId\s*:\s*"\w{24}"', block)
            for match in target_dashboard_matches:
                id_value_match = re.search(r'"\w{24}"', match)
                if id_value_match:
                    id_value = id_value_match.group().strip('"')
                    if id_value not in jtd_ids:
                        jtd_ids.add(id_value)
                        jtd_count += 1

        # targetDashboards: { ... dashboardId: "..." }
        pivot_single_dashboard_id_pattern = r'targetDashboards\s*:\s*{[^}]*dashboardId\s*:\s*"\w{24}"'
        pivot_single_dashboard_id = re.findall(pivot_single_dashboard_id_pattern, block)
        for match in pivot_single_dashboard_id:
            id_value_match = re.search(r'"\w{24}"', match)
            if id_value_match:
                id_value = id_value_match.group().strip('"')
                if id_value not in jtd_ids:
                    jtd_ids.add(id_value)
                    jtd_count += 1

        return jtd_count

    def check_dashboard_widget_counts(
        self,
        dashboards: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Compute widget counts for one or more dashboards.

        This method retrieves each specified dashboard definition, counts the
        number of widgets on that dashboard, and returns a per-dashboard
        summary.

        Parameters
        ----------
        dashboards : list of str, optional
            One or more dashboard references to analyze. Each reference can be:
              - a Sisense dashboard ID, or
              - a dashboard title (name).
            At least one dashboard reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.

        Returns
        -------
        list of dict
            A list with one entry per successfully processed dashboard. Each
            entry contains:
              - dashboard_id (str): Resolved dashboard ID.
              - dashboard_title (str): Resolved dashboard title.
              - widget_count (int): Number of widgets on the dashboard.

            If no dashboards are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting dashboard widget count check.")
        self.logger.debug(f"Input dashboards parameter: {dashboards}")

        # Validate input
        if dashboards is None:
            error_msg = "At least one dashboard reference (ID or name) is required."
            self.logger.error(error_msg)
            return []

        # Normalize to list of strings
        dashboard_refs = [dashboards] if isinstance(dashboards, str) else [ref for ref in dashboards if isinstance(ref, str)]

        if not dashboard_refs:
            error_msg = "No valid dashboard references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified dashboards: {dashboard_refs}")

        results: list[dict[str, Any]] = []
        total_dashboards = 0
        total_widgets = 0

        for ref in dashboard_refs:
            self.logger.info(f"Processing dashboard reference: {ref}")

            # Resolve ID and title using the Dashboard helper
            resolved = self.dashboard.resolve_dashboard_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(f"Skipping dashboard reference '{ref}': {resolved.get('error')}")
                continue

            dashboard_id = resolved.get("dashboard_id")
            dashboard_title = resolved.get("dashboard_title") or ref

            if not dashboard_id:
                self.logger.warning(f"Resolved dashboard reference '{ref}' has no dashboard_id. Skipping.")
                continue

            # Fetch full dashboard definition to count widgets
            endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"
            self.logger.debug(f"Fetching dashboard definition from: {endpoint}")

            response = self.api_client.get(endpoint)

            if response is None:
                # Failed data retrieval
                self.logger.warning(f"Failed to retrieve data or no widgets found for dashboard ID: {dashboard_id}")
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(f"Failed to retrieve data or no widgets found for dashboard ID: {dashboard_id}. Status: {response.status_code}, Error: {error_body}")
                continue

            try:
                dashboard_data = response.json()
            except Exception as exc:
                self.logger.exception(f"Failed to parse dashboard JSON for '{dashboard_id}': {exc}")
                continue

            row = self._compute_widget_count_for_dashboard(
                dashboard_data=dashboard_data,
                dashboard_id=dashboard_id,
                dashboard_title=dashboard_title,
            )

            if row is not None:
                results.append(row)
                total_dashboards += 1
                total_widgets += row["widget_count"]

        if total_dashboards == 0:
            self.logger.warning("No dashboards were successfully processed for widget count check.")
            return []

        # Summary logs
        self.logger.info(f"Total number of dashboards retrieved: {total_dashboards}")
        self.logger.info(f"Total widgets across processed dashboards: {total_widgets}")
        self.logger.info("Completed dashboard widget count check.")

        return results

    def _compute_widget_count_for_dashboard(
        self,
        dashboard_data: dict[str, Any],
        dashboard_id: str,
        dashboard_title: str,
    ) -> dict[str, Any] | None:
        """
        Compute the widget count for a single dashboard payload.
        """
        widgets = dashboard_data.get("widgets")
        if not widgets or not isinstance(widgets, list):
            self.logger.warning(f"Failed to retrieve data or no widgets found for dashboard ID: {dashboard_id}")
            return None

        widget_count = len(widgets)
        resolved_id = dashboard_data.get("oid", dashboard_id)
        resolved_title = dashboard_data.get("title", dashboard_title)

        self.logger.info(f"Processed dashboard '{resolved_title}' with {widget_count} widgets.")

        return {
            "dashboard_id": resolved_id,
            "dashboard_title": resolved_title,
            "widget_count": widget_count,
        }

    def check_pivot_widget_fields(
        self,
        dashboards: list[str] | None = None,
        max_fields: int = 20,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Count the fields of every pivot widget on one or more dashboards and flag the wide ones.

        Reads each dashboard (``GET /api/dashboards/{id}?adminAccess=true``), finds its pivot
        widgets (types containing ``"pivot"``), counts the items across their panels and
        returns one row per pivot widget, flagged when the count is above ``max_fields``.
        Every pivot is returned, so an empty list means the dashboards have no pivot
        widgets, not that all of them are within the limit.

        Parameters
        ----------
        dashboards : list[str] | None, optional
            One or more dashboard references, each an ID or a title. A single string is
            accepted in place of a one-element list.
        max_fields : int, optional
            Field count above which a pivot widget is flagged (strictly greater). Default ``20``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            One row per pivot widget: ``dashboard_id``, ``dashboard_title``, ``widget_id``,
            ``field_count`` (items across the widget's panels), ``has_more_fields``
            (``field_count > max_fields``), ``status`` (``"checked"``) and ``error`` (``None``).
            A dashboard that cannot be resolved or read contributes one row with ``status``
            ``"error"``, the message in ``error`` and ``None`` for the other fields. When
            ``dashboards`` is missing or holds no reference, the standard
            ``{"ok": False, "error": "..."}`` dict.
        """
        self.logger.info("Starting widget field check for dashboards.")
        self.logger.debug("Input dashboards parameter for widget field check: %s", dashboards)

        if dashboards is None:
            failure = {"ok": False, "error": "At least one dashboard reference (ID or name) is required for widget field analysis."}
            self.logger.error(failure["error"])
            return failure
        dashboard_refs = [dashboards] if isinstance(dashboards, str) else [ref for ref in dashboards if isinstance(ref, str)]
        if not dashboard_refs:
            failure = {"ok": False, "error": "No valid dashboard references provided for widget field analysis."}
            self.logger.error(failure["error"])
            return failure
        self.logger.info("Processing specified dashboards for widget fields: %s", dashboard_refs)

        def error_row(ref: str, message: str, dashboard_id: str | None = None, title: str | None = None) -> dict[str, Any]:
            return {"dashboard_id": dashboard_id or ref, "dashboard_title": title, "widget_id": None, "field_count": None, "has_more_fields": None, "status": "error", "error": message}

        results: list[dict[str, Any]] = []
        total_dashboards = 0
        total_pivot_widgets = 0
        total_pivot_widgets_over_threshold = 0

        for ref in dashboard_refs:
            self.logger.info("Processing dashboard reference for widget fields: %s", ref)
            resolved = self.dashboard.resolve_dashboard_reference(ref)
            if not resolved.get("success") or not resolved.get("dashboard_id"):
                message = f"Dashboard '{ref}' could not be resolved: {resolved.get('error') or 'not found'}"
                self.logger.warning(message)
                results.append(error_row(ref, message))
                continue
            dashboard_id = resolved["dashboard_id"]
            dashboard_title = resolved.get("dashboard_title") or ref

            endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"
            self.logger.debug("Fetching full dashboard definition for widget fields from: %s", endpoint)
            response = self.api_client.get(endpoint)
            if response is None or response.status_code != 200:
                message = f"Dashboard '{dashboard_title}' ({dashboard_id}) could not be read" + (f" (HTTP {response.status_code})" if response is not None else " (no response)")
                self.logger.warning(message)
                results.append(error_row(ref, message, dashboard_id, dashboard_title))
                continue
            try:
                dashboard_data = response.json()
            except Exception as exc:
                message = f"Dashboard '{dashboard_title}' ({dashboard_id}) returned unreadable JSON: {exc}"
                self.logger.warning(message)
                results.append(error_row(ref, message, dashboard_id, dashboard_title))
                continue

            rows_for_dashboard, pivot_widget_found, over_threshold, pivot_widget_count = self._compute_pivot_widget_field_details(
                dashboard_data=dashboard_data,
                resolved_title=dashboard_title,
                max_fields=max_fields,
            )
            results.extend(rows_for_dashboard)
            total_dashboards += 1
            total_pivot_widgets += pivot_widget_count
            total_pivot_widgets_over_threshold += over_threshold
            if not pivot_widget_found:
                self.logger.info("Dashboard:%s has no pivot widgets", dashboard_title)

        self.logger.info("Total dashboards processed for widget fields: %d", total_dashboards)
        self.logger.info("Total pivot widgets inspected across dashboards: %d", total_pivot_widgets)
        self.logger.info("Total pivot widgets above field threshold (%d): %d", max_fields, total_pivot_widgets_over_threshold)
        self.logger.info("Completed widget field check for dashboards.")
        return results

    def _compute_pivot_widget_field_details(
        self,
        dashboard_data: dict[str, Any],
        resolved_title: str,
        max_fields: int,
    ) -> tuple[list[dict[str, Any]], bool, int, int]:
        """
        Compute pivot widget field counts for a single dashboard payload.
        """
        dashboard_oid = dashboard_data.get("oid")
        dashboard_title = dashboard_data.get("title", resolved_title)

        if dashboard_oid is None:
            self.logger.warning("Dashboard missing OID: %s", dashboard_title)
            return [], False, 0, 0

        widgets = dashboard_data.get("widgets")
        if not widgets or not isinstance(widgets, list):
            self.logger.warning(
                "Failed to retrieve data for dashboard OID: %s (Title: %s)",
                dashboard_oid,
                dashboard_title,
            )
            return [], False, 0, 0

        rows: list[dict[str, Any]] = []
        pivot_widget_found = False
        pivot_widgets_over_threshold = 0
        pivot_widget_count = 0

        for widget in widgets:
            (
                maybe_row,
                pivot_found_here,
                over_threshold_here,
            ) = self._process_pivot_widget_for_fields(
                widget=widget,
                dashboard_oid=dashboard_oid,
                dashboard_title=dashboard_title,
                max_fields=max_fields,
            )

            if pivot_found_here:
                pivot_widget_found = True
                pivot_widget_count += 1

            if maybe_row is not None:
                rows.append(maybe_row)
                pivot_widgets_over_threshold += over_threshold_here

        return rows, pivot_widget_found, pivot_widgets_over_threshold, pivot_widget_count

    def _process_pivot_widget_for_fields(
        self,
        widget: dict[str, Any],
        dashboard_oid: str,
        dashboard_title: str,
        max_fields: int,
    ) -> tuple[dict[str, Any] | None, bool, int]:
        """
        Process a single widget and, if it is a pivot, compute field count.

        Returns a tuple of:
          - row dict (or None if not above threshold / not a pivot),
          - pivot_found (bool),
          - over_threshold (0 or 1)
        """
        widget_type = widget.get("type", "")
        widget_id = widget.get("oid")

        # Non-pivot widgets are ignored for this check
        if "pivot2" not in widget_type and "pivot" not in widget_type:
            return None, False, 0

        # At this point we know it is a pivot widget
        # Count items across all panels
        metadata = widget.get("metadata", {})
        panels = metadata.get("panels", [])
        panel_count = 0

        for panel in panels:
            items = panel.get("items", [])
            panel_count += len(items)

        over = panel_count > max_fields
        self.logger.info("Dashboard:%s Pivot Widget: %s has %d fields (limit %d)", dashboard_title, widget_id, panel_count, max_fields)
        row: dict[str, Any] = {
            "dashboard_id": dashboard_oid,
            "dashboard_title": dashboard_title,
            "widget_id": widget_id,
            "field_count": panel_count,
            "has_more_fields": over,
            "status": "checked",
            "error": None,
        }
        return row, True, 1 if over else 0
