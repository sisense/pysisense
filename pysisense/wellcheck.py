from typing import Any, Dict, List, Optional, Set, Tuple, Union
import re

from .sisenseclient import SisenseClient
from .dashboard import Dashboard
from .datamodel import DataModel
from .access_management import AccessManagement


class WellCheck:
    """
    Collection of wellcheck / health-check methods over Sisense assets.
    WellCheck methods typically analyze Sisense assets (dashboards,
    widgets, data models) for complexity, best practices, and potential issues.
    """

    def __init__(self, api_client: Optional[SisenseClient] = None, debug: bool = False) -> None:
        """
        Initialize the WellCheck helper.

        Parameters
        ----------
        api_client : SisenseClient, optional
            Existing SisenseClient instance. If None, a new SisenseClient will
            be created using the provided debug flag.
        debug : bool, optional
            Enables debug logging when creating an internal SisenseClient.
        """
        if api_client is not None:
            self.api_client = api_client
        else:
            self.api_client = SisenseClient(debug=debug)

        # Use the SisenseClient's logger
        self.logger = self.api_client.logger
        self.dashboard = Dashboard(api_client=self.api_client, debug=debug)
        self.datamodel = DataModel(api_client=self.api_client, debug=debug)
        self.access_mgmt = AccessManagement(api_client=self.api_client, debug=debug)

        self.logger.debug("WellCheck class initialized.")

    # ------------------------------------------------------------------ #
    # WellCheck methods for dashboards                                   #
    # ------------------------------------------------------------------ #

    def check_dashboard_structure(
        self,
        dashboards: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
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
        if isinstance(dashboards, str):
            dashboard_refs = [dashboards]
        else:
            dashboard_refs = [ref for ref in dashboards if isinstance(ref, str)]

        if not dashboard_refs:
            error_msg = "No valid dashboard references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified dashboards: {dashboard_refs}")

        results: List[Dict[str, Any]] = []
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
                self.logger.warning(
                    f"Skipping dashboard reference '{ref}': {resolved.get('error')}"
                )
                continue

            dashboard_id = resolved.get("dashboard_id")
            dashboard_title = resolved.get("dashboard_title") or ref

            if not dashboard_id:
                self.logger.warning(
                    f"Resolved dashboard reference '{ref}' has no dashboard_id. Skipping."
                )
                continue

            # Fetch full dashboard definition (widgets, scripts, etc.)
            endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"
            self.logger.debug(f"Fetching full dashboard definition from: {endpoint}")

            response = self.api_client.get(endpoint)

            if response is None:
                self.logger.warning(
                    f"Failed to retrieve dashboard data for dashboard OID: {dashboard_id} "
                    f"(Title: {dashboard_title})"
                )
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(
                    f"Failed to retrieve dashboard data for dashboard OID: {dashboard_id} "
                    f"(Title: {dashboard_title}). Status: {response.status_code}, "
                    f"Error: {error_body}"
                )
                continue

            try:
                dashboard_data = response.json()
            except Exception as exc:
                self.logger.exception(
                    f"Failed to parse dashboard JSON for '{dashboard_id}': {exc}"
                )
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
            self.logger.warning(
                "No dashboards were successfully processed for structure check."
            )
            return []

        # Summary logs
        self.logger.info(f"Total dashboards processed: {total_dashboards}")
        self.logger.info(f"Total pivot widgets: {total_pivot_count}")
        self.logger.info(f"Total tabber widgets: {total_tabber_count}")
        self.logger.info(
            f"Total JTD (Jump to Dashboard) instances: {total_jtd_count}"
        )
        self.logger.info(
            f"Total accordion widgets: {total_accordion_count}"
        )
        self.logger.info("Completed dashboard structure check.")

        return results

    # ------------------------------------------------------------------ #
    # Internal helpers for counting structure metrics                    #
    # ------------------------------------------------------------------ #

    def _compute_dashboard_structure_counts(
        self,
        dashboard_data: Dict[str, Any],
        resolved_title: str,
    ) -> Optional[Dict[str, Any]]:
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
            self.logger.warning(
                f"Failed to retrieve data for dashboard OID: {dashboard_oid} "
                f"(Title: {dashboard_title})"
            )
            return None

        pivot_count = 0
        tabber_count = 0
        accordion_count = 0
        jtd_count = 0
        jtd_ids: Set[str] = set()

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
        widget: Dict[str, Any],
        pivot_count: int,
        tabber_count: int,
        jtd_count: int,
        accordion_count: int,
        jtd_ids: Set[str],
        dashboard_title: str,
    ) -> Tuple[int, int, int, int, Set[str]]:
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
                self.logger.warning(
                    f"Expected string for 'script' in widget, but got {type(value)} "
                    f"in dashboard: {dashboard_title} in widget: {widget.get('oid')}"
                )

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
        jtd_ids: Set[str],
    ) -> Tuple[int, int]:
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
    def _count_non_pivot_jtds(block: str, jtd_count: int, jtd_ids: Set[str]) -> int:
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
    def _count_pivot_jtds(block: str, jtd_count: int, jtd_ids: Set[str]) -> int:
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
        pivot_single_dashboard_id_pattern = (
            r'targetDashboards\s*:\s*{[^}]*dashboardId\s*:\s*"\w{24}"'
        )
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
        dashboards: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
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
        if isinstance(dashboards, str):
            dashboard_refs = [dashboards]
        else:
            dashboard_refs = [ref for ref in dashboards if isinstance(ref, str)]

        if not dashboard_refs:
            error_msg = "No valid dashboard references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified dashboards: {dashboard_refs}")

        results: List[Dict[str, Any]] = []
        total_dashboards = 0
        total_widgets = 0

        for ref in dashboard_refs:
            self.logger.info(f"Processing dashboard reference: {ref}")

            # Resolve ID and title using the Dashboard helper
            resolved = self.dashboard.resolve_dashboard_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(
                    f"Skipping dashboard reference '{ref}': {resolved.get('error')}"
                )
                continue

            dashboard_id = resolved.get("dashboard_id")
            dashboard_title = resolved.get("dashboard_title") or ref

            if not dashboard_id:
                self.logger.warning(
                    f"Resolved dashboard reference '{ref}' has no dashboard_id. Skipping."
                )
                continue

            # Fetch full dashboard definition to count widgets
            endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"
            self.logger.debug(f"Fetching dashboard definition from: {endpoint}")

            response = self.api_client.get(endpoint)

            if response is None:
                # Failed data retrieval
                self.logger.warning(
                    f"Failed to retrieve data or no widgets found for dashboard ID: {dashboard_id}"
                )
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(
                    f"Failed to retrieve data or no widgets found for dashboard ID: {dashboard_id}. "
                    f"Status: {response.status_code}, Error: {error_body}"
                )
                continue

            try:
                dashboard_data = response.json()
            except Exception as exc:
                self.logger.exception(
                    f"Failed to parse dashboard JSON for '{dashboard_id}': {exc}"
                )
                continue

            widgets = dashboard_data.get("widgets")
            if not widgets or not isinstance(widgets, list):
                self.logger.warning(
                    f"Failed to retrieve data or no widgets found for dashboard ID: {dashboard_id}"
                )
                continue

            widget_count = len(widgets)
            resolved_id = dashboard_data.get("oid", dashboard_id)
            resolved_title = dashboard_data.get("title", dashboard_title)

            # Per-dashboard log
            self.logger.info(
                f"Processed dashboard '{resolved_title}' with {widget_count} widgets."
            )

            results.append(
                {
                    "dashboard_id": resolved_id,
                    "dashboard_title": resolved_title,
                    "widget_count": widget_count,
                }
            )

            total_dashboards += 1
            total_widgets += widget_count

        if total_dashboards == 0:
            self.logger.warning(
                "No dashboards were successfully processed for widget count check."
            )
            return []

        # Summary logs
        self.logger.info(f"Total number of dashboards retrieved: {total_dashboards}")
        self.logger.info(
            f"Total widgets across processed dashboards: {total_widgets}"
        )
        self.logger.info("Completed dashboard widget count check.")

        return results

    def check_pivot_widget_fields(
        self,
        dashboards: Optional[List[str]] = None,
        max_fields: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Analyze pivot widgets on one or more dashboards and report those with many fields.

        This method retrieves each specified dashboard, scans all pivot widgets
        (types containing ``"pivot"`` or ``"pivot2"``), counts how many fields
        (items) are attached to those widgets, and returns a per-widget summary
        for any pivot with more than ``max_fields`` fields.

        Parameters
        ----------
        dashboards : list of str, optional
            One or more dashboard references to analyze. Each reference can be:
              - a Sisense dashboard ID, or
              - a dashboard title (name).
            At least one dashboard reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.
        max_fields : int, optional
            Threshold for the number of fields on a pivot widget. Only pivot
            widgets with more than this number of fields are included in the
            returned data. Defaults to 20.

        Returns
        -------
        list of dict
            A list of dictionaries, each describing a pivot widget that exceeds
            the configured field threshold. Each entry contains:
              - dashboard_id (str): Resolved dashboard ID.
              - dashboard_title (str): Resolved dashboard title.
              - widget_id (str): Pivot widget ID.
              - has_more_fields (bool): Always True for returned rows, since
                only widgets above the threshold are included.
              - field_count (int): Total number of fields (items) in the widget.

            If no dashboards are successfully processed, or no pivot widgets
            exceed the threshold, an empty list is returned and details are
            available in the logs.
        """
        self.logger.info("Starting widget field check for dashboards.")
        self.logger.debug(
            "Input dashboards parameter for widget field check: %s", dashboards
        )

        # Validate input
        if dashboards is None:
            error_msg = (
                "At least one dashboard reference (ID or name) is required "
                "for widget field analysis."
            )
            self.logger.error(error_msg)
            return []

        # Normalize to list of strings
        if isinstance(dashboards, str):
            dashboard_refs = [dashboards]
        else:
            dashboard_refs = [ref for ref in dashboards if isinstance(ref, str)]

        if not dashboard_refs:
            error_msg = "No valid dashboard references provided for widget field analysis."
            self.logger.error(error_msg)
            return []

        self.logger.info("Processing specified dashboards for widget fields: %s", dashboard_refs)

        results: List[Dict[str, Any]] = []
        total_dashboards = 0
        total_pivot_widgets = 0
        total_pivot_widgets_over_threshold = 0

        for ref in dashboard_refs:
            self.logger.info("Processing dashboard reference for widget fields: %s", ref)

            # Resolve ID and title using the Dashboard helper
            resolved = self.dashboard.resolve_dashboard_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(
                    "Skipping dashboard reference '%s' for widget fields: %s",
                    ref,
                    resolved.get("error"),
                )
                continue

            dashboard_id = resolved.get("dashboard_id")
            dashboard_title = resolved.get("dashboard_title") or ref

            if not dashboard_id:
                self.logger.warning(
                    "Resolved dashboard reference '%s' has no dashboard_id. "
                    "Skipping widget field analysis.",
                    ref,
                )
                continue

            # Fetch full dashboard definition (widgets, scripts, etc.)
            endpoint = f"/api/dashboards/{dashboard_id}?adminAccess=true"
            self.logger.debug(
                "Fetching full dashboard definition for widget fields from: %s",
                endpoint,
            )

            response = self.api_client.get(endpoint)
            if response is None:
                self.logger.warning(
                    "Failed to retrieve dashboard data for widget fields. "
                    "Dashboard OID: %s (Title: %s)",
                    dashboard_id,
                    dashboard_title,
                )
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(
                    "Failed to retrieve dashboard data for widget fields. "
                    "Dashboard OID: %s (Title: %s). Status: %s, Error: %s",
                    dashboard_id,
                    dashboard_title,
                    response.status_code,
                    error_body,
                )
                continue

            try:
                dashboard_data = response.json()
            except Exception as exc:
                self.logger.exception(
                    "Failed to parse dashboard JSON for widget fields '%s': %s",
                    dashboard_id,
                    exc,
                )
                continue

            (
                rows_for_dashboard,
                pivot_widget_found,
                pivot_widgets_over_threshold,
                pivot_widget_count,
            ) = self._compute_pivot_widget_field_details(
                dashboard_data=dashboard_data,
                resolved_title=dashboard_title,
                max_fields=max_fields,
            )

            if rows_for_dashboard:
                results.extend(rows_for_dashboard)

            total_dashboards += 1
            total_pivot_widgets += pivot_widget_count
            total_pivot_widgets_over_threshold += pivot_widgets_over_threshold

            if not pivot_widget_found:
                self.logger.info("Dashboard:%s has no pivot widgets", dashboard_title)

        if total_dashboards == 0:
            self.logger.warning(
                "No dashboards were successfully processed for widget field check."
            )
            return []

        # Summary logs
        self.logger.info("Total dashboards processed for widget fields: %d", total_dashboards)
        self.logger.info(
            "Total pivot widgets inspected across dashboards: %d", total_pivot_widgets
        )
        self.logger.info(
            "Total pivot widgets above field threshold (%d): %d",
            max_fields,
            total_pivot_widgets_over_threshold,
        )
        self.logger.info("Completed widget field check for dashboards.")

        return results

    # ------------------------------------------------------------------ #
    # Internal helpers for widget field metrics                          #
    # ------------------------------------------------------------------ #

    def _compute_pivot_widget_field_details(
        self,
        dashboard_data: Dict[str, Any],
        resolved_title: str,
        max_fields: int,
    ) -> Tuple[List[Dict[str, Any]], bool, int, int]:
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

        rows: List[Dict[str, Any]] = []
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
        widget: Dict[str, Any],
        dashboard_oid: str,
        dashboard_title: str,
        max_fields: int,
    ) -> Tuple[Optional[Dict[str, Any]], bool, int]:
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

        # Log field counts for pivot widgets

        if panel_count > max_fields:
            self.logger.info(
                "Dashboard:%s Pivot Widget: %s has %d fields",
                dashboard_title,
                widget_id,
                panel_count,
            )
            row: Dict[str, Any] = {
                "dashboard_id": dashboard_oid,
                "dashboard_title": dashboard_title,
                "widget_id": widget_id,
                "has_more_fields": True,
                "field_count": panel_count,
            }
            return row, True, 1

        # Below or equal to threshold: log but do not include in output rows
        self.logger.info(
            "Dashboard:%s Pivot Widget: %s has no more than %d fields",
            dashboard_title,
            widget_id,
            max_fields,
        )

        return None, True, 0

    # ------------------------------------------------------------------ #
    # WellCheck methods for datamodels                                   #
    # ------------------------------------------------------------------ #

    def check_datamodel_custom_tables(
        self,
        datamodels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Inspect custom tables in one or more data models and flag the use of UNION.

        This method resolves each data model reference (ID or title), retrieves
        its schema from the Sisense API, iterates through all datasets/tables,
        and returns one row per custom table with a flag indicating whether its
        SQL expression contains the word "union" (case-insensitive).

        Parameters
        ----------
        datamodels : list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a Sisense data model ID, or
              - a data model title (name).
            At least one data model reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.

        Returns
        -------
        list of dict
            A list with one entry per custom table. Each entry contains:
              - data_model (str): Data model title.
              - table (str): Table name.
              - has_union (str): "yes" if the custom table expression contains
                "union" (case-insensitive), otherwise "no".

            If no data models are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting custom table check for data models.")
        self.logger.debug(f"Input datamodels parameter: {datamodels}")

        # Validate input
        if datamodels is None:
            error_msg = "At least one data model reference (ID or name) is required."
            self.logger.error(error_msg)
            return []

        # Normalize to list of strings
        if isinstance(datamodels, str):
            datamodel_refs = [datamodels]
        else:
            datamodel_refs = [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid data model references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: List[Dict[str, Any]] = []
        total_tables = 0
        custom_tables = 0
        custom_tables_with_union = 0
        processed_datamodels = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            # Resolve ID and title using the DataModel helper
            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(
                    f"Skipping datamodel reference '{ref}': {resolved.get('error')}"
                )
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(
                    f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping."
                )
                continue

            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(f"Fetching datamodel schema from: {schema_endpoint}")

            response = self.api_client.get(schema_endpoint)
            if response is None:
                self.logger.warning(
                    f"Schema data is None or does not contain datasets for datamodel '{datamodel_title}'"
                )
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(
                    f"Failed to retrieve data for datamodel ID: {datamodel_id}"
                    f" with status {response.status_code}: {error_body}"
                )
                continue

            try:
                schema_data = response.json()
            except Exception as exc:
                self.logger.exception(
                    f"Failed to parse datamodel schema JSON for '{datamodel_id}': {exc}"
                )
                continue

            if not schema_data or "datasets" not in schema_data:
                self.logger.warning(
                    f"Schema data is None or does not contain datasets for datamodel '{datamodel_title}'"
                )
                continue

            processed_datamodels += 1

            for dataset in schema_data.get("datasets", []):
                schema = dataset.get("schema")

                if not isinstance(schema, dict) or "tables" not in schema:
                    self.logger.warning(
                        f"Schema or tables keys are missing in the dataset for datamodel '{datamodel_title}'"
                    )
                    continue

                tables = schema.get("tables") or []
                if not tables:
                    self.logger.warning(
                        f'No tables found in dataset {dataset.get("oid")} for datamodel {datamodel_title}'
                    )
                    continue

                for table in tables:
                    total_tables += 1

                    # Only look at custom tables
                    if table.get("type") != "custom":
                        continue

                    custom_tables += 1
                    table_name = table.get("name", "")

                    row: Dict[str, Any] = {
                        "data_model": datamodel_title,
                        "table": table_name,
                        "has_union": "no",
                    }

                    expr_container = table.get("expression")
                    if isinstance(expr_container, dict) and "expression" in expr_container:
                        expression = expr_container.get("expression")

                        if expression is None:
                            self.logger.warning(
                                f"Expression is null in table '{table_name}' for datamodel '{datamodel_title}'"
                            )
                        else:
                            expr_str = str(expression)
                            if "union" in expr_str.lower():
                                row["has_union"] = "yes"
                                custom_tables_with_union += 1
                            else:
                                self.logger.info(
                                    f"SQL expression does not contain 'union' for table '{table_name}' "
                                    f"for datamodel '{datamodel_title}'"
                                )
                    else:
                        self.logger.warning(
                            f"Expression not found for table '{table_name}' for datamodel '{datamodel_title}'"
                        )

                    results.append(row)

        if processed_datamodels == 0:
            self.logger.warning("No datamodels to process.")
            return []

        # summary logs
        self.logger.info(f"Processed {processed_datamodels} data models.")
        self.logger.info(f"Processed {total_tables} tables.")
        self.logger.info(f"Processed {custom_tables} custom tables.")
        self.logger.info(
            f"Found {custom_tables_with_union} custom tables using 'union'."
        )
        self.logger.info("Completed custom table check for data models.")

        return results

    def check_datamodel_island_tables(
        self,
        datamodels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Identify island tables (tables with no relationships) in one or more data models.

        This method retrieves the schema for each specified data model, inspects
        its relations and tables, and returns information about tables that do
        not participate in any relationship (often called "island tables").

        Parameters
        ----------
        datamodels : list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a Sisense data model ID, or
              - a data model title (name).
            At least one data model reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.

        Returns
        -------
        list of dict
            A list with one entry per island table. Each entry contains:
              - datamodel (str): Data model title.
              - datamodel_oid (str): Data model ID.
              - table (str): Table name.
              - table_oid (str): Table ID.
              - type (str): Table type (e.g., 'live', 'custom').
              - relation (str): Always "no" for island tables.

            If no data models are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting datamodel island tables check.")
        self.logger.debug(f"Input datamodels parameter: {datamodels}")

        # Validate input
        if datamodels is None:
            error_msg = "At least one datamodel reference (ID or name) is required."
            self.logger.error(error_msg)
            return []

        # Normalize to list of strings
        if isinstance(datamodels, str):
            datamodel_refs = [datamodels]
        else:
            datamodel_refs = [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        results: List[Dict[str, Any]] = []
        total_datamodels = 0
        total_tables = 0
        tables_without_relations = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            # Resolve ID and title using the Datamodel helper
            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(
                    f"Skipping datamodel reference '{ref}': {resolved.get('error')}"
                )
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(
                    f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping."
                )
                continue

            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(f"Fetching datamodel schema from: {schema_endpoint}")

            response = self.api_client.get(schema_endpoint)
            if response is None:
                self.logger.warning(
                    f'schema_data is None or no relations exist for the datamodel \'{datamodel_title}\''
                )
                self.logger.warning(
                    f'schema_data is None or does not contain datasets for datamodel \'{datamodel_title}\''
                )
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(
                    f"Failed to retrieve schema for datamodel '{datamodel_title}' "
                    f"({datamodel_id}). Status: {response.status_code}, Error: {error_body}"
                )
                continue

            try:
                schema_data = response.json()
            except Exception as exc:
                self.logger.exception(
                    f"Failed to parse schema JSON for datamodel '{datamodel_title}' "
                    f"({datamodel_id}): {exc}"
                )
                continue

            self.logger.info(f'\nStarting to process datamodel \'{datamodel_title}\'')

            (
                dm_results,
                dm_total_tables,
                dm_tables_without_relations,
            ) = self._compute_island_tables_for_datamodel(
                schema_data=schema_data,
                datamodel_id=datamodel_id,
                datamodel_title=datamodel_title,
            )

            if dm_results:
                results.extend(dm_results)

            total_datamodels += 1
            total_tables += dm_total_tables
            tables_without_relations += dm_tables_without_relations

        if total_datamodels == 0:
            self.logger.warning(
                "No datamodels were successfully processed for island tables check."
            )
            return []

        # Summary statistics
        self.logger.info(f"Processed {total_datamodels} data models.")
        self.logger.info(f"Processed {total_tables} tables.")
        self.logger.info(f"Found {tables_without_relations} Island tables.")
        self.logger.info("Completed datamodel island tables check.")

        return results

    # ------------------------------------------------------------------ #
    # Internal helpers for island table detection                        #
    # ------------------------------------------------------------------ #

    def _compute_island_tables_for_datamodel(
        self,
        schema_data: Dict[str, Any],
        datamodel_id: str,
        datamodel_title: str,
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        """
        Compute island tables for a single data model schema payload.
        """
        results: List[Dict[str, Any]] = []
        total_tables = 0
        tables_without_relations = 0

        datamodel_tables: List[Dict[str, Any]] = []
        relation_tables: List[str] = []
        island_tables: List[Dict[str, Any]] = []

        # Step 2 - Getting the list of tables which are involved in the relations of the DataModel
        if schema_data and "relations" in schema_data:
            for relation in schema_data["relations"]:
                if "columns" in relation:
                    for column in relation["columns"]:
                        if "table" in column:
                            relation_tables.append(column["table"])
                        else:
                            self.logger.warning(
                                f'table information is missing in one of the column in the relation \'{relation["oid"]}\' for datamdodel \'{datamodel_title}\''
                            )
                else:
                    self.logger.warning(
                        f'column information is missing in the relation \'{relation["oid"]}\' for datamdodel \'{datamodel_title}\''
                    )
        else:
            self.logger.warning(
                f'schema_data is None or no relations exist for the datamodel \'{datamodel_title}\''
            )

        # De-duping the relation_tables list
        relation_tables = list(set(relation_tables))

        # Step 3 - Getting the list of all the tables in DataModel
        if schema_data and "datasets" in schema_data:
            for dataset in schema_data["datasets"]:
                if "schema" in dataset and "tables" in dataset["schema"]:
                    tables = dataset["schema"]["tables"]

                    if not tables:
                        self.logger.warning(
                            f'No tables found in dataset {dataset["oid"]} for datamodel {datamodel_title}'
                        )
                        continue

                    for table in tables:
                        total_tables += 1
                        table_oid = table.get("oid")
                        table_name = table.get("name")
                        table_type = table.get("type")

                        new_dict: Dict[str, Any] = {
                            "datamodel": datamodel_title,
                            "datamodel_oid": datamodel_id,
                            "table": table_name,
                            "table_oid": table_oid,
                            "type": table_type,
                            "relation": "no",  # Default it to "no"
                        }

                        if table_oid in relation_tables:
                            new_dict["relation"] = "yes"
                        else:
                            tables_without_relations += 1
                            island_tables.append(new_dict)
                            results.append(new_dict)

                        datamodel_tables.append(new_dict)
                else:
                    self.logger.warning(
                        f'schema or tables keys are missing in the dataset for datamodel \'{datamodel_title}\''
                    )
        else:
            self.logger.warning(
                f'schema_data is None or does not contain datasets for datamodel \'{datamodel_title}\''
            )

        # Per-datamodel summary logs
        self.logger.info(
            f'Total Tables in the datamodel \'{datamodel_title}\': {len(datamodel_tables)}'
        )
        self.logger.info(
            f'Island tables in the datamodel \'{datamodel_title}\': {len(island_tables)}'
        )

        return results, total_tables, tables_without_relations

    def check_datamodel_rls_datatypes(
        self,
        datamodels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Inspect row-level security (RLS) rules for one or more data models and
        report the datatype of the columns used in those rules.

        This method resolves each data model reference, fetches its RLS (data
        security) rules from the appropriate API endpoint based on the data
        model type (extract or live), and returns one row per unique
        (datamodel, table, column, datatype) combination.

        Parameters
        ----------
        datamodels : list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a Sisense data model ID, or
              - a data model title (name).
            At least one data model reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.

        Returns
        -------
        list of dict
            A list with one entry per unique RLS column. Each entry contains:
              - datamodel (str): Data model title.
              - table (str): Table name where RLS is applied.
              - column (str): Column name used in the RLS rule.
              - datatype (str): Datatype reported by Sisense for that column.

            If no data models are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting RLS datatype inspection for data models.")
        self.logger.debug(f"Input datamodels parameter: {datamodels}")

        # Validate input
        if datamodels is None:
            error_msg = "At least one datamodel reference (ID or title) is required."
            self.logger.error(error_msg)
            return []

        # Normalize to list of strings
        if isinstance(datamodels, str):
            datamodel_refs = [datamodels]
        else:
            datamodel_refs = [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: List[Dict[str, Any]] = []
        total_datamodels_processed = 0
        total_rls = 0
        total_rls_non_numeric = 0

        for ref in datamodel_refs:
            # Resolve ID and title using the Datamodel helper
            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(
                    f"Skipping datamodel reference '{ref}': {resolved.get('error')}"
                )
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(
                    f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping."
                )
                continue

            self.logger.info(
                f"Starting to process datamodel '{datamodel_title}'"
            )

            # Fetch schema so we can read type/server needed for RLS endpoints
            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(
                f"Fetching datamodel schema for RLS inspection from: {schema_endpoint}"
            )

            schema_response = self.api_client.get(schema_endpoint)
            if schema_response is None:
                self.logger.warning(
                    f"Failed to retrieve schema for datamodel '{datamodel_title}' "
                    f"({datamodel_id})"
                )
                continue

            if schema_response.status_code != 200:
                try:
                    error_body = schema_response.json()
                except Exception:
                    error_body = getattr(schema_response, "text", "No response text")
                self.logger.warning(
                    f"Failed to retrieve schema for datamodel '{datamodel_title}' "
                    f"({datamodel_id}). Status: {schema_response.status_code}, "
                    f"Error: {error_body}"
                )
                continue

            try:
                schema_data = schema_response.json()
            except Exception as exc:
                self.logger.exception(
                    f"Failed to parse schema JSON for datamodel '{datamodel_title}' "
                    f"({datamodel_id}): {exc}"
                )
                continue

            datamodel_type = schema_data.get("type")
            datamodel_server = schema_data.get("server")

            if not datamodel_type or not datamodel_server:
                self.logger.warning(
                    f"Datamodel '{datamodel_title}' ({datamodel_id}) is missing "
                    f"'type' or 'server' in schema; cannot inspect RLS."
                )
                continue

            # Determine RLS endpoint based on datamodel type
            if datamodel_type == "extract":
                rls_endpoint = (
                    f"/api/elasticubes/{datamodel_server}/{datamodel_title}/datasecurity"
                )
            elif datamodel_type == "live":
                rls_endpoint = (
                    f"/api/v1/elasticubes/live/{datamodel_title}/datasecurity"
                )
            else:
                self.logger.warning(
                    f"Datamodel '{datamodel_title}' has unsupported type "
                    f"'{datamodel_type}' for RLS inspection."
                )
                continue

            self.logger.debug(
                f"Fetching data security rules from: {rls_endpoint} "
                f"(type={datamodel_type}, server={datamodel_server})"
            )

            rls_response = self.api_client.get(rls_endpoint)
            if rls_response is None:
                self.logger.warning(
                    f"No data security exists for the datamodel '{datamodel_title}'"
                )
                continue

            if rls_response.status_code != 200:
                try:
                    error_body = rls_response.json()
                except Exception:
                    error_body = getattr(rls_response, "text", "No response text")
                self.logger.warning(
                    f"Failed to retrieve data security rules for the datamodel "
                    f"'{datamodel_title}'. Status: {rls_response.status_code}, "
                    f"Error: {error_body}"
                )
                continue

            try:
                rls_data = rls_response.json()
            except Exception as exc:
                self.logger.exception(
                    f"Failed to parse data security rules JSON for datamodel "
                    f"'{datamodel_title}': {exc}"
                )
                continue

            if not rls_data:
                self.logger.warning(
                    f"No data security exists for the datamodel '{datamodel_title}'"
                )
                continue

            datamodel_rls: List[Dict[str, Any]] = []
            datamodel_rls_non_numeric: List[Dict[str, Any]] = []

            if isinstance(rls_data, list):
                for rls in rls_data:
                    if not rls:
                        self.logger.warning(
                            f"The datamodel '{datamodel_title}' contains an "
                            f"invalid Data Security Rule"
                        )
                        continue

                    new_rls_dict = {
                        "datamodel": datamodel_title,
                        "table": rls.get("table"),
                        "column": rls.get("column"),
                        "datatype": rls.get("datatype"),
                    }

                    if new_rls_dict not in datamodel_rls:
                        datamodel_rls.append(new_rls_dict)
                        results.append(new_rls_dict)
                        total_rls += 1

                        if new_rls_dict["datatype"] != "numeric":
                            datamodel_rls_non_numeric.append(new_rls_dict)
                            total_rls_non_numeric += 1
            else:
                self.logger.warning(
                    f"Unexpected data security payload type for datamodel "
                    f"'{datamodel_title}': {type(rls_data).__name__}"
                )
                continue

            total_datamodels_processed += 1

            self.logger.info(
                f"Total Data Security Rules in the datamodel "
                f"'{datamodel_title}': {len(datamodel_rls)}"
            )
            self.logger.info(
                f"Total Non-Numeric Data Security Rules in the datamodel "
                f"'{datamodel_title}': {len(datamodel_rls_non_numeric)}"
            )

        if total_datamodels_processed == 0:
            self.logger.warning(
                "No datamodels were successfully processed for RLS datatype inspection."
            )
            return []

        self.logger.info(f"Processed {total_datamodels_processed} data models.")
        self.logger.info(f"Processed {total_rls} data security rules.")
        self.logger.info(
            f"Found {total_rls_non_numeric} non-numeric data security rules."
        )
        self.logger.info("Completed RLS datatype inspection for data models.")

        return results

    def check_datamodel_import_queries(
        self,
        datamodels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Inspect tables in one or more data models for import queries.

        This method resolves each data model reference (ID or title), loads its
        schema, and checks every table for a ``configOptions.importQuery``
        configuration. For each table, it returns a row indicating whether an
        import query is configured.

        Parameters
        ----------
        datamodels : list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a Sisense data model ID, or
              - a data model title (name).
            At least one data model reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.

        Returns
        -------
        list of dict
            A list with one entry per table across all successfully processed
            data models. Each entry contains:
              - data_model (str): Resolved data model title.
              - table (str): Table name.
              - has_import_query (str): "yes" if an importQuery is present in
                the table config options, otherwise "no".

            If no data models are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting data model import-queries check.")
        self.logger.debug(f"Input datamodels parameter: {datamodels}")

        # Validate input
        if datamodels is None:
            error_msg = "At least one datamodel reference (ID or title) is required."
            self.logger.error(error_msg)
            return []

        # Normalize to list of strings
        if isinstance(datamodels, str):
            datamodel_refs = [datamodels]
        else:
            datamodel_refs = [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: List[Dict[str, Any]] = []
        total_datamodels = 0
        total_tables = 0
        tables_with_import_query = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                # Preserve style: warn when skipping unresolved references
                self.logger.warning(
                    f"Skipping datamodel reference '{ref}': {resolved.get('error')}"
                )
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(
                    f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping."
                )
                continue

            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(f"Fetching datamodel schema from: {schema_endpoint}")

            response = self.api_client.get(schema_endpoint)

            if response is None:
                # Mirrors original intent: unable to retrieve datamodel data
                self.logger.warning(
                    f"Failed to retrieve data for datamodel ID: {datamodel_id}"
                )
                # Also keep the schema_data-style warning text
                self.logger.warning(
                    f"schema_data is None or does not contain datasets for datamodel '{datamodel_title}'"
                )
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(
                    f"Failed to retrieve data for datamodel ID: {datamodel_id}. "
                    f"Status: {response.status_code}, Error: {error_body}"
                )
                continue

            try:
                schema_data = response.json()
            except Exception as exc:
                self.logger.exception(
                    f"Failed to parse schema JSON for datamodel '{datamodel_id}': {exc}"
                )
                continue

            if not schema_data or "datasets" not in schema_data:
                # Preserve original wording
                self.logger.warning(
                    f"schema_data is None or does not contain datasets for datamodel '{datamodel_title}'"
                )
                continue

            (
                datamodel_results,
                datamodel_tables,
                datamodel_import_query_tables,
            ) = self._compute_import_queries_for_datamodel(
                datamodel_title=datamodel_title,
                schema_data=schema_data,
            )

            if datamodel_results:
                results.extend(datamodel_results)
                total_datamodels += 1
                total_tables += datamodel_tables
                tables_with_import_query += datamodel_import_query_tables

        if total_datamodels == 0:
            # Keep a close variant of the original summary message
            self.logger.warning("No datamodels to process.")
            return []

        # Summary logs, preserving original lines
        self.logger.info(f"Processed {total_datamodels} data models.")
        self.logger.info(f"Processed {total_tables} tables.")
        self.logger.info(
            f"Found {tables_with_import_query} tables with import queries."
        )
        self.logger.info("Completed data model import-queries check.")

        return results

    def _compute_import_queries_for_datamodel(
        self,
        datamodel_title: str,
        schema_data: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], int, int]:
        """
        Internal helper to compute import-query flags for all tables
        in a single data model schema.
        """
        results: List[Dict[str, Any]] = []
        total_tables = 0
        tables_with_import_query = 0

        datasets = schema_data.get("datasets", [])
        for dataset in datasets:
            schema = dataset.get("schema")
            tables = schema.get("tables") if isinstance(schema, dict) else None

            if schema is None or tables is None:
                # Preserve original wording
                self.logger.warning(
                    f"Schema or tables keys are missing in the dataset for datamodel '{datamodel_title}'"
                )
                continue

            if not tables:
                # Preserve original wording
                self.logger.warning(
                    f'No tables found in dataset {dataset.get("oid")} for datamodel {datamodel_title}'
                )
                continue

            for table in tables:
                table_name = table.get("name", "Unknown")
                total_tables += 1

                row: Dict[str, Any] = {
                    "data_model": datamodel_title,
                    "table": table_name,
                    "has_import_query": "no",
                }

                if table and "configOptions" in table:
                    config_options = table.get("configOptions")

                    if config_options is None:
                        # Preserve original wording
                        self.logger.warning(
                            f"configOptions is null in table '{table_name}' "
                            f"for datamodel '{datamodel_title}'"
                        )
                    elif "importQuery" in config_options:
                        row["has_import_query"] = "yes"
                        tables_with_import_query += 1
                    else:
                        # Preserve original wording
                        self.logger.info(
                            f"importQuery not found in configOptions for table "
                            f"'{table_name}' for datamodel '{datamodel_title}'"
                        )
                else:
                    # Preserve original wording
                    self.logger.warning(
                        f"configOptions not found for table '{table_name}' "
                        f"for datamodel '{datamodel_title}'"
                    )

                results.append(row)

        return results, total_tables, tables_with_import_query

    def check_datamodel_m2m_relationships(
        self,
        datamodels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Check for potential many-to-many (M2M) relationships between tables
        in one or more data models.

        For each data model, this method inspects the relation graph, builds
        table/column pairs from the relations, and runs aggregate SQL queries
        against the data source to detect whether both sides of the relation
        contain duplicate keys. Pairs where each side has more than one
        occurrence of its key are flagged as many-to-many.

        Parameters
        ----------
        datamodels : list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a Sisense data model ID, or
              - a data model title (name).
            At least one data model reference is required. At runtime this
            method is tolerant of a single string being passed instead of a
            list, and will normalize it to a one-element list.

        Returns
        -------
        list of dict
            A list with one entry per relation field pair checked. Each entry
            contains:
              - data_model (str): Data model title.
              - left_table (str): Name of the left table.
              - left_column (str): Name of the left column.
              - right_table (str): Name of the right table.
              - right_column (str): Name of the right column.
              - is_m2m (bool): True when both sides have more than one
                occurrence of their key, False otherwise.

            If no data models are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting many-to-many (M2M) relationship check.")
        self.logger.debug(f"Input datamodels parameter: {datamodels}")

        if datamodels is None:
            error_msg = "At least one datamodel reference (ID or name) is required."
            self.logger.error(error_msg)
            return []

        if isinstance(datamodels, str):
            datamodel_refs = [datamodels]
        else:
            datamodel_refs = [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: List[Dict[str, Any]] = []
        total_datamodels_processed = 0
        total_pairs_checked = 0
        total_m2m = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(
                    f"Skipping datamodel reference '{ref}': {resolved.get('error')}"
                )
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(
                    f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping."
                )
                continue

            self.logger.debug(
                f"Resolved datamodel reference '{ref}' to ID '{datamodel_id}', "
                f"title '{datamodel_title}'."
            )

            # Collect relation-based table/column pairs for this datamodel
            pairs = self._collect_datamodel_relation_pairs_for_m2m(
                datamodel_id=datamodel_id,
                datamodel_title=datamodel_title,
            )

            if not pairs:
                self.logger.info(
                    f"No relation column pairs found for datamodel '{datamodel_title}'."
                )
                total_datamodels_processed += 1
                continue

            datasource_endpoint = f"/api/datasources/{datamodel_title}/sql"

            for pair in pairs:
                left_table = pair["left_table"]
                left_column = pair["left_column"]
                right_table = pair["right_table"]
                right_column = pair["right_column"]

                # Build the two aggregate queries
                query1 = (
                    f"select [{left_column}], count([{left_column}]) as key_count1 "
                    f"from [{left_table}] "
                    f"group by [{left_column}] "
                    f"having count([{left_column}]) > 1"
                )
                query2 = (
                    f"select [{right_column}], count([{right_column}]) as key_count2 "
                    f"from [{right_table}] "
                    f"group by [{right_column}] "
                    f"having count([{right_column}]) > 1"
                )

                # Execute queries as CSV
                resp1 = self.api_client.get(
                    datasource_endpoint,
                    params={"query": query1, "format": "csv"},
                )
                resp2 = self.api_client.get(
                    datasource_endpoint,
                    params={"query": query2, "format": "csv"},
                )

                def _count_rows_from_csv_response(response: Any) -> int:
                    if response is None:
                        return 0
                    if getattr(response, "status_code", None) != 200:
                        return 0
                    text = getattr(response, "text", "") or ""
                    if not text:
                        return 0
                    lines = [line for line in text.splitlines() if line.strip()]
                    if not lines:
                        return 0
                    # First line is assumed to be header
                    return max(len(lines) - 1, 0)

                count1 = _count_rows_from_csv_response(resp1)
                count2 = _count_rows_from_csv_response(resp2)

                is_m2m = count1 > 1 and count2 > 1

                # Preserve the original print-style output as a log line
                # Original: ec_name, left_table, left_column, right_table, right_column, is_m2m
                self.logger.info(
                    f"{datamodel_title}, {left_table}, {left_column}, "
                    f"{right_table}, {right_column}, {is_m2m}"
                )

                results.append(
                    {
                        "data_model": datamodel_title,
                        "left_table": left_table,
                        "left_column": left_column,
                        "right_table": right_table,
                        "right_column": right_column,
                        "is_m2m": is_m2m,
                    }
                )

                total_pairs_checked += 1
                if is_m2m:
                    total_m2m += 1

            total_datamodels_processed += 1

        if total_datamodels_processed == 0:
            self.logger.warning(
                "No datamodels were successfully processed for M2M checks."
            )
            return []

        self.logger.info(
            f"Processed {total_datamodels_processed} data models for many-to-many checks."
        )
        self.logger.info(f"Processed {total_pairs_checked} relation column pairs.")
        self.logger.info(f"Found {total_m2m} many-to-many relationships.")
        self.logger.info("Completed many-to-many (M2M) relationship check.")

        return results

    # ------------------------------------------------------------------ #
    # Internal helpers for M2M checks                                   #
    # ------------------------------------------------------------------ #

    def _collect_datamodel_relation_pairs_for_m2m(
        self,
        datamodel_id: str,
        datamodel_title: str,
    ) -> List[Dict[str, str]]:
        """
        Build unique table/column pairs from the relations of a single datamodel.
        """
        endpoint = f"/api/v2/datamodels/{datamodel_id}/schema/relations"
        self.logger.debug(
            f"Fetching relations for datamodel '{datamodel_title}' "
            f"from endpoint: {endpoint}"
        )

        response = self.api_client.get(endpoint)
        if response is None:
            self.logger.warning(
                f"Failed to retrieve relations for datamodel ID: {datamodel_id} "
                f"(Title: {datamodel_title})"
            )
            return []

        if response.status_code != 200:
            try:
                error_body = response.json()
            except Exception:
                error_body = getattr(response, "text", "No response text")
            self.logger.warning(
                f"Failed to retrieve relations for datamodel ID: {datamodel_id} "
                f"(Title: {datamodel_title}). Status: {response.status_code}, "
                f"Error: {error_body}"
            )
            return []

        try:
            relations = response.json()
        except Exception as exc:
            self.logger.exception(
                f"Failed to parse relations JSON for datamodel '{datamodel_title}': {exc}"
            )
            return []

        if not isinstance(relations, list):
            self.logger.warning(
                f"Unexpected relations payload type for datamodel '{datamodel_title}': "
                f"{type(relations)}"
            )
            return []

        pairs: List[Dict[str, str]] = []
        seen_keys: Set[Tuple[str, str, str, str]] = set()
        table_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

        for relation in relations:
            columns = relation.get("columns", [])
            if not isinstance(columns, list) or len(columns) < 2:
                continue

            for i in range(len(columns)):
                for j in range(i + 1, len(columns)):
                    left_ref = columns[i]
                    right_ref = columns[j]

                    left_details = self._get_table_details_for_m2m(
                        datamodel_id=datamodel_id,
                        column_ref=left_ref,
                        table_cache=table_cache,
                        datamodel_title=datamodel_title,
                    )
                    right_details = self._get_table_details_for_m2m(
                        datamodel_id=datamodel_id,
                        column_ref=right_ref,
                        table_cache=table_cache,
                        datamodel_title=datamodel_title,
                    )

                    if left_details is None or right_details is None:
                        continue

                    left_table_name = left_details.get("name") or str(
                        left_ref.get("table")
                    )
                    right_table_name = right_details.get("name") or str(
                        right_ref.get("table")
                    )

                    left_column_oid = left_ref.get("column")
                    right_column_oid = right_ref.get("column")

                    left_column_name = self._resolve_column_name_for_m2m(
                        table_details=left_details,
                        column_oid=left_column_oid,
                        datamodel_title=datamodel_title,
                    )
                    right_column_name = self._resolve_column_name_for_m2m(
                        table_details=right_details,
                        column_oid=right_column_oid,
                        datamodel_title=datamodel_title,
                    )

                    key = (
                        left_table_name,
                        left_column_name,
                        right_table_name,
                        right_column_name,
                    )
                    if key in seen_keys:
                        continue

                    seen_keys.add(key)
                    pairs.append(
                        {
                            "data_model": datamodel_title,
                            "left_table": left_table_name,
                            "left_column": left_column_name,
                            "right_table": right_table_name,
                            "right_column": right_column_name,
                        }
                    )

        return pairs

    def _get_table_details_for_m2m(
        self,
        datamodel_id: str,
        column_ref: Dict[str, Any],
        table_cache: Dict[Tuple[str, str], Dict[str, Any]],
        datamodel_title: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch and cache table details for a given dataset/table reference.
        """
        dataset_id = column_ref.get("dataset")
        table_id = column_ref.get("table")

        if not dataset_id or not table_id:
            self.logger.warning(
                f"Missing dataset or table reference in relation column for "
                f"datamodel '{datamodel_title}'."
            )
            return None

        cache_key = (str(dataset_id), str(table_id))
        if cache_key in table_cache:
            return table_cache[cache_key]

        endpoint = (
            f"/api/v2/datamodels/{datamodel_id}/schema/"
            f"datasets/{dataset_id}/tables/{table_id}"
        )
        self.logger.debug(
            f"Fetching table details for dataset '{dataset_id}', table '{table_id}' "
            f"in datamodel '{datamodel_title}' from endpoint: {endpoint}"
        )

        response = self.api_client.get(endpoint)
        if response is None:
            self.logger.warning(
                f"Failed to retrieve table details for dataset '{dataset_id}', "
                f"table '{table_id}' in datamodel '{datamodel_title}'."
            )
            return None

        if response.status_code != 200:
            try:
                error_body = response.json()
            except Exception:
                error_body = getattr(response, "text", "No response text")
            self.logger.warning(
                f"Failed to retrieve table details for dataset '{dataset_id}', "
                f"table '{table_id}' in datamodel '{datamodel_title}'. "
                f"Status: {response.status_code}, Error: {error_body}"
            )
            return None

        try:
            details = response.json()
        except Exception as exc:
            self.logger.exception(
                f"Failed to parse table JSON for dataset '{dataset_id}', "
                f"table '{table_id}' in datamodel '{datamodel_title}': {exc}"
            )
            return None

        table_cache[cache_key] = details
        return details

    def _resolve_column_name_for_m2m(
        self,
        table_details: Dict[str, Any],
        column_oid: Any,
        datamodel_title: str,
    ) -> str:
        """
        Resolve a column OID to a column name using table metadata.
        """
        columns = table_details.get("columns", [])
        if isinstance(columns, list):
            for col in columns:
                if col.get("oid") == column_oid:
                    name = col.get("name")
                    if isinstance(name, str) and name:
                        return name

        # Fallback if the column cannot be resolved
        self.logger.warning(
            f"Unable to resolve column OID '{column_oid}' to a name in datamodel "
            f"'{datamodel_title}'. Using OID as fallback."
        )
        return str(column_oid)

    def run_full_wellcheck(
        self,
        dashboards: Optional[Union[str, List[str]]] = None,
        datamodels: Optional[Union[str, List[str]]] = None,
        max_pivot_fields: int = 20,
    ) -> Dict[str, Any]:
        """
        Run a composite "full" wellcheck across dashboards and data models.

        This method is a convenience wrapper that orchestrates multiple
        dashboard-level and data-model-level checks and returns a structured
        report that groups their results.

        Parameters
        ----------
        dashboards : str or list of str, optional
            One or more dashboard references to analyze. Each reference can be:
              - a Sisense dashboard ID, or
              - a dashboard title (name).
            At runtime this parameter is tolerant of a single string and will
            normalize it to a one-element list.
        datamodels : str or list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a data model ID, or
              - a data model title (name).
            At runtime this parameter is tolerant of a single string and will
            normalize it to a one-element list.
        max_pivot_fields : int, optional
            Threshold used by the pivot-fields check. Any pivot widget with
            more than this number of fields is flagged.

        Returns
        -------
        dict
            A dictionary with two top-level sections:

            - "dashboards": {
                  "structure": [...],
                  "widget_counts": [...],
                  "pivot_widget_fields": [...],
              }

            - "datamodels": {
                  "custom_tables": [...],
                  "island_tables": [...],
                  "rls_datatypes": [...],
                  "import_queries": [...],
                  "m2m_relationships": [...],
                  "unused_columns": [...],
              }

            Each subsection contains the list of rows returned by the
            corresponding check method. If a given set of references is not
            provided or no assets are successfully processed, that subsection
            will be an empty list.
        """
        self.logger.info("Starting full wellcheck run.")

        # ------------------------------------------------------------------ #
        # Normalize dashboard references                                     #
        # ------------------------------------------------------------------ #
        if dashboards is None:
            dashboard_refs: List[str] = []
        elif isinstance(dashboards, str):
            dashboard_refs = [dashboards]
        else:
            dashboard_refs = [ref for ref in dashboards if isinstance(ref, str)]

        if not dashboard_refs:
            self.logger.info(
                "No dashboard references provided. Dashboard-level checks will be skipped."
            )

        # ------------------------------------------------------------------ #
        # Normalize datamodel references                                     #
        # ------------------------------------------------------------------ #
        if datamodels is None:
            datamodel_refs: List[str] = []
        elif isinstance(datamodels, str):
            datamodel_refs = [datamodels]
        else:
            datamodel_refs = [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            self.logger.info(
                "No data model references provided. Data model-level checks will be skipped."
            )

        dashboards_section: Dict[str, Any] = {
            "structure": [],
            "widget_counts": [],
            "pivot_widget_fields": [],
        }

        datamodels_section: Dict[str, Any] = {
            "custom_tables": [],
            "island_tables": [],
            "rls_datatypes": [],
            "import_queries": [],
            "m2m_relationships": [],
            "unused_columns": [],
        }

        # ------------------------------------------------------------------ #
        # Dashboard-level checks                                             #
        # ------------------------------------------------------------------ #
        if dashboard_refs:
            self.logger.info("Starting dashboard-level checks in run_full_wellcheck.")

            self.logger.info("Starting dashboard structure check.")
            dashboards_section["structure"] = self.check_dashboard_structure(
                dashboards=dashboard_refs
            )
            self.logger.info("Completed dashboard structure check.")

            self.logger.info("Starting dashboard widget-count check.")
            dashboards_section["widget_counts"] = self.check_dashboard_widget_counts(
                dashboards=dashboard_refs
            )
            self.logger.info("Completed dashboard widget-count check.")

            self.logger.info("Starting pivot widget-fields check.")
            dashboards_section["pivot_widget_fields"] = self.check_pivot_widget_fields(
                dashboards=dashboard_refs,
                max_fields=max_pivot_fields,
            )
            self.logger.info("Completed pivot widget-fields check.")

            self.logger.info("Completed dashboard-level checks in run_full_wellcheck.")

        # ------------------------------------------------------------------ #
        # Data-model-level checks                                            #
        # ------------------------------------------------------------------ #
        if datamodel_refs:
            self.logger.info("Starting data model-level checks in run_full_wellcheck.")

            self.logger.info("Starting custom-tables check.")
            datamodels_section["custom_tables"] = self.check_datamodel_custom_tables(
                datamodels=datamodel_refs
            )
            self.logger.info("Completed custom-tables check.")

            self.logger.info("Starting island-tables check.")
            datamodels_section["island_tables"] = self.check_datamodel_island_tables(
                datamodels=datamodel_refs
            )
            self.logger.info("Completed island-tables check.")

            self.logger.info("Starting RLS datatype check.")
            datamodels_section["rls_datatypes"] = self.check_datamodel_rls_datatypes(
                datamodels=datamodel_refs
            )
            self.logger.info("Completed RLS datatype check.")

            self.logger.info("Starting import-queries check.")
            datamodels_section["import_queries"] = self.check_datamodel_import_queries(
                datamodels=datamodel_refs
            )
            self.logger.info("Completed import-queries check.")

            self.logger.info("Starting many-to-many relationships check.")
            datamodels_section["m2m_relationships"] = self.check_datamodel_m2m_relationships(
                datamodels=datamodel_refs
            )
            self.logger.info("Completed many-to-many relationships check.")

            # Unused columns – delegated to AccessManagement
            self.logger.info("Starting unused-columns analysis (delegated to AccessManagement).")
            unused_columns: List[Dict[str, Any]] = []
            access_mgmt = getattr(self, "access_mgmt", None)

            if access_mgmt is None:
                self.logger.warning(
                    "WellCheck.access_mgmt is not configured. "
                    "Unused-columns analysis will be skipped in run_full_wellcheck."
                )
            else:
                unused_columns = access_mgmt.get_unused_columns_bulk(
                    datamodels=datamodel_refs
                )
                self.logger.info(
                    "Completed unused-columns analysis for %d data model reference(s). "
                    "Total result rows: %d",
                    len(datamodel_refs),
                    len(unused_columns),
                )

            datamodels_section["unused_columns"] = unused_columns

            self.logger.info("Completed data model-level checks in run_full_wellcheck.")

        self.logger.info("Full wellcheck run completed.")
        return {
            "dashboards": dashboards_section,
            "datamodels": datamodels_section,
        }
