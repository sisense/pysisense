from __future__ import annotations


class ColumnsMixin:
    def get_dashboard_columns(self, dashboard_name):
        """
        Retrieves columns from a specific dashboard, including both widget and filter-level columns.

        This method:
        - Uses the `get_dashboard_by_name` method to fetch the dashboard.
        - Extracts columns from widgets and filters.
        - Deduplicates the final column list.

        Parameters:
            dashboard_name (str): The name of the dashboard to retrieve columns from.

        Returns:
            list: A list of dictionaries containing distinct table and column information from the dashboard.
        """
        self.logger.info(f"Starting column retrieval for dashboard: {dashboard_name}")

        dashboard_columns = []

        # Step 1: Get dashboard details using existing method
        dashboard = self.get_dashboard_by_name(dashboard_name)
        if not dashboard or "error" in dashboard:
            error_msg = f"Dashboard '{dashboard_name}' not found."
            self.logger.error(error_msg)
            return []
        dashboard_id = dashboard[0].get("oid")
        self.logger.info(f"Dashboard '{dashboard_name}' found with ID: {dashboard_id}")

        # Step 2: Export full dashboard metadata
        dashboard_url = f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true"
        dashboard_response = self.api_client.get(dashboard_url)

        if not dashboard_response or dashboard_response.status_code != 200:
            self.logger.error(f"Failed to export dashboard with ID '{dashboard_id}'")
            return []

        try:
            dashboard_data = dashboard_response.json()
        except Exception:
            self.logger.exception(f"Failed to parse dashboard export response for ID '{dashboard_id}'")
            return []

        if not dashboard_data or not isinstance(dashboard_data, list):
            self.logger.error(f"Unexpected dashboard data structure for ID '{dashboard_id}'")
            return []

        dashboard = dashboard_data[0]
        self.logger.debug(f"Analyzing dashboard '{dashboard['title']}' (ID: {dashboard_id})")

        # Step 3: Extract columns from filters
        filter_count = 0
        self.logger.debug(f"Extracting columns from filters for dashboard '{dashboard_name}'")

        if "filters" in dashboard:
            total_filters = len(dashboard["filters"])
            self.logger.debug(f"Total filters found: {total_filters}")

            for filter_index, filter in enumerate(dashboard["filters"], start=1):
                filter_count += 1
                self.logger.debug(f"Processing filter {filter_index}/{total_filters}")

                if "levels" in filter:
                    levels_count = len(filter["levels"])
                    self.logger.debug(f"Filter {filter_index}: Extracting {levels_count} levels")

                    for level in filter["levels"]:
                        dim_value = level.get("dim", "Unknown.Table")
                        table, column = dim_value.strip("[]").split(".", 1) if "." in dim_value else (dim_value.strip("[]"), "Unknown Column")

                        dashboard_columns.append({"dashboard_name": dashboard_name, "source": "filter", "widget_id": "N/A", "table": table, "column": column})

                        self.logger.debug(f"Filter {filter_index}: Extracted from levels - Table: {table}, Column: {column}")

                elif "jaql" in filter:
                    dim_value = filter["jaql"].get("dim", "Unknown.Table")
                    table, column = dim_value.strip("[]").split(".", 1) if "." in dim_value else (dim_value.strip("[]"), "Unknown Column")

                    dashboard_columns.append({"dashboard_name": dashboard_name, "source": "filter", "widget_id": "N/A", "table": table, "column": column})

                    self.logger.debug(f"Filter {filter_index}: Extracted from JAQL - Table: {table}, Column: {column}")

        self.logger.info(f"Processed {filter_count} filters for dashboard '{dashboard_name}'")

        # Step 4: Extract columns from widgets
        total_widgets = len(dashboard.get("widgets", []))
        column_count = 0

        self.logger.debug(f"Extracting columns from {total_widgets} widgets in dashboard '{dashboard_name}'")

        for widget_index, widget in enumerate(dashboard.get("widgets", []), start=1):
            # Safely access widget ID and handle potential issues
            try:
                columns = dashboard.get("layout", {}).get("columns", [])
                if not columns:
                    self.logger.warning(f"No columns found in dashboard layout for widget index {widget_index}")
                    widget_id = "Unknown Widget ID"
                else:
                    cells = columns[0].get("cells", [])
                    if len(cells) < widget_index:
                        self.logger.warning(f"Insufficient cells in layout for widget index {widget_index}")
                        widget_id = "Unknown Widget ID"
                    else:
                        subcells = cells[widget_index - 1].get("subcells", [])
                        if not subcells or not subcells[0].get("elements"):
                            self.logger.warning(f"No elements found in subcell for widget index {widget_index}")
                            widget_id = "Unknown Widget ID"
                        else:
                            widget_id = subcells[0]["elements"][0].get("widgetid", "Unknown Widget ID")
            except Exception:
                self.logger.exception(f"Exception occurred while extracting widget ID for index {widget_index}")
                widget_id = "Unknown Widget ID"

            widget_title = widget.get("title", "Unnamed Widget")

            self.logger.debug(f"Processing widget {widget_index}/{total_widgets} - ID: {widget_id}, Title: {widget_title}")

            for panel in widget.get("metadata", {}).get("panels", []):
                for item in panel.get("items", []):
                    jaql = item.get("jaql", {})

                    # Case 1: Extract from 'context' (Formula-based columns)
                    if "context" in jaql and isinstance(jaql["context"], dict):
                        for context_key, value in jaql["context"].items():
                            dim_value = value.get("dim", "Unknown.Table")
                            if "." in dim_value:
                                table, column = dim_value.strip("[]").split(".", 1)
                            else:
                                table, column = dim_value.strip("[]"), "Unknown Column"

                            dashboard_columns.append({"dashboard_name": dashboard_name, "source": "widget", "widget_id": widget_id, "table": table, "column": column})
                            column_count += 1

                            self.logger.debug(f"Widget {widget_index}: Extracted from context (Formula) - Key: {context_key}, Table: {table}, Column: {column}")

                    # Case 2: Extract from 'dim' (Regular columns)
                    else:
                        dim_value = jaql.get("dim", "Unknown.Table")
                        if "." in dim_value:
                            table, column = dim_value.strip("[]").split(".", 1)
                        else:
                            table, column = dim_value.strip("[]"), "Unknown Column"

                        dashboard_columns.append({"dashboard_name": dashboard_name, "source": "widget", "widget_id": widget_id, "table": table, "column": column})
                        column_count += 1

                        self.logger.debug(f"Widget {widget_index}: Extracted from regular source - Table: {table}, Column: {column}")

        self.logger.info(f"Processed {total_widgets} widgets and extracted {column_count} columns for dashboard '{dashboard_name}'")

        # Step 5: Deduplicate columns based on 'table' and 'column'
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
