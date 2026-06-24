from __future__ import annotations

from typing import Any


class ColumnsMixin:
    def get_datamodel_columns(self, datamodel_name: str) -> list[dict[str, Any]]:
        """Retrieve columns from a DataModel by collecting them from its datasets and tables.

        Resolves the DataModel ID by title, then walks its datasets and tables
        to gather every column.

        Parameters
        ----------
        datamodel_name : str
            The name of the DataModel from which to extract columns.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries, each containing ``datamodel_id``,
            ``datamodel_name``, ``table``, and ``column``. An empty list is
            returned if the DataModel cannot be found or has no columns.
        """
        all_columns = []

        self.logger.info(f"Fetching columns for DataModel: {datamodel_name}")

        # Step 1: Get DataModel ID
        self.logger.debug(f"Fetching DataModel ID for '{datamodel_name}'")
        schema_url = f"/api/v2/datamodels/schema?title={datamodel_name}"
        response = self.api_client.get(schema_url)

        if not response or response.status_code != 200:
            self.logger.error(f"Failed to fetch DataModel schema for '{datamodel_name}'")
            return []

        response_data = response.json()

        # Endpoint is already filtered by title; just extract the oid
        if isinstance(response_data, list):
            first_match = next(
                (x for x in response_data if isinstance(x, dict) and x.get("oid")),
                None,
            )
            datamodel_id = first_match.get("oid") if first_match else None
        elif isinstance(response_data, dict):
            datamodel_id = response_data.get("oid")
        else:
            datamodel_id = None

        if not datamodel_id:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []

        self.logger.info(f"DataModel ID for '{datamodel_name}': {datamodel_id}")

        # Step 2: Get DataSets
        self.logger.debug(f"Fetching DataSets for DataModel ID '{datamodel_id}'")
        dataset_url = f"/api/v2/datamodels/{datamodel_id}/schema/datasets"
        response = self.api_client.get(dataset_url)

        if not response or response.status_code != 200:
            self.logger.error(f"Failed to fetch DataSet schema for DataModel ID '{datamodel_id}'")
            return []

        response_data = response.json()
        dataset_ids = [x.get("oid") for x in response_data if isinstance(x, dict) and "oid" in x]

        if not dataset_ids:
            self.logger.warning(f"No datasets found for DataModel '{datamodel_name}' (ID: {datamodel_id}).")
            return []

        total_datasets = len(dataset_ids)
        self.logger.info(f"Found {total_datasets} datasets for DataModel '{datamodel_name}': {dataset_ids}")

        # Step 3: Loop through datasets and collect columns from tables
        total_tables = 0
        total_columns = 0

        for dataset_index, dataset_id in enumerate(dataset_ids, start=1):
            self.logger.debug(f"Processing DataSet {dataset_index}/{total_datasets}: Fetching tables for DataSet ID '{dataset_id}'")

            table_url = f"{dataset_url}/{dataset_id}/tables"
            response = self.api_client.get(table_url)

            if not response or response.status_code != 200:
                self.logger.error(f"Failed to fetch tables for DataSet ID '{dataset_id}'")
                continue

            tables = response.json()
            dataset_table_count = len(tables)
            total_tables += dataset_table_count
            self.logger.info(f"Dataset {dataset_index}: Found {dataset_table_count} tables in DataSet ID '{dataset_id}'")

            for table in tables:
                table_name = table.get("name")
                if not table_name:
                    self.logger.warning(f"Table in DataSet ID '{dataset_id}' has no name. Skipping.")
                    continue

                columns = table.get("columns")
                if not columns or not isinstance(columns, list):
                    self.logger.warning(f"Table '{table_name}' in DataSet ID '{dataset_id}' has no columns. Skipping.")
                    continue

                table_column_count = len(columns)
                total_columns += table_column_count
                self.logger.debug(f"Table '{table_name}' contains {table_column_count} columns")

                for column in columns:
                    column_name = column.get("name")
                    if not column_name:
                        self.logger.warning(f"A column in table '{table_name}' has no name. Skipping.")
                        continue

                    all_columns.append({"datamodel_id": datamodel_id, "datamodel_name": datamodel_name, "table": table_name, "column": column_name})

        # Step 4: Final logging
        self.logger.info(f"DataModel '{datamodel_name}': Processed {total_datasets} datasets, {total_tables} tables, and {total_columns} columns.")
        self.logger.debug(f"Final collected column data: {all_columns}")

        return all_columns

    def get_unused_columns(self, datamodel_name: str) -> list[dict[str, Any]]:
        """Identify unused columns in a DataModel by comparing all columns against dashboard usage.

        Compares every available column against the columns referenced in the
        dashboards associated with the DataModel. Coverage includes dashboard
        filters (dashboard-level, widget, and dependent filters) and widget
        panels (row, values, column panels, and measured filters).

        Parameters
        ----------
        datamodel_name : str
            The name of the DataModel to analyze.

        Returns
        -------
        list[dict[str, Any]]
            A list of column dictionaries, each with a ``used`` field set to
            ``True`` or ``False``. An empty list is returned if dashboards
            cannot be fetched.

        Raises
        ------
        ValueError
            If no columns are found for the given DataModel (for example, if it
            does not exist or is not accessible).
        """
        self.logger.info(f"Starting analysis for unused columns in DataModel: {datamodel_name}")

        # Step 1: Get all columns from the DataModel
        all_columns = self.get_datamodel_columns(datamodel_name)
        if not all_columns:
            self.logger.warning(f"No columns found for DataModel '{datamodel_name}'. Exiting.")
            # Treat this as an error condition: the DataModel likely does not exist or is not accessible.
            raise ValueError(f"No columns found for DataModel '{datamodel_name}'. The DataModel may not exist or may not be accessible.")

        total_datamodel_columns = len(all_columns)
        self.logger.info(f"Retrieved {total_datamodel_columns} columns from DataModel '{datamodel_name}'")

        # Step 2: Fetch dashboards associated with this DataModel
        self.logger.info(f"Fetching dashboards linked to DataModel '{datamodel_name}'")
        dashboard_url = f"/api/v1/dashboards/admin?dashboardType=owner&datasourceTitle={datamodel_name}"
        response = self.api_client.get(dashboard_url)

        if not response or not response.ok:
            self.logger.error(f"Failed to fetch dashboards for DataModel '{datamodel_name}'.")
            return []

        dashboards = response.json()
        if not dashboards:
            self.logger.warning(f"No dashboards found using DataModel '{datamodel_name}' or access is restricted.")
            # For a valid DataModel with no dashboards, treat all columns as unused.
            used_columns_count = 0
            unused_columns_count = len(all_columns)

            for entry in all_columns:
                entry["used"] = False

            self.logger.info(f"Total used columns: {used_columns_count}")
            self.logger.info(f"Total unused columns: {unused_columns_count}")

            return all_columns

        dashboard_ids = {dash["oid"] for dash in dashboards}  # Get unique dashboard IDs
        total_dashboards = len(dashboard_ids)
        self.logger.info(f"Found {total_dashboards} dashboards linked to DataModel '{datamodel_name}'")
        self.logger.debug(f"Dashboard IDs: {dashboard_ids}")

        # Step 3: Extract columns from all linked dashboards
        dashboard_columns = []
        total_filters = 0
        total_widgets = 0

        for dashboard_id in dashboard_ids:
            dashboard_url = f"/api/v1/dashboards/export?dashboardIds={dashboard_id}&adminAccess=true"
            response = self.api_client.get(dashboard_url)

            if not response or not response.ok:
                self.logger.error(f"Failed to export dashboard with ID '{dashboard_id}'")
                continue

            dashboard = response.json()[0]
            dashboard_name = dashboard["title"]
            self.logger.debug(f"Analyzing Dashboard '{dashboard_name}' (ID: {dashboard_id})")

            # Extract columns from filters
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
                            if "." in dim_value:
                                table, column = dim_value.strip("[]").split(".", 1)
                            else:
                                table, column = dim_value.strip("[]"), "Unknown Column"

                            dashboard_columns.append({"dashboard_name": dashboard_name, "source": "filter", "widget_id": "N/A", "table": table, "column": column})

                            self.logger.debug(f"Filter {filter_index}: Extracted from levels - Table: {table}, Column: {column}")

                    elif "jaql" in filter:
                        dim_value = filter["jaql"].get("dim", "Unknown.Table")
                        if "." in dim_value:
                            table, column = dim_value.strip("[]").split(".", 1)
                        else:
                            table, column = dim_value.strip("[]"), "Unknown Column"

                        dashboard_columns.append({"dashboard_name": dashboard_name, "source": "filter", "widget_id": "N/A", "table": table, "column": column})

                        self.logger.debug(f"Filter {filter_index}: Extracted from JAQL - Table: {table}, Column: {column}")

            self.logger.info(f"Processed {filter_count} filters for dashboard '{dashboard_name}'")

            # Extract columns from widgets
            widget_count = 0
            column_count = 0
            self.logger.debug(f"Extracting columns from widgets for dashboard '{dashboard_name}'")

            total_widgets_in_dashboard = len(dashboard.get("widgets", []))
            self.logger.debug(f"Total widgets found: {total_widgets_in_dashboard}")

            for widget_index, widget in enumerate(dashboard.get("widgets", []), start=1):
                widget_count += 1
                widget_id = widget.get("oid", "Unknown Widget")
                widget_title = widget.get("title", "Unnamed Widget")

                self.logger.debug(f"Processing widget {widget_index}/{total_widgets_in_dashboard}: '{widget_title}' (ID: {widget_id})")

                for panel in widget.get("metadata", {}).get("panels", []):
                    for item in panel.get("items", []):
                        jaql = item.get("jaql", {})

                        # Extract columns from 'context' (Formula-based columns)
                        if "context" in jaql and isinstance(jaql["context"], dict):
                            if not jaql["context"]:
                                self.logger.info(f"Widget {widget_index}: 'context' is an empty dict. Skipping context extraction.")
                                continue

                            for _, value in jaql["context"].items():
                                dim_value = value.get("dim", "Unknown.Table")
                                if "." in dim_value:
                                    table, column = dim_value.strip("[]").split(".", 1)
                                else:
                                    table, column = dim_value.strip("[]"), "Unknown Column"

                                dashboard_columns.append(
                                    {"datamodel_name": datamodel_name, "dashboard_name": dashboard_name, "source": "widget", "widget_id": widget_id, "table": table, "column": column}
                                )
                                column_count += 1

                                self.logger.debug(f"Widget {widget_index}: Extracted from context (Formula) - Table: {table}, Column: {column}")

                        # Extract columns from 'dim' (Regular columns)
                        else:
                            dim_value = jaql.get("dim", "Unknown.Table")
                            if not dim_value:
                                self.logger.info(f"Widget {widget_index}: Missing 'dim' in jaql. Skipping item.")
                                continue
                            if "." in dim_value:
                                table, column = dim_value.strip("[]").split(".", 1)
                            else:
                                table, column = dim_value.strip("[]"), "Unknown Column"

                            dashboard_columns.append({"datamodel_name": datamodel_name, "dashboard_name": dashboard_name, "source": "widget", "widget_id": widget_id, "table": table, "column": column})
                            column_count += 1

                            self.logger.debug(f"Widget {widget_index}: Extracted from regular source - Table: {table}, Column: {column}")

            total_widgets += widget_count
            self.logger.info(f"Processed {widget_count} widgets and {filter_count} filters and extracted {column_count} columns for dashboard '{dashboard_name}'")

        self.logger.info(f"Total filters processed: {total_filters}")
        self.logger.info(f"Total widgets processed: {total_widgets}")
        self.logger.info(f"Total dashboard columns extracted: {len(dashboard_columns)}")

        # Step 4: Identify used and unused columns
        dashboard_columns_set = set()

        for entry in dashboard_columns:
            table = entry["table"]
            column = entry["column"]

            # Fix issue: Remove "(Calendar)" from dashboard columns only
            if column.endswith(" (Calendar)"):
                column = column.replace(" (Calendar)", "").strip()

            dashboard_columns_set.add((table, column))

        used_columns_count = 0
        unused_columns_count = 0

        for entry in all_columns:
            table = entry["table"]
            column = entry["column"]

            # Check against cleaned dashboard column names
            entry["used"] = (table, column) in dashboard_columns_set

            if entry["used"]:
                used_columns_count += 1
            else:
                unused_columns_count += 1

        self.logger.info(f"Total used columns: {used_columns_count}")
        self.logger.info(f"Total unused columns: {unused_columns_count}")

        return all_columns

    def get_unused_columns_bulk(
        self,
        datamodels: str | list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Run unused-column analysis for one or more data models and return a
        combined result set.

        Parameters
        ----------
        datamodels : str or list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a data model ID, or
              - a data model title (name).
            At least one data model reference is required. At runtime this
            parameter is tolerant of a single string and will normalize it to a
            one-element list.

        Returns
        -------
        list of dict
            A flat list of rows across all processed data models. Each row has
            the same structure as returned by get_unused_columns().
            If no data models are successfully processed, an empty list is
            returned and details are available in the logs.
        """
        self.logger.info("Starting bulk unused-column analysis for data models.")
        self.logger.debug(f"Input datamodels parameter: {datamodels}")

        if datamodels is None:
            error_msg = "At least one data model reference (ID or name) is required."
            self.logger.error(error_msg)
            return []

        refs = [datamodels] if isinstance(datamodels, str) else [ref for ref in datamodels if isinstance(ref, str)]

        if not refs:
            error_msg = "No valid data model references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified data models: {refs}")

        all_results: list[dict[str, Any]] = []
        processed_count = 0

        for ref in refs:
            self.logger.info(f"Resolving data model reference: {ref}")
            resolved = self.datamodel.resolve_datamodel_reference(ref)

            if not resolved.get("success"):
                self.logger.warning(f"Skipping data model reference '{ref}': {resolved.get('error')}")
                continue

            datamodel_title = resolved.get("datamodel_title")
            if not datamodel_title:
                self.logger.warning(f"Resolved data model reference '{ref}' has no title. Skipping.")
                continue

            try:
                self.logger.info(f"Running unused-column analysis for data model '{datamodel_title}'")
                rows = self.get_unused_columns(datamodel_title)
            except ValueError as exc:
                # get_unused_columns raises ValueError when no columns found
                self.logger.warning(f"Skipping data model '{datamodel_title}' due to error: {exc}")
                continue

            all_results.extend(rows)
            processed_count += 1

        if processed_count == 0:
            self.logger.warning("No data models were successfully processed in get_unused_columns_bulk.")
            return []

        self.logger.info(
            "Completed unused-column analysis for %d data model(s). Total result rows: %d",
            processed_count,
            len(all_results),
        )
        return all_results
