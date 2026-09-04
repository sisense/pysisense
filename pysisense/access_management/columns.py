from __future__ import annotations

from typing import Any

from typing_extensions import deprecated

from ..utils import _discover_dashboards_on_datasource, _extract_dashboard_columns


class ColumnsMixin:
    def get_datamodel_columns(self, datamodel_name: str) -> list[dict[str, Any]]:
        """Retrieve every column of a DataModel as flat ``table`` / ``column`` rows.

        Resolves the DataModel by title, then reads its full schema
        (``GET /api/v2/datamodels/{oid}/schema``) in one call and flattens every
        table's columns. If that read yields nothing, falls back to walking the
        per-dataset ``/schema/datasets/{id}/tables`` endpoints, which some models
        answer when the full schema does not. Entries that are not well-formed
        (a ``null`` in a table or column list) are skipped rather than crashing.

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
        self.logger.info(f"Fetching columns for DataModel: {datamodel_name}")

        # Step 1: Get DataModel ID
        response = self.api_client.get("/api/v2/datamodels/schema", params={"title": datamodel_name})
        if not response or response.status_code != 200:
            self.logger.error(f"Failed to fetch DataModel schema for '{datamodel_name}'")
            return []
        try:
            response_data = response.json()
        except Exception:
            self.logger.exception(f"Failed to parse DataModel schema response for '{datamodel_name}'")
            return []
        if isinstance(response_data, list):
            first_match = next((x for x in response_data if isinstance(x, dict) and x.get("oid")), None)
            datamodel_id = first_match.get("oid") if first_match else None
        elif isinstance(response_data, dict):
            datamodel_id = response_data.get("oid")
        else:
            datamodel_id = None
        if not datamodel_id:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []
        self.logger.info(f"DataModel ID for '{datamodel_name}': {datamodel_id}")

        def rows_from_tables(tables: Any, where: str) -> list[dict[str, Any]]:
            rows: list[dict[str, Any]] = []
            for table in tables if isinstance(tables, list) else []:
                if not isinstance(table, dict):
                    self.logger.warning(f"Skipping a malformed table entry in {where}.")
                    continue
                table_name = table.get("name")
                if not table_name:
                    self.logger.warning(f"Table in {where} has no name. Skipping.")
                    continue
                columns = table.get("columns")
                if not isinstance(columns, list) or not columns:
                    self.logger.warning(f"Table '{table_name}' in {where} has no columns. Skipping.")
                    continue
                for column in columns:
                    if not isinstance(column, dict) or not column.get("name"):
                        self.logger.warning(f"A column in table '{table_name}' is malformed or has no name. Skipping.")
                        continue
                    rows.append({"datamodel_id": datamodel_id, "datamodel_name": datamodel_name, "table": table_name, "column": column["name"]})
            return rows

        # Step 2: Read the full schema in one call
        all_columns: list[dict[str, Any]] = []
        full = self.api_client.get(f"/api/v2/datamodels/{datamodel_id}/schema")
        if full is not None and full.status_code == 200:
            try:
                payload = full.json()
            except Exception:
                payload = None
                self.logger.exception(f"Failed to parse the full schema for DataModel ID '{datamodel_id}'")
            if isinstance(payload, dict):
                for dataset in payload.get("datasets") or []:
                    if not isinstance(dataset, dict):
                        continue
                    schema = dataset.get("schema") if isinstance(dataset.get("schema"), dict) else {}
                    all_columns.extend(rows_from_tables(schema.get("tables"), f"dataset '{dataset.get('oid')}'"))
        else:
            self.logger.debug(f"Full schema read failed for DataModel ID '{datamodel_id}'; falling back to the per-dataset endpoints.")

        # Step 3: Fallback — walk datasets and tables
        if not all_columns:
            dataset_url = f"/api/v2/datamodels/{datamodel_id}/schema/datasets"
            response = self.api_client.get(dataset_url)
            if not response or response.status_code != 200:
                self.logger.error(f"Failed to fetch DataSet schema for DataModel ID '{datamodel_id}'")
                return []
            try:
                datasets = response.json()
            except Exception:
                self.logger.exception(f"Failed to parse DataSet schema for DataModel ID '{datamodel_id}'")
                return []
            dataset_ids = [x.get("oid") for x in (datasets if isinstance(datasets, list) else []) if isinstance(x, dict) and x.get("oid")]
            if not dataset_ids:
                self.logger.warning(f"No datasets found for DataModel '{datamodel_name}' (ID: {datamodel_id}).")
                return []
            self.logger.info(f"Found {len(dataset_ids)} datasets for DataModel '{datamodel_name}': {dataset_ids}")
            for dataset_id in dataset_ids:
                response = self.api_client.get(f"{dataset_url}/{dataset_id}/tables")
                if not response or response.status_code != 200:
                    self.logger.error(f"Failed to fetch tables for DataSet ID '{dataset_id}'")
                    continue
                try:
                    tables = response.json()
                except Exception:
                    self.logger.exception(f"Failed to parse tables for DataSet ID '{dataset_id}'")
                    continue
                all_columns.extend(rows_from_tables(tables, f"DataSet ID '{dataset_id}'"))

        self.logger.info(f"Retrieved {len(all_columns)} columns across {len({r['table'] for r in all_columns})} tables for DataModel '{datamodel_name}'")
        return all_columns

    @deprecated("use get_unused_columns_bulk")
    def get_unused_columns(self, datamodel_name: str) -> list[dict[str, Any]]:
        """Identify unused columns in a single DataModel.

        Deprecated alias kept for backward compatibility (behavior frozen,
        including the ``ValueError`` below) — prefer
        :meth:`get_unused_columns_bulk`, which accepts a single reference or a
        list, resolves IDs as well as titles, and returns errors instead of
        raising.

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
        return self._unused_columns_for_model(datamodel_name)

    def _unused_columns_for_model(self, datamodel_name: str) -> list[dict[str, Any]]:
        """Identify unused columns in a DataModel by comparing all columns against dashboard usage.

        Compares every available column against the columns referenced in the
        dashboards associated with the DataModel. Coverage includes dashboard
        and default filters (plain, dependent and measured), drill hierarchies,
        widget panels (including nested formulas, conditional formatting and
        drill chains), drill history, widget query metadata and table headers;
        widgets and filters that point at a different datasource are not
        counted. Raises ``ValueError`` when no columns are found for the model.
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
        known_columns = {(entry.get("table"), entry.get("column")) for entry in all_columns}

        # Step 2: Fetch dashboards associated with this DataModel
        self.logger.info(f"Fetching dashboards linked to DataModel '{datamodel_name}'")
        discovered = _discover_dashboards_on_datasource(self.api_client, self.logger, datamodel_name)
        if discovered.get("ok") is False:
            self.logger.error(f"Failed to fetch dashboards for DataModel '{datamodel_name}': {discovered['error']}")
            return []

        dashboard_ids = set(discovered["matches"])
        if not dashboard_ids:
            self.logger.warning(f"No dashboards found using DataModel '{datamodel_name}' or access is restricted.")
            # For a valid DataModel with no dashboards, treat all columns as unused.
            for entry in all_columns:
                entry["used"] = False
            self.logger.info("Total used columns: 0")
            self.logger.info(f"Total unused columns: {len(all_columns)}")
            return all_columns

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

            exported = response.json()
            dashboard = exported[0] if isinstance(exported, list) and exported and isinstance(exported[0], dict) else None
            if dashboard is None:
                self.logger.error(f"Unexpected export structure for dashboard with ID '{dashboard_id}'")
                continue
            dashboard_name = dashboard.get("title", "Unknown Dashboard")
            self.logger.debug(f"Analyzing Dashboard '{dashboard_name}' (ID: {dashboard_id})")

            # Extract every column reference from filters and widgets (shared walk).
            # known_columns lets a dim whose names contain dots resolve against the schema.
            extracted = _extract_dashboard_columns(dashboard, dashboard_name, known_columns=known_columns, logger=self.logger, datasource=datamodel_name)
            dashboard_columns.extend(extracted)

            filter_count = len(dashboard.get("filters") or [])
            widget_count = len(dashboard.get("widgets") or [])
            total_filters += filter_count
            total_widgets += widget_count
            self.logger.info(f"Processed {widget_count} widgets and {filter_count} filters and extracted {len(extracted)} columns for dashboard '{dashboard_name}'")

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
        datamodels: str | list[str],
    ) -> dict[str, Any]:
        """
        Run unused-column analysis for one or more data models and return a
        combined per-model outcome.

        Parameters
        ----------
        datamodels : str or list of str
            One or more data model references to analyze. **Required.** Each
            reference can be:
              - a data model ID, or
              - a data model title (name).
            At runtime this parameter is tolerant of a single string and will
            normalize it to a one-element list.

        Returns
        -------
        dict[str, Any]
            Always a dict with ``"results"`` and ``"errors"``:
              - ``"results"``: flat list of column rows across all processed
                data models, each row shaped as ``get_datamodel_columns`` rows
                plus a ``"used"`` boolean. A model that resolves and genuinely
                has no columns in use contributes rows with ``used: False``.
              - ``"errors"``: list of ``{"ref": ..., "error": ...}`` entries,
                one per reference that could not be resolved or processed —
                empty when every reference succeeded.
            When **none** of the given references can be processed (or the
            input is invalid), the dict additionally carries ``"ok": False``
            and a top-level ``"error"`` summary, so standard failure detection
            (``payload.get("ok") is False``) still fires.
        """
        self.logger.info("Starting bulk unused-column analysis for data models.")
        self.logger.debug(f"Input datamodels parameter: {datamodels}")

        if datamodels is None:
            error_msg = "get_unused_columns_bulk requires at least one data model reference (ID or name)."
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg, "results": [], "errors": []}

        refs = [datamodels] if isinstance(datamodels, str) else [ref for ref in datamodels if isinstance(ref, str)]

        if not refs:
            error_msg = "No valid data model references provided — pass a data model ID or title, or a list of them."
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg, "results": [], "errors": []}

        self.logger.info(f"Processing specified data models: {refs}")

        all_results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        processed_count = 0

        for ref in refs:
            self.logger.info(f"Resolving data model reference: {ref}")
            resolved = self.datamodel.resolve_datamodel_reference(ref)

            if not resolved.get("success"):
                self.logger.warning(f"Skipping data model reference '{ref}': {resolved.get('error')}")
                errors.append({"ref": ref, "error": resolved.get("error", "Could not resolve data model reference.")})
                continue

            datamodel_title = resolved.get("datamodel_title")
            if not datamodel_title:
                self.logger.warning(f"Resolved data model reference '{ref}' has no title. Skipping.")
                errors.append({"ref": ref, "error": "Resolved data model has no title."})
                continue

            try:
                self.logger.info(f"Running unused-column analysis for data model '{datamodel_title}'")
                rows = self._unused_columns_for_model(datamodel_title)
            except ValueError as exc:
                # _unused_columns_for_model raises ValueError when no columns found
                self.logger.warning(f"Skipping data model '{datamodel_title}' due to error: {exc}")
                errors.append({"ref": ref, "error": str(exc)})
                continue

            all_results.extend(rows)
            processed_count += 1

        if processed_count == 0:
            # A silent empty result here would read as "no unused columns" to
            # consumers that count rows — fail loudly, naming each reference.
            failure_summary = "; ".join(f"'{f['ref']}': {f['error']}" for f in errors) or "no references given"
            error_msg = f"None of the given data model references could be processed — {failure_summary}"
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg, "results": [], "errors": errors}

        if errors:
            self.logger.warning(
                "get_unused_columns_bulk skipped %d unresolvable reference(s): %s",
                len(errors),
                [f["ref"] for f in errors],
            )

        self.logger.info(
            "Completed unused-column analysis for %d data model(s). Total result rows: %d",
            processed_count,
            len(all_results),
        )
        return {"results": all_results, "errors": errors}
