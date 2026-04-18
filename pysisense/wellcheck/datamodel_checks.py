from __future__ import annotations

from typing import Any


class DatamodelChecksMixin:
    def check_datamodel_custom_tables(
        self,
        datamodels: list[str] | None = None,
    ) -> list[dict[str, Any]]:
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
        datamodel_refs = [datamodels] if isinstance(datamodels, str) else [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid data model references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: list[dict[str, Any]] = []
        total_tables = 0
        custom_tables = 0
        custom_tables_with_union = 0
        processed_datamodels = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            # Resolve ID and title using the DataModel helper
            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(f"Skipping datamodel reference '{ref}': {resolved.get('error')}")
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping.")
                continue

            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(f"Fetching datamodel schema from: {schema_endpoint}")

            response = self.api_client.get(schema_endpoint)
            if response is None:
                self.logger.warning(f"Schema data is None or does not contain datasets for datamodel '{datamodel_title}'")
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(f"Failed to retrieve data for datamodel ID: {datamodel_id} with status {response.status_code}: {error_body}")
                continue

            try:
                schema_data = response.json()
            except Exception as exc:
                self.logger.exception(f"Failed to parse datamodel schema JSON for '{datamodel_id}': {exc}")
                continue

            if not schema_data or "datasets" not in schema_data:
                self.logger.warning(f"Schema data is None or does not contain datasets for datamodel '{datamodel_title}'")
                continue

            processed_datamodels += 1

            for dataset in schema_data.get("datasets", []):
                schema = dataset.get("schema")

                if not isinstance(schema, dict) or "tables" not in schema:
                    self.logger.warning(f"Schema or tables keys are missing in the dataset for datamodel '{datamodel_title}'")
                    continue

                tables = schema.get("tables") or []
                if not tables:
                    self.logger.warning(f"No tables found in dataset {dataset.get('oid')} for datamodel {datamodel_title}")
                    continue

                for table in tables:
                    total_tables += 1

                    # Only look at custom tables
                    if table.get("type") != "custom":
                        continue

                    custom_tables += 1
                    table_name = table.get("name", "")

                    row: dict[str, Any] = {
                        "data_model": datamodel_title,
                        "table": table_name,
                        "has_union": "no",
                    }

                    expr_container = table.get("expression")
                    if isinstance(expr_container, dict) and "expression" in expr_container:
                        expression = expr_container.get("expression")

                        if expression is None:
                            self.logger.warning(f"Expression is null in table '{table_name}' for datamodel '{datamodel_title}'")
                        else:
                            expr_str = str(expression)
                            if "union" in expr_str.lower():
                                row["has_union"] = "yes"
                                custom_tables_with_union += 1
                            else:
                                self.logger.info(f"SQL expression does not contain 'union' for table '{table_name}' for datamodel '{datamodel_title}'")
                    else:
                        self.logger.warning(f"Expression not found for table '{table_name}' for datamodel '{datamodel_title}'")

                    results.append(row)

        if processed_datamodels == 0:
            self.logger.warning("No datamodels to process.")
            return []

        # summary logs
        self.logger.info(f"Processed {processed_datamodels} data models.")
        self.logger.info(f"Processed {total_tables} tables.")
        self.logger.info(f"Processed {custom_tables} custom tables.")
        self.logger.info(f"Found {custom_tables_with_union} custom tables using 'union'.")
        self.logger.info("Completed custom table check for data models.")

        return results

    def check_datamodel_island_tables(
        self,
        datamodels: list[str] | None = None,
    ) -> list[dict[str, Any]]:
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
        datamodel_refs = [datamodels] if isinstance(datamodels, str) else [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        results: list[dict[str, Any]] = []
        total_datamodels = 0
        total_tables = 0
        tables_without_relations = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            # Resolve ID and title using the Datamodel helper
            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(f"Skipping datamodel reference '{ref}': {resolved.get('error')}")
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping.")
                continue

            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(f"Fetching datamodel schema from: {schema_endpoint}")

            response = self.api_client.get(schema_endpoint)
            if response is None:
                self.logger.warning(f"schema_data is None or no relations exist for the datamodel '{datamodel_title}'")
                self.logger.warning(f"schema_data is None or does not contain datasets for datamodel '{datamodel_title}'")
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(f"Failed to retrieve schema for datamodel '{datamodel_title}' ({datamodel_id}). Status: {response.status_code}, Error: {error_body}")
                continue

            try:
                schema_data = response.json()
            except Exception as exc:
                self.logger.exception(f"Failed to parse schema JSON for datamodel '{datamodel_title}' ({datamodel_id}): {exc}")
                continue

            self.logger.info(f"\nStarting to process datamodel '{datamodel_title}'")

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
            self.logger.warning("No datamodels were successfully processed for island tables check.")
            return []

        # Summary statistics
        self.logger.info(f"Processed {total_datamodels} data models.")
        self.logger.info(f"Processed {total_tables} tables.")
        self.logger.info(f"Found {tables_without_relations} Island tables.")
        self.logger.info("Completed datamodel island tables check.")

        return results

    def _compute_island_tables_for_datamodel(
        self,
        schema_data: dict[str, Any],
        datamodel_id: str,
        datamodel_title: str,
    ) -> tuple[list[dict[str, Any]], int, int]:
        """
        Compute island tables for a single data model schema payload.
        """
        results: list[dict[str, Any]] = []
        total_tables = 0
        tables_without_relations = 0

        datamodel_tables: list[dict[str, Any]] = []
        relation_tables: list[str] = []
        island_tables: list[dict[str, Any]] = []

        # Step 2 - Getting the list of tables which are involved in the relations of the DataModel
        if schema_data and "relations" in schema_data:
            for relation in schema_data["relations"]:
                if "columns" in relation:
                    for column in relation["columns"]:
                        if "table" in column:
                            relation_tables.append(column["table"])
                        else:
                            self.logger.warning(f"table information is missing in one of the column in the relation '{relation['oid']}' for datamdodel '{datamodel_title}'")
                else:
                    self.logger.warning(f"column information is missing in the relation '{relation['oid']}' for datamdodel '{datamodel_title}'")
        else:
            self.logger.warning(f"schema_data is None or no relations exist for the datamodel '{datamodel_title}'")

        # De-duping the relation_tables list
        relation_tables = list(set(relation_tables))

        # Step 3 - Getting the list of all the tables in DataModel
        if schema_data and "datasets" in schema_data:
            for dataset in schema_data["datasets"]:
                if "schema" in dataset and "tables" in dataset["schema"]:
                    tables = dataset["schema"]["tables"]

                    if not tables:
                        self.logger.warning(f"No tables found in dataset {dataset['oid']} for datamodel {datamodel_title}")
                        continue

                    for table in tables:
                        total_tables += 1
                        table_oid = table.get("oid")
                        table_name = table.get("name")
                        table_type = table.get("type")

                        new_dict: dict[str, Any] = {
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
                    self.logger.warning(f"schema or tables keys are missing in the dataset for datamodel '{datamodel_title}'")
        else:
            self.logger.warning(f"schema_data is None or does not contain datasets for datamodel '{datamodel_title}'")

        # Per-datamodel summary logs
        self.logger.info(f"Total Tables in the datamodel '{datamodel_title}': {len(datamodel_tables)}")
        self.logger.info(f"Island tables in the datamodel '{datamodel_title}': {len(island_tables)}")

        return results, total_tables, tables_without_relations

    def check_datamodel_rls_datatypes(
        self,
        datamodels: list[str] | None = None,
    ) -> list[dict[str, Any]]:
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
        datamodel_refs = [datamodels] if isinstance(datamodels, str) else [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: list[dict[str, Any]] = []
        total_datamodels_processed = 0
        total_rls = 0
        total_rls_non_numeric = 0

        for ref in datamodel_refs:
            # Resolve ID and title using the Datamodel helper
            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(f"Skipping datamodel reference '{ref}': {resolved.get('error')}")
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping.")
                continue

            self.logger.info(f"Starting to process datamodel '{datamodel_title}'")

            # Fetch schema so we can read type/server needed for RLS endpoints
            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(f"Fetching datamodel schema for RLS inspection from: {schema_endpoint}")

            schema_response = self.api_client.get(schema_endpoint)
            if schema_response is None:
                self.logger.warning(f"Failed to retrieve schema for datamodel '{datamodel_title}' ({datamodel_id})")
                continue

            if schema_response.status_code != 200:
                try:
                    error_body = schema_response.json()
                except Exception:
                    error_body = getattr(schema_response, "text", "No response text")
                self.logger.warning(f"Failed to retrieve schema for datamodel '{datamodel_title}' ({datamodel_id}). Status: {schema_response.status_code}, Error: {error_body}")
                continue

            try:
                schema_data = schema_response.json()
            except Exception as exc:
                self.logger.exception(f"Failed to parse schema JSON for datamodel '{datamodel_title}' ({datamodel_id}): {exc}")
                continue

            datamodel_type = schema_data.get("type")
            datamodel_server = schema_data.get("server")

            if not datamodel_type or not datamodel_server:
                self.logger.warning(f"Datamodel '{datamodel_title}' ({datamodel_id}) is missing 'type' or 'server' in schema; cannot inspect RLS.")
                continue

            # Determine RLS endpoint based on datamodel type
            if datamodel_type == "extract":
                rls_endpoint = f"/api/elasticubes/{datamodel_server}/{datamodel_title}/datasecurity"
            elif datamodel_type == "live":
                rls_endpoint = f"/api/v1/elasticubes/live/{datamodel_title}/datasecurity"
            else:
                self.logger.warning(f"Datamodel '{datamodel_title}' has unsupported type '{datamodel_type}' for RLS inspection.")
                continue

            self.logger.debug(f"Fetching data security rules from: {rls_endpoint} (type={datamodel_type}, server={datamodel_server})")

            rls_response = self.api_client.get(rls_endpoint)
            if rls_response is None:
                self.logger.warning(f"No data security exists for the datamodel '{datamodel_title}'")
                continue

            if rls_response.status_code != 200:
                try:
                    error_body = rls_response.json()
                except Exception:
                    error_body = getattr(rls_response, "text", "No response text")
                self.logger.warning(f"Failed to retrieve data security rules for the datamodel '{datamodel_title}'. Status: {rls_response.status_code}, Error: {error_body}")
                continue

            try:
                rls_data = rls_response.json()
            except Exception as exc:
                self.logger.exception(f"Failed to parse data security rules JSON for datamodel '{datamodel_title}': {exc}")
                continue

            if not rls_data:
                self.logger.warning(f"No data security exists for the datamodel '{datamodel_title}'")
                continue

            datamodel_rls: list[dict[str, Any]] = []
            datamodel_rls_non_numeric: list[dict[str, Any]] = []

            if isinstance(rls_data, list):
                for rls in rls_data:
                    if not rls:
                        self.logger.warning(f"The datamodel '{datamodel_title}' contains an invalid Data Security Rule")
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
                self.logger.warning(f"Unexpected data security payload type for datamodel '{datamodel_title}': {type(rls_data).__name__}")
                continue

            total_datamodels_processed += 1

            self.logger.info(f"Total Data Security Rules in the datamodel '{datamodel_title}': {len(datamodel_rls)}")
            self.logger.info(f"Total Non-Numeric Data Security Rules in the datamodel '{datamodel_title}': {len(datamodel_rls_non_numeric)}")

        if total_datamodels_processed == 0:
            self.logger.warning("No datamodels were successfully processed for RLS datatype inspection.")
            return []

        self.logger.info(f"Processed {total_datamodels_processed} data models.")
        self.logger.info(f"Processed {total_rls} data security rules.")
        self.logger.info(f"Found {total_rls_non_numeric} non-numeric data security rules.")
        self.logger.info("Completed RLS datatype inspection for data models.")

        return results

    def check_datamodel_import_queries(
        self,
        datamodels: list[str] | None = None,
    ) -> list[dict[str, Any]]:
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
        datamodel_refs = [datamodels] if isinstance(datamodels, str) else [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: list[dict[str, Any]] = []
        total_datamodels = 0
        total_tables = 0
        tables_with_import_query = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                # Preserve style: warn when skipping unresolved references
                self.logger.warning(f"Skipping datamodel reference '{ref}': {resolved.get('error')}")
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping.")
                continue

            schema_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema"
            self.logger.debug(f"Fetching datamodel schema from: {schema_endpoint}")

            response = self.api_client.get(schema_endpoint)

            if response is None:
                # Mirrors original intent: unable to retrieve datamodel data
                self.logger.warning(f"Failed to retrieve data for datamodel ID: {datamodel_id}")
                # Also keep the schema_data-style warning text
                self.logger.warning(f"schema_data is None or does not contain datasets for datamodel '{datamodel_title}'")
                continue

            if response.status_code != 200:
                try:
                    error_body = response.json()
                except Exception:
                    error_body = getattr(response, "text", "No response text")
                self.logger.warning(f"Failed to retrieve data for datamodel ID: {datamodel_id}. Status: {response.status_code}, Error: {error_body}")
                continue

            try:
                schema_data = response.json()
            except Exception as exc:
                self.logger.exception(f"Failed to parse schema JSON for datamodel '{datamodel_id}': {exc}")
                continue

            if not schema_data or "datasets" not in schema_data:
                # Preserve original wording
                self.logger.warning(f"schema_data is None or does not contain datasets for datamodel '{datamodel_title}'")
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
        self.logger.info(f"Found {tables_with_import_query} tables with import queries.")
        self.logger.info("Completed data model import-queries check.")

        return results

    def _compute_import_queries_for_datamodel(
        self,
        datamodel_title: str,
        schema_data: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], int, int]:
        """
        Internal helper to compute import-query flags for all tables
        in a single data model schema.
        """
        results: list[dict[str, Any]] = []
        total_tables = 0
        tables_with_import_query = 0

        datasets = schema_data.get("datasets", [])
        for dataset in datasets:
            schema = dataset.get("schema")
            tables = schema.get("tables") if isinstance(schema, dict) else None

            if schema is None or tables is None:
                # Preserve original wording
                self.logger.warning(f"Schema or tables keys are missing in the dataset for datamodel '{datamodel_title}'")
                continue

            if not tables:
                # Preserve original wording
                self.logger.warning(f"No tables found in dataset {dataset.get('oid')} for datamodel {datamodel_title}")
                continue

            for table in tables:
                table_name = table.get("name", "Unknown")
                total_tables += 1

                row: dict[str, Any] = {
                    "data_model": datamodel_title,
                    "table": table_name,
                    "has_import_query": "no",
                }

                if table and "configOptions" in table:
                    config_options = table.get("configOptions")

                    if config_options is None:
                        # Preserve original wording
                        self.logger.warning(f"configOptions is null in table '{table_name}' for datamodel '{datamodel_title}'")
                    elif "importQuery" in config_options:
                        row["has_import_query"] = "yes"
                        tables_with_import_query += 1
                    else:
                        # Preserve original wording
                        self.logger.info(f"importQuery not found in configOptions for table '{table_name}' for datamodel '{datamodel_title}'")
                else:
                    # Preserve original wording
                    self.logger.warning(f"configOptions not found for table '{table_name}' for datamodel '{datamodel_title}'")

                results.append(row)

        return results, total_tables, tables_with_import_query

    def check_datamodel_m2m_relationships(
        self,
        datamodels: list[str] | None = None,
    ) -> list[dict[str, Any]]:
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

        datamodel_refs = [datamodels] if isinstance(datamodels, str) else [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            error_msg = "No valid datamodel references provided."
            self.logger.error(error_msg)
            return []

        self.logger.info(f"Processing specified datamodels: {datamodel_refs}")

        results: list[dict[str, Any]] = []
        total_datamodels_processed = 0
        total_pairs_checked = 0
        total_m2m = 0

        for ref in datamodel_refs:
            self.logger.info(f"Processing datamodel reference: {ref}")

            resolved = self.datamodel.resolve_datamodel_reference(ref)
            if not resolved.get("success"):
                self.logger.warning(f"Skipping datamodel reference '{ref}': {resolved.get('error')}")
                continue

            datamodel_id = resolved.get("datamodel_id")
            datamodel_title = resolved.get("datamodel_title") or ref

            if not datamodel_id:
                self.logger.warning(f"Resolved datamodel reference '{ref}' has no datamodel_id. Skipping.")
                continue

            self.logger.debug(f"Resolved datamodel reference '{ref}' to ID '{datamodel_id}', title '{datamodel_title}'.")

            # Collect relation-based table/column pairs for this datamodel
            pairs = self._collect_datamodel_relation_pairs_for_m2m(
                datamodel_id=datamodel_id,
                datamodel_title=datamodel_title,
            )

            if not pairs:
                self.logger.info(f"No relation column pairs found for datamodel '{datamodel_title}'.")
                total_datamodels_processed += 1
                continue

            datasource_endpoint = f"/api/datasources/{datamodel_title}/sql"

            for pair in pairs:
                left_table = pair["left_table"]
                left_column = pair["left_column"]
                right_table = pair["right_table"]
                right_column = pair["right_column"]

                # Build the two aggregate queries
                query1 = f"select [{left_column}], count([{left_column}]) as key_count1 from [{left_table}] group by [{left_column}] having count([{left_column}]) > 1"
                query2 = f"select [{right_column}], count([{right_column}]) as key_count2 from [{right_table}] group by [{right_column}] having count([{right_column}]) > 1"

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
                self.logger.info(f"{datamodel_title}, {left_table}, {left_column}, {right_table}, {right_column}, {is_m2m}")

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
            self.logger.warning("No datamodels were successfully processed for M2M checks.")
            return []

        self.logger.info(f"Processed {total_datamodels_processed} data models for many-to-many checks.")
        self.logger.info(f"Processed {total_pairs_checked} relation column pairs.")
        self.logger.info(f"Found {total_m2m} many-to-many relationships.")
        self.logger.info("Completed many-to-many (M2M) relationship check.")

        return results

    def _collect_datamodel_relation_pairs_for_m2m(
        self,
        datamodel_id: str,
        datamodel_title: str,
    ) -> list[dict[str, str]]:
        """
        Build unique table/column pairs from the relations of a single datamodel.
        """
        endpoint = f"/api/v2/datamodels/{datamodel_id}/schema/relations"
        self.logger.debug(f"Fetching relations for datamodel '{datamodel_title}' from endpoint: {endpoint}")

        response = self.api_client.get(endpoint)
        if response is None:
            self.logger.warning(f"Failed to retrieve relations for datamodel ID: {datamodel_id} (Title: {datamodel_title})")
            return []

        if response.status_code != 200:
            try:
                error_body = response.json()
            except Exception:
                error_body = getattr(response, "text", "No response text")
            self.logger.warning(f"Failed to retrieve relations for datamodel ID: {datamodel_id} (Title: {datamodel_title}). Status: {response.status_code}, Error: {error_body}")
            return []

        try:
            relations = response.json()
        except Exception as exc:
            self.logger.exception(f"Failed to parse relations JSON for datamodel '{datamodel_title}': {exc}")
            return []

        if not isinstance(relations, list):
            self.logger.warning(f"Unexpected relations payload type for datamodel '{datamodel_title}': {type(relations)}")
            return []

        pairs: list[dict[str, str]] = []
        seen_keys: set[tuple[str, str, str, str]] = set()
        table_cache: dict[tuple[str, str], dict[str, Any]] = {}

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

                    left_table_name = left_details.get("name") or str(left_ref.get("table"))
                    right_table_name = right_details.get("name") or str(right_ref.get("table"))

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
        column_ref: dict[str, Any],
        table_cache: dict[tuple[str, str], dict[str, Any]],
        datamodel_title: str,
    ) -> dict[str, Any] | None:
        """
        Fetch and cache table details for a given dataset/table reference.
        """
        dataset_id = column_ref.get("dataset")
        table_id = column_ref.get("table")

        if not dataset_id or not table_id:
            self.logger.warning(f"Missing dataset or table reference in relation column for datamodel '{datamodel_title}'.")
            return None

        cache_key = (str(dataset_id), str(table_id))
        if cache_key in table_cache:
            return table_cache[cache_key]

        endpoint = f"/api/v2/datamodels/{datamodel_id}/schema/datasets/{dataset_id}/tables/{table_id}"
        self.logger.debug(f"Fetching table details for dataset '{dataset_id}', table '{table_id}' in datamodel '{datamodel_title}' from endpoint: {endpoint}")

        response = self.api_client.get(endpoint)
        if response is None:
            self.logger.warning(f"Failed to retrieve table details for dataset '{dataset_id}', table '{table_id}' in datamodel '{datamodel_title}'.")
            return None

        if response.status_code != 200:
            try:
                error_body = response.json()
            except Exception:
                error_body = getattr(response, "text", "No response text")
            self.logger.warning(
                f"Failed to retrieve table details for dataset '{dataset_id}', table '{table_id}' in datamodel '{datamodel_title}'. Status: {response.status_code}, Error: {error_body}"
            )
            return None

        try:
            details = response.json()
        except Exception as exc:
            self.logger.exception(f"Failed to parse table JSON for dataset '{dataset_id}', table '{table_id}' in datamodel '{datamodel_title}': {exc}")
            return None

        table_cache[cache_key] = details
        return details

    def _resolve_column_name_for_m2m(
        self,
        table_details: dict[str, Any],
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
        self.logger.warning(f"Unable to resolve column OID '{column_oid}' to a name in datamodel '{datamodel_title}'. Using OID as fallback.")
        return str(column_oid)
