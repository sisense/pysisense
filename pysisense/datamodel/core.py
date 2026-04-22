from __future__ import annotations

from typing import Any


class DataModelCoreMixin:
    def get_datamodel(self, datamodel_name):
        """
        Retrieves a DataModel by its name.

        Parameters:
            datamodel_name (str): Name of the DataModel to retrieve.

        Returns:
            dict: DataModel details if found, or a dictionary with an error message.
        """
        self.logger.debug(f"Fetching DataModel with title: '{datamodel_name}'")

        endpoint = f"/api/v2/datamodels/schema?title={datamodel_name}"
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"No response received from API while retrieving DataModel '{datamodel_name}'")
            return {"error": "No response from API while retrieving DataModel"}

        if not response.ok:
            self.logger.error(f"Failed to retrieve DataModel '{datamodel_name}'. Status Code: {response.status_code}, Error: {response.text}")
            return {"error": f"Failed to retrieve DataModel. Status Code: {response.status_code}"}

        datamodels = response.json()
        if not datamodels:
            self.logger.warning(f"No DataModel found with name '{datamodel_name}'")
            return {"error": f"DataModel '{datamodel_name}' not found"}

        self.logger.info(f"Successfully retrieved DataModel '{datamodel_name}'")
        self.logger.debug(f"DataModel details: {datamodels}")
        return datamodels

    def get_all_datamodel(self):
        """
        Retrieves metadata details of all DataModels using an undocumented internal API.
        This includes additional fields like build status, size, and timestamps that may
        not be available through the standard public endpoints.

        Returns:
            dict: Parsed metadata details of all DataModels, or a dictionary with an error message.
        """
        self.logger.debug("Fetching all DataModel metadata using undocumented API.")

        endpoint = "/api/v2/ecm/"
        payload = {
            "query": """
                query elasticubesMetadata($tenantFilter: String, $isViewMode: Boolean) {
                elasticubesMetadata(tenantFilter: $tenantFilter, isViewMode: $isViewMode) {
                    oid
                    title
                    type
                    status
                    sizeInMb
                }
                }
            """
        }

        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error("No response received from API while retrieving datamodel metadata.")
            return {"error": "No response from API while retrieving datamodel metadata."}

        if not response.ok:
            self.logger.error(f"Failed to retrieve datamodel metadata. Status Code: {response.status_code}, Error: {response.text}")
            return {"error": f"Failed to retrieve datamodel metadata. Status Code: {response.status_code}"}

        data = response.json()

        new_data = []
        for dm in data["data"]["elasticubesMetadata"]:
            if "building" in dm.get("status", []):
                dm["status"] = "building"
            else:
                status_list = dm.get("status", [])
                dm["status"] = status_list[0] if isinstance(status_list, list) and status_list else "unknown"

            if isinstance(dm.get("sizeInMb"), int | float):
                dm["sizeInMb"] = round(dm["sizeInMb"], 2)
            new_data.append(dm)

        self.logger.info("Successfully retrieved all datamodel metadata.")
        self.logger.debug(f"Datamodel metadata details: {data}")
        self.logger.info(f"Total number of datamodels: {len(new_data)}")
        return new_data

    def describe_datamodel_raw(self, datamodel_name):
        """
        Retrieve detailed information about a specific DataModel, including share details.

        Parameters:
            datamodel_name (str): Name of the DataModel to describe.

        Returns:
            dict: Detailed information about the DataModel, or an error message if not found.
        """
        self.logger.debug(f"[START] Describing DataModel '{datamodel_name}'")

        # Step 1: Get DataModel by name
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"error": f"DataModel '{datamodel_name}' not found."}

        datamodel_id = datamodel.get("oid")
        datamodel_type = datamodel.get("type")
        datamodel_name = datamodel.get("title")

        # Step 3: Resolve last build/publish time
        last_build_publish = datamodel.get("lastBuildTime") if datamodel_type.upper() == "EXTRACT" else datamodel.get("lastPublishTime")

        # Step 4: Resolve Dataset and Connections
        datasets = datamodel.get("datasets", [])
        dataset_info = []

        for dataset in datasets:
            dataset_id = dataset.get("oid")
            dataset_name = dataset.get("name", "Unknown Dataset")

            connection = dataset.get("connection")
            if connection:
                provider = connection.get("provider", "Unknown Provider")
                connection_name = connection.get("name", "Unknown Connection")
            else:
                provider = "Unknown Provider"
                connection_name = "Unknown Connection"

            tables = dataset.get("schema", {}).get("tables", [])
            table_info = []

            self.logger.debug(f"Resolving tables for dataset '{dataset_name}'")
            for table in tables:
                self.logger.debug(table)
                table_name = table.get("name", "Unknown Table")
                table_type = dataset.get("type", "Unknown Type")
                table_info.append({"table_name": table_name, "table_type": table_type})

            dataset_info.append({"dataset_id": dataset_id, "dataset_name": dataset_name, "provider": provider, "connection_name": connection_name, "tables": table_info})

        self.logger.debug(f"Resolved datasets: {dataset_info}")
        self.logger.info(f"Total datasets resolved: {len(dataset_info)}")

        # Step 5: Build output dictionary
        datamodel_info = {
            "name": datamodel_name,
            "id": datamodel_id,
            "type": datamodel_type,
            "datamodel_last_build_publish": last_build_publish,
            "datamodel_last_updated": datamodel.get("lastUpdated", ""),
            "datasets": dataset_info,
        }

        self.logger.info(f"DataModel '{datamodel_name}' described successfully.")
        return datamodel_info

    def describe_datamodel(self, datamodel_name):
        """
        Retrieve detailed datamodel structure in a flat, row-based format suitable for DataFrame or CSV export.

        Parameters:
            datamodel_name (str): Name of the DataModel to describe.

        Returns:
            list: List of dictionaries, each representing a single table row with context (datamodel, dataset, table).
        """
        self.logger.debug(f"[START] Generating flat structure for DataModel '{datamodel_name}'")

        # Step 1: Get DataModel by name
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []
        datamodel_id = datamodel.get("oid")
        datamodel_type = datamodel.get("type")
        datamodel_name = datamodel.get("title")

        # Step 2: Resolve last build/publish time
        last_build_publish = datamodel.get("lastBuildTime") if datamodel_type.upper() == "EXTRACT" else datamodel.get("lastPublishTime")

        last_updated = datamodel.get("lastUpdated", "")

        # Step 3: Extract datasets and tables as rows
        rows = []
        datasets = datamodel.get("datasets", [])

        for dataset in datasets:
            dataset_id = dataset.get("oid")
            dataset_name = dataset.get("name", "Unknown Dataset")
            connection = dataset.get("connection")

            provider = connection.get("provider", "Unknown Provider") if connection else "Unknown Provider"
            connection_name = connection.get("name", "Unknown Connection") if connection else "Unknown Connection"

            tables = dataset.get("schema", {}).get("tables", [])

            for table in tables:
                row = {
                    "datamodel_name": datamodel_name,
                    "datamodel_id": datamodel_id,
                    "datamodel_type": datamodel_type,
                    "datamodel_last_build_publish": last_build_publish,
                    "datamodel_last_updated": last_updated,
                    "dataset_id": dataset_id,
                    "dataset_name": dataset_name,
                    "provider": provider,
                    "connection_name": connection_name,
                    "table_name": table.get("name", "Unknown Table"),
                    "table_type": dataset.get("type", "Unknown Type"),
                }
                rows.append(row)

        self.logger.info(f"Flattened {len(rows)} rows from DataModel '{datamodel_name}'")
        return rows

    def get_model_schema(self, datamodel_name):
        """
        Retrieves the schema of a DataModel, including tables and columns.

        Parameters:
            datamodel_name (str): Name of the DataModel to retrieve the schema for.

        Returns:
            list: A list of dictionaries containing schema information (one per column),
                or an error message if not found.
        """
        self.logger.debug(f"[START] Resolving schema for DataModel '{datamodel_name}'")

        # Step 1: Get DataModel by name
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"error": f"DataModel '{datamodel_name}' not found."}

        datamodel_type = datamodel.get("type")
        datamodel_name = datamodel.get("title")
        datamodel_datasets = datamodel.get("datasets", [])

        schema_info = []
        table_count = 0

        # Column Type Mapping
        column_type_mapping = {
            4: "DateTime",
            40: "Double",
            8: "Integer",
            0: "BigInt",
            18: "Text",
            5: "Decimal",
            6: "Float",
            13: "Real",
        }

        for dataset in datamodel_datasets:
            dataset_name = dataset.get("name", "Unknown Dataset")
            tables = dataset.get("schema", {}).get("tables", [])

            for table in tables:
                table_name = table.get("name", "Unknown Table")
                columns = table.get("columns", [])
                column_count = 0

                for column in columns:
                    info = {
                        "datamodel_name": datamodel_name,
                        "datamodel_type": datamodel_type,
                        "dataset_name": dataset_name,
                        "table_name": table_name,
                        "column_name": column.get("name", "Unknown Column"),
                        "column_type": column_type_mapping.get(column.get("type"), "Unknown Type"),
                    }
                    schema_info.append(info)
                    column_count += 1

                self.logger.debug(f"Processed table '{table_name}' with {column_count} columns.")
                table_count += 1

        self.logger.info(f"Resolved schema for {table_count} tables in DataModel '{datamodel_name}'")
        self.logger.info(f"Total columns extracted: {len(schema_info)}")
        return schema_info

    def resolve_datamodel_reference(self, datamodel_ref: str) -> dict[str, Any]:
        """
        Resolve a data model reference (ID or title) to a concrete data model ID and title.

        This helper accepts a single string that may be either:
        - a Sisense data model ID, or
        - a data model title (schema name).

        It first attempts to treat the reference as an ID using
        `/api/v2/datamodels/{id}/schema`. If that fails, it falls back to
        calling `/api/v2/datamodels/schema` with a `title` query parameter.

        Parameters
        ----------
        datamodel_ref : str
            Data model reference to resolve. This can be either an ID or a name.

        Returns
        -------
        dict
            A dictionary with the following keys:
            - success (bool): True if the reference was resolved to a data model.
            - status_code (int): 200 if resolved successfully, 404 if not found,
              or 500 if an unexpected error occurred.
            - datamodel_id (str or None): Resolved data model ID (oid) if found,
              otherwise None.
            - datamodel_title (str or None): Resolved data model title if found,
              otherwise None.
            - error (str or None): Error message if success is False, otherwise None.
        """
        self.logger.debug(
            "Resolving data model reference.",
            extra={"datamodel_ref": datamodel_ref},
        )

        # -------------------------
        # 1) Try treating it as an ID
        # -------------------------
        id_endpoint = f"/api/v2/datamodels/{datamodel_ref}/schema"
        id_response = self.api_client.get(id_endpoint)

        if id_response is not None and id_response.status_code == 200:
            try:
                payload: dict[str, Any] = id_response.json()
                datamodel_id = payload.get("oid")
                datamodel_title = payload.get("title")

                if datamodel_id:
                    self.logger.debug(
                        "Resolved data model reference as ID.",
                        extra={
                            "datamodel_ref": datamodel_ref,
                            "datamodel_id": datamodel_id,
                            "datamodel_title": datamodel_title,
                        },
                    )
                    return {
                        "success": True,
                        "status_code": 200,
                        "datamodel_id": datamodel_id,
                        "datamodel_title": datamodel_title,
                        "error": None,
                    }
            except Exception as exc:
                self.logger.exception(
                    "Failed to parse data model JSON when treating reference as ID.",
                    extra={"datamodel_ref": datamodel_ref, "error": str(exc)},
                )

        # -------------------------
        # 2) Treat it as a title
        # -------------------------
        title_endpoint = "/api/v2/datamodels/schema"
        title_params = {"title": datamodel_ref}

        self.logger.debug(
            "Attempting to resolve data model reference by title.",
            extra={"datamodel_ref": datamodel_ref, "endpoint": title_endpoint},
        )

        title_response = self.api_client.get(title_endpoint, params=title_params)

        if title_response is None:
            error_msg = "No response received while resolving data model by title."
            self.logger.error(
                error_msg,
                extra={"datamodel_ref": datamodel_ref},
            )
            return {
                "success": False,
                "status_code": 500,
                "datamodel_id": None,
                "datamodel_title": None,
                "error": error_msg,
            }

        if title_response.status_code != 200:
            try:
                error_body = title_response.json()
            except Exception:
                error_body = getattr(title_response, "text", "No response text")
            error_msg = f"Failed to resolve data model by title. Status: {title_response.status_code}, Error: {error_body}"
            self.logger.error(
                error_msg,
                extra={"datamodel_ref": datamodel_ref},
            )
            return {
                "success": False,
                "status_code": title_response.status_code,
                "datamodel_id": None,
                "datamodel_title": None,
                "error": error_msg,
            }

        try:
            payload = title_response.json()
        except Exception as exc:
            error_msg = "Failed to parse data model JSON when resolving by title."
            self.logger.exception(
                error_msg,
                extra={"datamodel_ref": datamodel_ref, "error": str(exc)},
            )
            return {
                "success": False,
                "status_code": 500,
                "datamodel_id": None,
                "datamodel_title": None,
                "error": error_msg,
            }

        # The API might return a single object or a list; handle both.
        candidates: list[dict[str, Any]] = []
        if isinstance(payload, list):
            candidates = payload
        elif isinstance(payload, dict):
            candidates = [payload]

        if not candidates:
            error_msg = f"Data model reference '{datamodel_ref}' not found."
            self.logger.warning(
                error_msg,
                extra={"datamodel_ref": datamodel_ref},
            )
            return {
                "success": False,
                "status_code": 404,
                "datamodel_id": None,
                "datamodel_title": None,
                "error": error_msg,
            }

        # Prefer an exact title match (case-insensitive), otherwise take the first one.
        exact_match = None
        for candidate in candidates:
            title = candidate.get("title")
            if isinstance(title, str) and title.lower() == datamodel_ref.lower():
                exact_match = candidate
                break

        chosen = exact_match or candidates[0]
        datamodel_id = chosen.get("oid")
        datamodel_title = chosen.get("title")

        if not datamodel_id:
            error_msg = "Resolved data model payload is missing 'oid' field."
            self.logger.error(
                error_msg,
                extra={"datamodel_ref": datamodel_ref, "payload": chosen},
            )
            return {
                "success": False,
                "status_code": 500,
                "datamodel_id": None,
                "datamodel_title": None,
                "error": error_msg,
            }

        self.logger.debug(
            "Resolved data model reference by title.",
            extra={
                "datamodel_ref": datamodel_ref,
                "datamodel_id": datamodel_id,
                "datamodel_title": datamodel_title,
            },
        )

        return {
            "success": True,
            "status_code": 200,
            "datamodel_id": datamodel_id,
            "datamodel_title": datamodel_title,
            "error": None,
        }
