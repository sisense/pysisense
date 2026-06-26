from __future__ import annotations

from typing import Any


class BuildMixin:
    def create_datamodel(self, datamodel_name: str, datamodel_type: str) -> dict[str, Any]:
        """Create a new data model in Sisense.

        Normalizes and validates the model type, then sends a request to create
        the data model and returns its assigned ID.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model.
        datamodel_type : str
            Type of the data model. Should be either ``"extract"`` (for Elasticube)
            or ``"live"`` (for Live).

        Returns
        -------
        dict[str, Any]
            Dictionary with the data model ID under ``"datamodel_id"`` on success,
            or ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Attempting to create DataModel '{datamodel_name}' with type '{datamodel_type}'")

        # Normalize and validate type
        datamodel_type = datamodel_type.lower()
        if datamodel_type not in ("live", "extract"):
            self.logger.error(f"Invalid DataModel type: '{datamodel_type}'. Must be 'live' or 'extract'.")
            return {"error": "Invalid datamodel_type. Must be 'live' or 'extract'."}

        payload = {"title": datamodel_name, "type": datamodel_type}

        endpoint = "/api/v2/datamodels"
        self.logger.debug(f"Sending request to create DataModel with payload: {payload}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error(f"No response received while creating DataModel '{datamodel_name}'")
            return {"error": "No response from API while creating DataModel"}

        if not response.ok:
            self.logger.error(f"Failed to create DataModel '{datamodel_name}'. Status Code: {response.status_code}, Error: {response.text}")
            return {"error": f"Failed to create DataModel. Status Code: {response.status_code}"}

        datamodel_id = response.json().get("oid")
        self.logger.info(f"Successfully created DataModel '{datamodel_name}' with ID: {datamodel_id}")
        return {"datamodel_id": datamodel_id}

    def create_dataset(self, datamodel_name: str, connection_name: str, database_name: str, schema_name: str, dataset_name: str | None = None) -> dict[str, Any]:
        """Create a new dataset in the specified data model.

        Resolves the data model and connection by name, then sends a request to
        create a dataset. When ``dataset_name`` is omitted, the schema name is used.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model where the dataset will be created.
        connection_name : str
            Name of the connection to use.
        database_name : str
            Name of the data source database.
        schema_name : str
            Name of the data source schema.
        dataset_name : str | None, optional
            Name of the dataset. Defaults to the schema name if not provided.

        Returns
        -------
        dict[str, Any]
            The full dataset object on success, or ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Creating dataset in DataModel '{datamodel_name}' with connection '{connection_name}', database '{database_name}', and schema '{schema_name}'")

        # Step 1: Get DataModel ID
        self.logger.debug(f"Retrieving DataModel ID for '{datamodel_name}'")
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found. Aborting dataset creation.")
            return {"error": f"DataModel '{datamodel_name}' not found."}
        datamodel_id = datamodel.get("oid")
        self.logger.debug(f"DataModel ID for '{datamodel_name}': {datamodel_id}")

        # Step 2: Get DataModel Type
        datamodel_type = datamodel.get("type")
        if not datamodel_type:
            self.logger.error(f"DataModel '{datamodel_name}' does not have a valid type. Aborting dataset creation.")
            return {"error": f"DataModel '{datamodel_name}' does not have a valid type."}
        self.logger.debug(f"DataModel Type for '{datamodel_name}': {datamodel_type}")

        # Step 2: Get Connection ID
        self.logger.debug(f"Retrieving Connection ID for '{connection_name}'")
        connection = self.get_connection(connection_name)
        if "error" in connection or not connection:
            self.logger.error(f"Connection '{connection_name}' not found. Aborting dataset creation.")
            return {"error": f"Connection '{connection_name}' not found."}
        connection_id = connection[0].get("oid")
        self.logger.debug(f"Connection ID for '{connection_name}': {connection_id}")

        # Step 3: Use schema as dataset name if not provided
        if not dataset_name:
            dataset_name = schema_name
            self.logger.debug(f"No dataset name provided. Defaulting to schema name '{schema_name}'")
        else:
            self.logger.debug(f"Using provided dataset name: '{dataset_name}'")

        # Step 4: Build request payload
        payload = {"name": dataset_name, "type": datamodel_type, "connection": {"oid": connection_id}, "database": database_name, "schemaName": schema_name}
        self.logger.debug(f"Dataset creation payload: {payload}")

        # Step 5: Send request
        endpoint = f"/api/v2/datamodels/{datamodel_id}/schema/datasets"
        response = self.api_client.post(endpoint, data=payload)

        if response and response.status_code == 201:
            dataset = response.json()
            dataset_id = dataset.get("oid")
            self.logger.info(f"Dataset '{dataset_name}' created in DataModel '{datamodel_name}' with ID: {dataset_id}")
            return dataset

        try:
            error_detail = response.json().get("detail", "No detail provided.")
        except Exception:
            error_detail = response.text or "Unable to parse error details from response."

        self.logger.error(f"Failed to create dataset '{dataset_name}' in DataModel '{datamodel_name}'. Error: {error_detail}")

        return {"error": f"Failed to create dataset: {error_detail}"}

    def create_table(
        self,
        datamodel_name: str,
        table_name: str,
        database_name: str | None = None,
        schema_name: str | None = None,
        dataset_id: str | None = None,
        import_query: str | None = None,
        description: str = "",
        tags: list[str] | None = None,
        build_behavior_config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a new table in the specified data model.

        Resolves the data model and dataset (inferring database, schema, and
        connection when not provided), fetches the table schema, and creates the
        table. For EXTRACT models, applies an optional build behavior configuration.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model where the table will be created.
        table_name : str
            Name of the table to create.
        database_name : str | None, optional
            Name of the data source database. If not provided, inferred from the
            data model's dataset.
        schema_name : str | None, optional
            Name of the data source schema. If not provided, inferred from the
            data model's dataset.
        dataset_id : str | None, optional
            ID of the dataset where the table will be created. If not provided,
            inferred from the data model (requires a single dataset).
        import_query : str | None, optional
            SQL statement used as a custom import query. Defaults to ``None``.
        description : str, optional
            Description for the table. Defaults to an empty string.
        tags : list[str] | None, optional
            List of tags to apply to the table. Defaults to ``None``.
        build_behavior_config : dict[str, Any] | None, optional
            Build behavior configuration applied for EXTRACT models. Supported fields:
            ``mode`` (one of ``"replace"``, ``"replace_changes"``, ``"append"``,
            ``"increment"``) and, when ``mode`` is ``"increment"``, ``column_name``
            (the incremental column).

        Returns
        -------
        dict[str, Any]
            The created (or build-behavior-updated) table object on success, or
            ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"[START] Creating table '{table_name}' in DataModel '{datamodel_name}'")

        # Step 1: Get DataModel Info
        self.logger.debug(f"Retrieving DataModel ID for '{datamodel_name}'")
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found. Aborting table creation.")
            return {"error": f"DataModel '{datamodel_name}' not found."}
        datamodel_id = datamodel.get("oid")
        datamodel_type = datamodel.get("type")
        self.logger.debug(f"DataModel ID for '{datamodel_name}': {datamodel_id}")

        # Step 2: Get Dataset ID and metadata if not provided
        if not dataset_id:
            self.logger.debug(f"Retrieving Dataset ID from DataModel '{datamodel_name}'")
            datasets = datamodel.get("datasets")
            if datasets and len(datasets) > 1:
                self.logger.warning(f"Multiple datasets found in DataModel '{datamodel_name}'. Provide a dataset_id to specify which one to use.")
                return {"error": (f"Multiple datasets found in DataModel '{datamodel_name}'. Provide a dataset_id to specify which one to use.")}
            dataset_info = datasets[0]
            dataset_id = dataset_info.get("oid")
            if not database_name:
                database_name = dataset_info.get("database")
                self.logger.debug(f"Using inferred database name from dataset: {database_name}")
            else:
                self.logger.debug(f"Using provided database name: {database_name}")
            if not schema_name:
                schema_name = dataset_info.get("schemaName")
                self.logger.debug(f"Using inferred schema name from dataset: {schema_name}")
            else:
                self.logger.debug(f"Using provided schema name: {schema_name}")
            connection_name = dataset_info.get("connection", {}).get("name")

            self.logger.debug(f"Resolved Dataset ID: {dataset_id}, Database Name: {database_name}, Schema Name: {schema_name}, Connection Name: {connection_name}")

            if not dataset_id:
                self.logger.error(f"No dataset ID found in DataModel '{datamodel_name}'. Aborting table creation.")
                return {"error": f"No dataset ID found in DataModel '{datamodel_name}'."}
            if not database_name:
                self.logger.error(f"No database name found in DataModel '{datamodel_name}'. Aborting table creation.")
                return {"error": f"No database name found in DataModel '{datamodel_name}'."}
            if not schema_name:
                self.logger.error(f"No schema name found in DataModel '{datamodel_name}'. Aborting table creation.")
                return {"error": f"No schema name found in DataModel '{datamodel_name}'."}
            if not connection_name:
                self.logger.error(f"No connection name found in DataModel '{datamodel_name}'. Aborting table creation.")
                return {"error": f"No connection name found in DataModel '{datamodel_name}'."}
        else:
            # If dataset_id is provided, fetch metadata
            self.logger.debug(f"Using provided Dataset ID: {dataset_id}")
            dataset = self.api_client.get(f"/api/v2/datamodels/{datamodel_id}/schema/datasets/{dataset_id}")
            if dataset and dataset.status_code == 200:
                dataset_details = dataset.json()
                if not database_name:
                    database_name = dataset_info.get("database")
                    self.logger.debug(f"Using inferred database name from dataset: {database_name}")
                else:
                    self.logger.debug(f"Using provided database name: {database_name}")
                if not schema_name:
                    schema_name = dataset_info.get("schemaName")
                    self.logger.debug(f"Using inferred schema name from dataset: {schema_name}")
                else:
                    self.logger.debug(f"Using provided schema name: {schema_name}")
                connection_name = dataset_details.get("connection", {}).get("name")
                self.logger.debug(f"Resolved Dataset ID: {dataset_id}, Database Name: {database_name}, Schema Name: {schema_name}, Connection Name: {connection_name}")
            else:
                self.logger.error(f"Failed to retrieve dataset details for Dataset ID '{dataset_id}'. Status Code: {dataset.status_code}, Error: {dataset.text}")
                return {"error": f"Failed to retrieve dataset details for Dataset ID '{dataset_id}'"}

        # Step 3: Fetch schema of the table to be created
        # Use provided database and schema if available, otherwise fallback to inferred values
        db_name_to_use = database_name if database_name else database_name
        schema_name_to_use = schema_name if schema_name else schema_name

        if not db_name_to_use or not schema_name_to_use:
            self.logger.error(f"Missing database or schema name for table '{table_name}'.")
            return {"error": f"Missing database or schema name for table '{table_name}'."}

        self.logger.debug(f"Fetching schema for table '{table_name}' in database '{db_name_to_use}' and schema '{schema_name_to_use}' under connection '{connection_name}'")
        table_schema = self.get_table_schema(connection_name, db_name_to_use, schema_name_to_use, table_name)

        if "error" in table_schema:
            self.logger.error(f"Failed to retrieve schema for table '{table_name}'. Aborting table creation.")
            return {"error": f"Failed to retrieve schema for table '{table_name}'."}

        self.logger.debug(f"Table schema for '{table_name}': {table_schema}")

        # Step 4: Create Table Payload
        tags = tags if tags else []
        columns = table_schema.get("columns", [])

        formatted_columns = []
        for column in columns:
            column_name = column.get("columnName", "UnknownColumn")

            formatted_columns.append(
                {
                    "id": column_name,
                    "name": column_name,
                    "type": column.get("dbType", 0),
                    "size": column.get("size", 0),
                    "precision": column.get("precision", 0),
                    "scale": column.get("scale", 0),
                    "hidden": False,
                    "indexed": True,
                    "isUpsertBy": False,
                    "description": None,
                    "expression": None,
                    "import": None,
                    "isCustom": None,
                }
            )

        payload = {
            "id": table_name,
            "name": table_name,
            "columns": formatted_columns,
            "hidden": False,
            "buildBehavior": {"type": "sync", "accumulativeConfig": None},
            "configOptions": {"importQuery": import_query} if import_query else None,
            "description": description,
            "tags": tags,
            "expression": None,
            "type": "base",
        }

        self.logger.debug(f"Table creation payload: {payload}")

        # Step 5: Send POST request to create the table
        endpoint = f"/api/v2/datamodels/{datamodel_id}/schema/datasets/{dataset_id}/tables"
        response = self.api_client.post(endpoint, data=payload)
        if response and response.status_code == 201:
            table = response.json()
            table_id = table.get("oid")
            self.logger.info(f"Table '{table_name}' created in DataModel '{datamodel_name}' with ID: {table_id}")

            # Step 7: Update build behavior if applicable
            if datamodel_type.upper() == "EXTRACT" and build_behavior_config:
                self.logger.debug(f"Updating build behavior for table '{table_name}' in DataModel '{datamodel_name}'")
                mode = build_behavior_config.get("mode", "replace")
                build_behavior = {}

                if mode == "replace":
                    build_behavior = {"type": "sync", "accumulativeConfig": None}
                elif mode == "replace_changes":
                    build_behavior = {"type": "ignoreIfExists", "accumulativeConfig": None}
                elif mode == "append":
                    build_behavior = {"type": "accumulativeSync", "accumulativeConfig": None}
                elif mode == "increment":
                    column_name = build_behavior_config.get("column_name")
                    column_id = None
                    for col in table.get("columns", []):
                        if col.get("name") == column_name:
                            column_id = col.get("oid")
                            break
                    if not column_name:
                        self.logger.error("Increment mode requires 'column_name' in build_behavior_config.")
                        return {"error": "Missing 'column_id' for increment mode."}
                    if not column_id:
                        self.logger.error(f"Couldn't resolve column id matching. Column '{column_name}' not found in table '{table_name}'.")
                        return {"error": f"Column '{column_name}' not found in table '{table_name}'"}
                    build_behavior = {"type": "accumulativeSync", "accumulativeConfig": {"column": column_id, "type": "lastStored", "lastDays": None, "keepOnlyDays": None}}
                else:
                    self.logger.warning(f"Unknown build mode '{mode}'. Defaulting to 'replace'.")
                    build_behavior = {"type": "sync", "accumulativeConfig": None}

                patch_endpoint = f"/api/v2/datamodels/{datamodel_id}/schema/datasets/{dataset_id}/tables/{table_id}"
                patch_payload = {"buildBehavior": build_behavior}
                patch_response = self.api_client.patch(patch_endpoint, data=patch_payload)

                if patch_response and patch_response.status_code == 200:
                    self.logger.info(f"Table '{table_name}' build behavior updated successfully.")
                    return patch_response.json()
                else:
                    self.logger.error(f"Failed to update table '{table_name}' build behavior. Status Code: {patch_response.status_code}, Error: {patch_response.text}")
                    return {"error": "Failed to update table build behavior"}

            return table

        error_msg = response.text if response else "No response from API."
        self.logger.error(f"Failed to create table '{table_name}' in DataModel '{datamodel_name}'. Error: {error_msg}")
        return {"error": "Failed to create table"}

    def setup_datamodel(
        self,
        datamodel_name: str,
        datamodel_type: str,
        connection_name: str,
        database_name: str,
        schema_name: str,
        tables: list[dict[str, Any]],
        dataset_name: str | None = None,
    ) -> dict[str, Any]:
        """Set up a data model end to end using an existing connection.

        Creates the data model, a dataset, and the requested tables in sequence,
        reusing the supplied connection.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model.
        datamodel_type : str
            Type of the data model. Should be either ``"extract"`` (for Elasticube)
            or ``"live"`` (for Live).
        connection_name : str
            Name of the connection to use.
        database_name : str
            Name of the data source database.
        schema_name : str
            Name of the data source schema.
        tables : list[dict[str, Any]]
            List of table definitions to create in the data model. Each table is a
            dictionary that may include the fields: ``table_name``, ``schema_name``,
            ``database_name``, ``import_query`` (SQL custom import query),
            ``description``, ``tags`` (list of tags), and ``build_behavior_config``
            (build behavior configuration).
        dataset_name : str | None, optional
            Name of the dataset. Defaults to the schema name if not provided.

        Returns
        -------
        dict[str, Any]
            Dictionary with ``"datamodel_id"``, ``"dataset_id"``, and ``"tables"``
            (list of created table names) on success, or ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"[START] Setup DataModel '{datamodel_name}'")

        # Step 1: Create DataModel
        self.logger.debug(f"Creating DataModel '{datamodel_name}'")
        datamodel_response = self.create_datamodel(datamodel_name=datamodel_name, datamodel_type=datamodel_type)
        if "error" in datamodel_response:
            self.logger.error(f"Failed to create DataModel '{datamodel_name}'. Aborting setup.")
            return {"error": f"Failed to create DataModel '{datamodel_name}'."}
        datamodel_id = datamodel_response.get("datamodel_id")
        self.logger.debug(f"DataModel '{datamodel_name}' created with ID: {datamodel_id}")

        # Step 2: Create Dataset
        self.logger.debug(f"Creating dataset in DataModel '{datamodel_name}'")
        dataset_response = self.create_dataset(datamodel_name=datamodel_name, connection_name=connection_name, database_name=database_name, schema_name=schema_name, dataset_name=dataset_name)
        if "error" in dataset_response:
            self.logger.error(f"Failed to create dataset in DataModel '{datamodel_name}'. Aborting setup.")
            return {"error": f"Failed to create dataset in DataModel '{datamodel_name}'."}
        dataset_id = dataset_response.get("oid")
        self.logger.debug(f"Dataset created with ID: {dataset_id}")

        # Step 3: Create Tables
        if not tables:
            self.logger.error("No table definitions provided. Aborting setup.")
            return {"error": "No table definitions provided."}

        self.logger.debug(f"Creating {len(tables)} tables in DataModel '{datamodel_name}'...")

        created_tables = []

        for table in tables:
            table_name = table.get("table_name")
            table_schema_name = table.get("schema_name", schema_name)
            table_database_name = table.get("database_name", database_name)
            self.logger.debug(f"Creating table '{table_name}' in DataModel '{datamodel_name}'")
            table_response = self.create_table(
                datamodel_name=datamodel_name,
                table_name=table_name,
                database_name=table_database_name,
                schema_name=table_schema_name,
                dataset_id=dataset_id,
                import_query=table.get("import_query"),
                description=table.get("description", ""),
                tags=table.get("tags", []),
                build_behavior_config=table.get("build_behavior_config"),
            )

            if "error" in table_response:
                self.logger.error(f"Failed to create table '{table_name}' in DataModel '{datamodel_name}'. Aborting.")
                return {"error": f"Failed to create table '{table_name}' in DataModel '{datamodel_name}'."}

            self.logger.debug(f"Table '{table_name}' created successfully.")
            created_tables.append(table_name)

        self.logger.info(f"DataModel '{datamodel_name}' setup successfully with tables: {created_tables}")
        self.logger.debug(f"[END] Setup DataModel '{datamodel_name}'")
        return {"datamodel_id": datamodel_id, "dataset_id": dataset_id, "tables": created_tables}

    def deploy_datamodel(self, datamodel_name: str, build_type: str = "full", row_limit: int = 0, schema_origin: str = "latest") -> dict[str, Any]:
        """Deploy (build or publish) the specified data model based on its type.

        Supports both Elasticube (EXTRACT) and Live models. For EXTRACT models a
        build is triggered using ``build_type``, ``row_limit``, and ``schema_origin``.
        For LIVE models a publish is triggered and ``row_limit`` and
        ``schema_origin`` are ignored.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model to deploy.
        build_type : str, optional
            Type of deployment for EXTRACT models. One of ``"schema_changes"``
            (build only schema changes), ``"by_table"`` (build per each table's
            config, e.g. incremental/accumulative), or ``"full"`` (rebuild the
            entire model, the default). For LIVE models this is overridden to
            ``"publish"``.
        row_limit : int, optional
            Maximum number of rows to process for EXTRACT builds. Defaults to ``0``
            (no limit). Ignored for LIVE models.
        schema_origin : str, optional
            Schema source for EXTRACT builds. One of ``"latest"`` (schema as seen
            in the Data page, the default) or ``"running"`` (last successfully built
            version). Ignored for LIVE models.

        Returns
        -------
        dict[str, Any]
            Deployment result including status on success, or ``{"error": "..."}``
            on failure.
        """
        self.logger.debug(f"[START] Deploying DataModel '{datamodel_name}'")

        # Step 1: Get DataModel by name
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found. Aborting deployment.")
            return {"error": f"DataModel '{datamodel_name}' not found."}

        datamodel_id = datamodel.get("oid")
        datamodel_type = datamodel.get("type")
        self.logger.debug(f"Resolved DataModel ID: {datamodel_id}, Type: {datamodel_type}")

        # Step 2: Prepare deployment payload based on model type
        if datamodel_type.upper() == "EXTRACT":
            self.logger.debug(f"Preparing Elasticube build for '{datamodel_name}'")
            payload = {"datamodelId": datamodel_id, "buildType": build_type, "rowLimit": row_limit, "schemaOrigin": schema_origin}
        elif datamodel_type.upper() == "LIVE":
            self.logger.debug(f"Preparing Live model publish for '{datamodel_name}'")
            payload = {"datamodelId": datamodel_id, "buildType": "publish"}
        else:
            self.logger.error(f"Unsupported DataModel type '{datamodel_type}' for '{datamodel_name}'.")
            return {"error": f"Unsupported DataModel type '{datamodel_type}' for '{datamodel_name}'."}

        # Step 3: Send deployment request
        endpoint = "/api/v2/builds"
        self.logger.debug(f"Sending POST request to '{endpoint}' with payload: {payload}")
        response = self.api_client.post(endpoint, data=payload)

        if response and response.status_code == 201:
            self.logger.info(f"DataModel '{datamodel_name}' deployed successfully.")
            return response.json()
        else:
            error_text = response.text if response else "No response from API."
            self.logger.error(f"Failed to deploy DataModel '{datamodel_name}'. Error: {error_text}")
            return {"error": f"Failed to deploy DataModel '{datamodel_name}'"}
