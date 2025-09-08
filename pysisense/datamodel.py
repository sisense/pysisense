from .sisenseclient import SisenseClient


class DataModel:

    def __init__(self, api_client=None, debug=False):
        """
        Initializes the DataModel class.

        If no Sisense client is provided, a new SisenseClient is created.

        Parameters:
            api_client (SisenseClient, optional): An existing SisenseClient instance.
                If None, a new SisenseClient is created.
            debug (bool, optional): Enables debug logging if True. Default is False.
        """
        # Use provided API client or create a new one
        self.api_client = api_client if api_client else SisenseClient(debug=debug)

        # Use the logger from the SisenseClient instance
        self.logger = self.api_client.logger
        self.logger.debug("DataModel class initialized.")

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
            self.logger.error(
                f"Failed to retrieve DataModel '{datamodel_name}'. "
                f"Status Code: {response.status_code}, Error: {response.text}"
            )
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
            self.logger.error(
                f"Failed to retrieve datamodel metadata. "
                f"Status Code: {response.status_code}, Error: {response.text}"
            )
            return {"error": f"Failed to retrieve datamodel metadata. Status Code: {response.status_code}"}

        data = response.json()

        new_data = []
        for dm in data["data"]["elasticubesMetadata"]:
            if "building" in dm.get("status", []):
                dm["status"] = "building"
            else:
                status_list = dm.get("status", [])
                dm["status"] = status_list[0] if isinstance(status_list, list) and status_list else "unknown"

            if isinstance(dm.get("sizeInMb"), (int, float)):
                dm["sizeInMb"] = round(dm["sizeInMb"], 2)
            new_data.append(dm)

        self.logger.info("Successfully retrieved all datamodel metadata.")
        self.logger.debug(f"Datamodel metadata details: {data}")
        self.logger.info(f"Total number of datamodels: {len(new_data)}")
        return new_data

    def get_connection(self, connection_name):
        """
        Retrieves a Connection by its name.

        Parameters:
            connection_name (str): Name of the connection to filter by.

        Returns:
            List: Connection details if found, or a dictionary with an error message.
        """
        self.logger.debug(f"Attempting to retrieve connections with name: '{connection_name}'")

        endpoint = f"/api/v2/connections?name={connection_name}"
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"No response received while retrieving connections with name '{connection_name}'")
            return {"error": "No response from API while retrieving connections"}

        if not response.ok:
            self.logger.error(
                f"Failed to retrieve connections. Status Code: {response.status_code}, "
                f"Error: {response.text}"
            )
            return {"error": f"Failed to retrieve connections. Status Code: {response.status_code}"}

        connections = response.json()
        if not connections:
            self.logger.warning(f"No connections found with name '{connection_name}'")
            return {"error": f"No connections found with name '{connection_name}'"}

        self.logger.info(f"Successfully retrieved connections with name '{connection_name}'")
        self.logger.debug(f"Connection details: {connections}")
        return connections

    def get_table_schema(self, connection_name, database_name, schema_name, table_name):
        """
        Retrieves the schema of a table in a specified connection from Data Source.
        This method uses an undocumented Sisense API endpoint to fetch the table schema details.
        NOTE: This endpoint is undocumented and may change in future versions of Sisense.
        It is recommended to use this method with caution.

        Parameters:
            connection_name (str): Name of the connection.
            database_name (str): Name of the database.
            schema_name (str): Name of the schema.
            table_name (str): Name of the table.

        Returns:
            dict: Table schema details if found, or a dictionary with an error message.
        """
        self.logger.debug(f"Fetching schema for table '{table_name}' in connection '{connection_name}'")

        # Step 1: Retrieve connection ID and provider
        connection = self.get_connection(connection_name)
        if not connection or "error" in connection:
            self.logger.error(f"Connection '{connection_name}' not found. Cannot retrieve table schema.")
            return {"error": f"Connection '{connection_name}' not found."}

        connection_id = connection[0].get("oid")
        connection_provider = connection[0].get("provider")
        self.logger.debug(f"Resolved connection ID: {connection_id}, Provider: {connection_provider}")

        # Step 2: Prepare payload and send request
        endpoint = f"/api/v1/connection/{connection_id}/table_schema_details"
        payload = {
            "provider": connection_provider,
            "connectionData": {
                "connection": {"Database": database_name},
                "schema": schema_name,
                "table": table_name
            }
        }

        response = self.api_client.post(endpoint, data=payload)

        # Step 3: Handle response
        if response is None:
            self.logger.error(f"No response received while retrieving schema for table '{table_name}'")
            return {"error": "No response from API while retrieving table schema"}

        if not response.ok:
            self.logger.error(
                f"Failed to retrieve schema for table '{table_name}'. "
                f"Status Code: {response.status_code}, Error: {response.text}"
            )
            return {"error": f"Failed to retrieve table schema. Status Code: {response.status_code}"}

        schema = response.json()
        if not schema:
            self.logger.warning(f"No schema data found for table '{table_name}'")
            return {"error": f"No schema found for table '{table_name}'"}

        self.logger.info(f"Successfully retrieved schema for table '{table_name}'")
        self.logger.debug(f"Table schema details: {schema}")
        return schema

    def create_datamodel(self, datamodel_name, datamodel_type):
        """
        Creates a new DataModel in Sisense.

        Parameters:
            datamodel_name (str): Name of the DataModel.
            datamodel_type (str): Type of the DataModel. Should be either "extract" (for Elasticube)
                or "live" (for Live).

        Returns:
            dict: Dictionary with the DataModel ID if created successfully, or an error message.
        """
        self.logger.debug(f"Attempting to create DataModel '{datamodel_name}' with type '{datamodel_type}'")

        # Normalize and validate type
        datamodel_type = datamodel_type.lower()
        if datamodel_type not in ("live", "extract"):
            self.logger.error(f"Invalid DataModel type: '{datamodel_type}'. Must be 'live' or 'extract'.")
            return {"error": "Invalid datamodel_type. Must be 'live' or 'extract'."}

        payload = {
            "title": datamodel_name,
            "type": datamodel_type
        }

        endpoint = "/api/v2/datamodels"
        self.logger.debug(f"Sending request to create DataModel with payload: {payload}")
        response = self.api_client.post(endpoint, data=payload)

        if response is None:
            self.logger.error(f"No response received while creating DataModel '{datamodel_name}'")
            return {"error": "No response from API while creating DataModel"}

        if not response.ok:
            self.logger.error(
                f"Failed to create DataModel '{datamodel_name}'. "
                f"Status Code: {response.status_code}, Error: {response.text}"
            )
            return {"error": f"Failed to create DataModel. Status Code: {response.status_code}"}

        datamodel_id = response.json().get("oid")
        self.logger.info(f"Successfully created DataModel '{datamodel_name}' with ID: {datamodel_id}")
        return {"datamodel_id": datamodel_id}

    def generate_connections_payload(self, datasource_type, connection_params):
        """
        Generates the appropriate connections payload based on the datasource type.

        Parameters:
            datasource_type (str): Type of datasource (e.g., "ATHENA", "SNOWFLAKE", "ORACLE").
            connection_params (dict): Connection details for the datasource.

        Returns:
            dict: Connections payload.
        """
        datasource_type = datasource_type.upper()
        self.logger.debug(f"Generating connection payload for datasource type: {datasource_type}")

        # Athena connection payload
        if datasource_type == "ATHENA":
            try:
                payload = {
                    "enabled": True,
                    "createdByUser": True,
                    "provider": "athena",
                    "name": connection_params["name"],
                    "description": connection_params.get("description", ""),
                    "parameters": {
                        "Basic": True,
                        "AwsRegion": connection_params["region"],
                        "S3OutputLocation": connection_params["s3_output_location"],
                        "userName": connection_params["aws_access_key"],
                        "password": connection_params["aws_secret_key"],
                        "UseDynamicSchema": False,
                        "SchemaName": connection_params.get("schema", ""),
                        "AdditionalParameters": connection_params.get("additional_parameters", ""),
                        "advance": False,
                        "EC2Instance": False
                    },
                    "supportedModelTypes": ["LIVE", "EXTRACT"]
                }
                self.logger.debug(f"Generated Athena connection payload: {payload}")
                return payload

            except KeyError as e:
                self.logger.error(f"Missing required Athena connection parameter: {e}")
                raise

        # Databricks connection payload
        elif datasource_type == "DATABRICKS":
            try:
                payload = {
                    "enabled": True,
                    "createdByUser": True,
                    "provider": "Databricks",
                    "name": connection_params["name"],
                    "description": connection_params.get("description", ""),
                    "parameters": {
                        "connectionString": connection_params["connection_string"],
                        "password": connection_params["token"],
                        "UseDynamicSchema": connection_params.get("use_dynamic_schema", False),
                        "Schema": connection_params.get("schema", ""),
                    },
                    "supportedModelTypes": [
                        "LIVE",
                        "EXTRACT"
                    ]
                }
                self.logger.debug(f"Generated Databricks connection payload: {payload}")
                return payload

            except KeyError as e:
                self.logger.error(f"Missing required Databricks connection parameter: {e}")
                raise

        # Bigquery connection payload
        elif datasource_type == "BIGQUERY":
            try:
                payload = {
                    "enabled": True,
                    "createdByUser": True,
                    "provider": "GoogleBigQuery",
                    "name": connection_params["name"],
                    "description": connection_params.get("description", ""),
                    "parameters": {
                        "googleAccount": False,
                        "serviceAccount": connection_params.get("use_service_account", True),
                        "serviceAccountKeyPath": connection_params["service_account_key_path"],
                        "UseProxyServer": connection_params.get("use_proxy_server", False),
                        "UseDynamicSchema": connection_params.get("use_dynamic_schema", False),
                        "samplingLevel": connection_params.get("record_field_flattening_level", "2"),
                        "unnestArrays": connection_params.get("unnest_arrays", False),
                        "allowLargeResults": connection_params.get("allow_large_results", False),
                        "useStorageApi": connection_params.get("use_storage_api", False),
                        "AdditionalParameters": connection_params.get("additional_parameters", ""),
                        "DB": connection_params.get("database", "")
                    },
                    "supportedModelTypes": [
                        "LIVE",
                        "EXTRACT"
                    ]
                }
                self.logger.debug(f"Generated BigQuery connection payload: {payload}")
                return payload

            except KeyError as e:
                self.logger.error(f"Missing required BigQuery connection parameter: {e}")
                raise

        # Redshift connection payload
        elif datasource_type == "REDSHIFT":
            try:
                payload = {
                    "enabled": True,
                    "createdByUser": True,
                    "provider": "RedShift",
                    "name": connection_params.get("name", ""),
                    "description": connection_params.get("description", ""),
                    "parameters": {
                        "Server": connection_params["server"],
                        "UserName": connection_params["username"],
                        "Password": connection_params["password"],
                        "DefaultDatabase": connection_params.get("default_database", ""),
                        "UseDynamicSchema": False,
                        "EncryptConnection": False,
                        "AdditionalParameters": connection_params.get("additional_parameters", ""),
                    },
                    "supportedModelTypes": ["LIVE", "EXTRACT"]
                }
                self.logger.debug(f"Generated Redshift connection payload: {payload}")
                return payload
            except KeyError as e:
                self.logger.error(f"Missing required Redshift connection parameter: {e}")
                raise
        else:
            error_msg = f"Unsupported datasource type: {datasource_type}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def create_connections(self, connection_payload):
        """
        Creates a new connection using the provided payload.

        Parameters:
            connection_payload (dict): The configuration payload for the connection.

        Returns:
            dict or None: JSON response with connection details if successful, otherwise None.
        """
        endpoint = "/api/v2/connections"
        self.logger.debug(f"Creating connection with payload: {connection_payload}")

        response = self.api_client.post(endpoint, data=connection_payload)

        if response and response.status_code == 201:
            connection_detail = response.json()
            self.logger.info(f"Connection created successfully: {connection_detail.get('name', 'Unknown')}")
            self.logger.debug(f"Full connection response: {connection_detail}")
            return connection_detail

        error_msg = response.text if response else "No response received from API."
        self.logger.error(f"Failed to create connection. Error: {error_msg}")
        return None

    def create_dataset(self, datamodel_name, connection_name, database_name, schema_name, dataset_name=None):
        """
        Creates a new dataset in the specified DataModel.

        Parameters:
            datamodel_name (str): Name of the DataModel where the dataset will be created.
            connection_name (str): Name of the connection to use.
            database_name (str): Name of the data source database.
            schema_name (str): Name of the data source schema.
            dataset_name (str, optional): Name of the dataset. Defaults to schema name if not provided.

        Returns:
            dict: A dictionary containing the full dataset object on success, or an error message on failure.
        """
        self.logger.debug(
            f"Creating dataset in DataModel '{datamodel_name}' with connection '{connection_name}', "
            f"database '{database_name}', and schema '{schema_name}'"
        )

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
        payload = {
            "name": dataset_name,
            "type": datamodel_type,
            "connection": {"oid": connection_id},
            "database": database_name,
            "schemaName": schema_name
        }
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

        self.logger.error(
            f"Failed to create dataset '{dataset_name}' in DataModel '{datamodel_name}'. Error: {error_detail}"
        )

        return {"error": f"Failed to create dataset: {error_detail}"}

    def create_table(self, datamodel_name, table_name, database_name=None, schema_name=None, dataset_id=None,
                     import_query=None, description="", tags=None, build_behavior_config=None):
        """
        Create a new table in the specified DataModel.

        Parameters:
            datamodel_name (str): Name of the DataModel where the table will be created.
            table_name (str): Name of the table to create.
            database_name (str, optional): Name of the data source database.
                If not provided, will try to infer from the DataModel.
            schema_name (str, optional): Name of the data source schema.
                If not provided, will try to infer from the DataModel.
            dataset_id (str, optional): ID of the dataset where the table will be created.
                If not provided, will try to infer from the DataModel.
            import_query (str, optional): SQL statement used as custom import query. Defaults to None.
            description (str, optional): Description for the table. Defaults to an empty string.
            tags (list, optional): List of tags to apply to the table. Defaults to None.
            build_behavior_config (dict, optional): Configuration for table build behavior.

        Returns:
            dict: Table object if created successfully or an error message.
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
                self.logger.warning(
                    f"Multiple datasets found in DataModel '{datamodel_name}'. "
                    f"Provide a dataset_id to specify which one to use."
                )
                return {
                    "error": (
                        f"Multiple datasets found in DataModel '{datamodel_name}'. "
                        f"Provide a dataset_id to specify which one to use."
                    )
                }

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

            self.logger.debug(
                f"Resolved Dataset ID: {dataset_id}, "
                f"Database Name: {database_name}, "
                f"Schema Name: {schema_name}, "
                f"Connection Name: {connection_name}"
            )

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
                self.logger.debug(
                    f"Resolved Dataset ID: {dataset_id}, "
                    f"Database Name: {database_name}, "
                    f"Schema Name: {schema_name}, "
                    f"Connection Name: {connection_name}"
                )
            else:
                self.logger.error(
                    f"Failed to retrieve dataset details for Dataset ID '{dataset_id}'. "
                    f"Status Code: {dataset.status_code}, Error: {dataset.text}"
                )
                return {"error": f"Failed to retrieve dataset details for Dataset ID '{dataset_id}'"}

        # Step 3: Fetch schema of the table to be created
        # Use provided database and schema if available, otherwise fallback to inferred values
        db_name_to_use = database_name if database_name else database_name
        schema_name_to_use = schema_name if schema_name else schema_name

        if not db_name_to_use or not schema_name_to_use:
            self.logger.error(f"Missing database or schema name for table '{table_name}'.")
            return {"error": f"Missing database or schema name for table '{table_name}'."}

        self.logger.debug(
            f"Fetching schema for table '{table_name}' in database '{db_name_to_use}' "
            f"and schema '{schema_name_to_use}' under connection '{connection_name}'"
        )
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

            formatted_columns.append({
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
                "isCustom": None
            })

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
            "type": "base"
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
                        self.logger.error(
                            f"Couldn't resolve column id matching. Column '{column_name}' "
                            f"not found in table '{table_name}'."
                        )
                        return {"error": f"Column '{column_name}' not found in table '{table_name}'"}
                    build_behavior = {
                        "type": "accumulativeSync",
                        "accumulativeConfig": {
                            "column": column_id,
                            "type": "lastStored",
                            "lastDays": None,
                            "keepOnlyDays": None
                        }
                    }
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
                    self.logger.error(
                        f"Failed to update table '{table_name}' build behavior. "
                        f"Status Code: {patch_response.status_code}, Error: {patch_response.text}"
                    )
                    return {"error": "Failed to update table build behavior"}

            return table

        error_msg = response.text if response else "No response from API."
        self.logger.error(f"Failed to create table '{table_name}' in DataModel '{datamodel_name}'. Error: {error_msg}")
        return {"error": "Failed to create table"}

    def setup_datamodel(self, datamodel_name, datamodel_type, connection_name, database_name, schema_name, tables,
                        dataset_name=None):
        """
        Setup a DataModel using existing connection and by creating a datamodel, dataset, and table.

        Parameters:
            datamodel_name (str): Name of the DataModel.
            datamodel_type (str): Type of DataModel. Should be either "extract" (for Elasticube) or "live" (for Live).
            connection_name (str): Name of the connection to use.
            database_name (str): Name of the data source database.
            schema_name (str): Name of the data source schema.
            dataset_name (str, optional): Name of the dataset. Defaults to schema name if not provided.
            tables (list): List of tables to create in the DataModel.
                Each table should be a dictionary with keys:
                "table_name", "import_query", "description", "tags", and "build_behavior_config".
                import_query (str, optional): SQL statement used as custom import query. Defaults to None.
                description (str, optional): Description for the table. Defaults to an empty string.
                tags (list, optional): List of tags to apply to the table. Defaults to None.
                build_behavior_config (dict, optional): Configuration for table build behavior.

        Returns:
            dict: A dictionary containing the full DataModel object on success or an error message on failure.
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
        dataset_response = self.create_dataset(
            datamodel_name=datamodel_name,
            connection_name=connection_name,
            database_name=database_name,
            schema_name=schema_name,
            dataset_name=dataset_name
        )
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
                build_behavior_config=table.get("build_behavior_config")
            )

            if "error" in table_response:
                self.logger.error(f"Failed to create table '{table_name}' in DataModel '{datamodel_name}'. Aborting.")
                return {"error": f"Failed to create table '{table_name}' in DataModel '{datamodel_name}'."}

            self.logger.debug(f"Table '{table_name}' created successfully.")
            created_tables.append(table_name)

        self.logger.info(f"DataModel '{datamodel_name}' setup successfully with tables: {created_tables}")
        self.logger.debug(f"[END] Setup DataModel '{datamodel_name}'")
        return {
            "datamodel_id": datamodel_id,
            "dataset_id": dataset_id,
            "tables": created_tables
        }

    def deploy_datamodel(self, datamodel_name, build_type="full", row_limit=0, schema_origin="latest"):
        """
        Deploy (build or publish) the specified DataModel based on its type.

        This method supports both Elasticube (EXTRACT) and Live models.
        The behavior and required parameters differ based on model type:

        - For Elasticube models:
            - build_type (str): Type of deployment. Options:
                * "schema_changes" — Build only schema changes
                * "by_table" — Build based on each table's config (e.g. incremental, accumulative)
                * "full" — Rebuild the entire model from scratch (default)
            - row_limit (int): Maximum number of rows to process. Defaults to 0 (no limit).
            - schema_origin (str): Schema source. Options:
                * "latest" — Build the schema as seen in the Data page (default)
                * "running" — Build from the last successfully built version

        - For Live models:
            - Only the `build_type` parameter is used internally and will be set to "publish"
            - `row_limit` and `schema_origin` are ignored

        Parameters:
            datamodel_name (str): Name of the DataModel to deploy.
            build_type (str): Type of deployment. Required for EXTRACT only.
            row_limit (int): Row limit for build. Applicable only for EXTRACT.
            schema_origin (str): Schema origin for build. Applicable only for EXTRACT.

        Returns:
            dict: Deployment result including status, or error details.
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
            payload = {
                "datamodelId": datamodel_id,
                "buildType": build_type,
                "rowLimit": row_limit,
                "schemaOrigin": schema_origin
            }
        elif datamodel_type.upper() == "LIVE":
            self.logger.debug(f"Preparing Live model publish for '{datamodel_name}'")
            payload = {
                "datamodelId": datamodel_id,
                "buildType": "publish"
            }
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
        if datamodel_type.upper() == "EXTRACT":
            last_build_publish = datamodel.get("lastBuildTime")
        else:
            last_build_publish = datamodel.get("lastPublishTime")

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
                table_info.append({
                    "table_name": table_name,
                    "table_type": table_type
                })

            dataset_info.append({
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "provider": provider,
                "connection_name": connection_name,
                "tables": table_info
            })

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
        if datamodel_type.upper() == "EXTRACT":
            last_build_publish = datamodel.get("lastBuildTime")
        else:
            last_build_publish = datamodel.get("lastPublishTime")

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
                    "table_type": dataset.get("type", "Unknown Type")
                }
                rows.append(row)

        self.logger.info(f"Flattened {len(rows)} rows from DataModel '{datamodel_name}'")
        return rows

    def get_datamodel_shares(self, datamodel_name):
        """
        Retrieves all share entries (users and groups) for a given DataModel in flat row format.

        Parameters:
            datamodel_name (str): Name of the DataModel to retrieve shares for.

        Returns:
            list: List of dicts with datamodel name, party name, type, and permission.
        """
        self.logger.debug(f"[START] Resolving share info for DataModel '{datamodel_name}'")

        # Step 1: Get datamodel object
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []

        datamodel_id = datamodel.get("oid")

        # Step 2: Fetch all users
        self.logger.debug("Fetching all users for share resolution.")
        users_response = self.api_client.get('/api/v1/users')
        users_detail = []
        if users_response and users_response.status_code == 200:
            users_data = users_response.json()
            users_detail = [{"id": user["_id"], "email": user.get("email", "Unknown Email")} for user in users_data]
        else:
            self.logger.warning("Could not fetch users for share resolution.")

        # Step 3: Fetch all groups
        self.logger.debug("Fetching all groups for share resolution.")
        groups_response = self.api_client.get('/api/v1/groups')
        groups_detail = []
        if groups_response and groups_response.status_code == 200:
            groups_data = groups_response.json()
            groups_detail = [{"id": group["_id"], "name": group.get("name", "Unknown Group")} for group in groups_data]
        else:
            self.logger.warning("Could not fetch groups for share resolution.")

        # Step 4: Parse shares
        permission_map = {"w": "EDIT", "a": "READ", "r": "USE"}
        shares = datamodel.get("shares", [])
        resolved_shares = []

        for share in shares:
            party_id = share.get("partyId")
            party_type = share.get("type")
            permission_code = share.get("permission", "")
            permission = permission_map.get(permission_code.lower(), permission_code)

            name = None
            if party_type == "user":
                user = next((u for u in users_detail if u["id"] == party_id), None)
                name = user["email"] if user else f"[Unknown user: {party_id}]"
            elif party_type == "group":
                group = next((g for g in groups_detail if g["id"] == party_id), None)
                name = group["name"] if group else f"[Unknown group: {party_id}]"

            resolved_shares.append({
                "datamodel_name": datamodel_name,
                "datamodel_id": datamodel_id,
                "party_name": name,
                "party_type": party_type,
                "permission": permission
            })

        self.logger.info(f"Resolved {len(resolved_shares)} share entries for DataModel '{datamodel_name}'")
        return resolved_shares

    def get_datasecurity(self, datamodel_name):
        """
        Retrieves datasecurity table and column entries for a given DataModel in flat row format.

        Parameters:
            datamodel_name (str): Name of the DataModel to retrieve datasecurity for.

        Returns:
            list: List of dicts with datamodel name, table name, column name, and security type.
                If no rules exist, a single row is returned with empty values and the datamodel name.
        """
        self.logger.debug(f"[START] Resolving datasecurity info for DataModel '{datamodel_name}'")

        # Step 1: Get datamodel object
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []

        datamodel_name = datamodel.get("title")
        datamodel_type = datamodel.get("type")

        # Step 2: Build API URL
        url = ""
        if datamodel_type.upper() == "EXTRACT":
            url = f"/api/elasticubes/localhost/{datamodel_name}/datasecurity"
        elif datamodel_type.upper() == "LIVE":
            url = f"/api/v1/elasticubes/live/{datamodel_name}/datasecurity"

        # Step 3: Fetch datasecurity
        self.logger.debug(f"Fetching datasecurity from '{url}'")
        datasecurity_response = self.api_client.get(url)
        if not datasecurity_response or datasecurity_response.status_code != 200:
            self.logger.warning(f"Could not fetch datasecurity for DataModel '{datamodel_name}'.")
            return [{
                "datamodel_name": datamodel_name,
                "table_name": "",
                "column_name": "",
                "data_type": ""
            }]

        datasecurity_data = datasecurity_response.json()
        self.logger.debug(f"Datasecurity data: {datasecurity_data}")

        # Step 4: Parse datasecurity
        datasecurity_info = []
        seen = set()  # track (table, column) pairs

        for rule in datasecurity_data:
            table_name = rule.get("table", "Unknown Table")
            column_name = rule.get("column", "Unknown Column")
            data_type = rule.get("datatype", "Unknown Type")

            key = (table_name, column_name)
            if key not in seen:
                datasecurity_info.append({
                    "datamodel_name": datamodel_name,
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": data_type
                })
                seen.add(key)

        if not datasecurity_info:
            self.logger.info(f"No datasecurity rules found for DataModel '{datamodel_name}'")
            return [{
                "datamodel_name": datamodel_name,
                "table_name": "",
                "column_name": "",
                "data_type": ""
            }]

        self.logger.info(f"Resolved {len(datasecurity_info)} datasecurity entries for DataModel '{datamodel_name}'")
        return datasecurity_info

    def get_datasecurity_detail(self, datamodel_name):
        """
        Retrieves detailed datasecurity rules for a specific DataModel, including share-level visibility.
        Each row represents a unique column-level rule and is repeated per share for clarity.

        Special handling is applied to interpret member values:
        - If "members" is an empty list and "exclusionary" is missing/null => interpreted as "Nothing"
        - If "members" is empty and "exclusionary" is False => interpreted as "Everything"
        - If values exist and "exclusionary" is True => treated as restricted subset

        Parameters:
            datamodel_name (str): Name of the DataModel to retrieve datasecurity rules for.

        Returns:
            list: A list of dictionaries representing datasecurity rules in flat, share-resolved format.
        """
        self.logger.debug(f"[START] Resolving datasecurity info for DataModel '{datamodel_name}'")

        # Step 1: Get datamodel object
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []

        datamodel_name = datamodel.get("title")
        datamodel_type = datamodel.get("type")

        # Step 2: Build API URL
        url = ""
        if datamodel_type.upper() == "EXTRACT":
            url = f"/api/elasticubes/localhost/{datamodel_name}/datasecurity"
        elif datamodel_type.upper() == "LIVE":
            url = f"/api/v1/elasticubes/live/{datamodel_name}/datasecurity"

        # Step 3: Fetch datasecurity
        self.logger.debug(f"Fetching datasecurity from '{url}'")
        datasecurity_response = self.api_client.get(url)
        if not datasecurity_response or datasecurity_response.status_code != 200:
            self.logger.warning(f"Could not fetch datasecurity for DataModel '{datamodel_name}'.")
            return [{
                "datamodel_name": datamodel_name,
                "table_name": "",
                "column_name": "",
                "data_type": "",
                "value": "",
                "exclusionary": "",
                "share_type": "",
                "share_name": "",
                "rule_description": ""
            }]

        datasecurity_data = datasecurity_response.json()
        self.logger.debug(f"Datasecurity data: {datasecurity_data}")

        # Step 4: Parse datasecurity rules
        detailed_rows = []

        if not datasecurity_data:
            self.logger.info(f"No datasecurity rules found for DataModel '{datamodel_name}'. Returning default row.")
            return [{
                "datamodel_name": datamodel_name,
                "table_name": "",
                "column_name": "",
                "data_type": "",
                "value": "",
                "exclusionary": "",
                "share_type": "",
                "share_name": "",
                "rule_description": ""
            }]

        for rule in datasecurity_data:
            table_name = rule.get("table", "Unknown Table")
            column_name = rule.get("column", "Unknown Column")
            data_type = rule.get("datatype", "Unknown Type")
            shares = rule.get("shares", [])
            members = rule.get("members", [])
            exclusionary = rule.get("exclusionary")

            if members:
                value = members
            elif exclusionary is False:
                value = "Everything"
            elif exclusionary is None:
                value = "Nothing"
            else:
                value = []

            if isinstance(value, list) and value:
                if exclusionary is True:
                    rule_description = f"Can see everything except {value}"
                elif exclusionary is False:
                    rule_description = f"Can see only {value}"
                else:
                    rule_description = "Unknown rule logic"
            elif value == "Nothing":
                rule_description = "Cannot see any value"
            elif value == "Everything":
                rule_description = "Can see all values"
            else:
                rule_description = "Unknown"

            if not shares:
                self.logger.warning(f"No shares found for datasecurity rule: {rule}")
                detailed_rows.append({
                    "datamodel_name": datamodel_name,
                    "table_name": table_name,
                    "column_name": column_name,
                    "data_type": data_type,
                    "value": value,
                    "exclusionary": exclusionary,
                    "share_type": "None",
                    "share_name": "None",
                    "rule_description": rule_description
                })
            else:
                for share in shares:
                    share_type = share.get("type", "Unknown Type")
                    share_name = share.get("partyName", "Unknown Share")

                    if share_type == "default":
                        share_type = "Everyone"
                        share_name = "Everyone"

                    detailed_rows.append({
                        "datamodel_name": datamodel_name,
                        "table_name": table_name,
                        "column_name": column_name,
                        "data_type": data_type,
                        "value": value,
                        "exclusionary": exclusionary,
                        "share_type": share_type,
                        "share_name": share_name,
                        "rule_description": rule_description
                    })

        detailed_rows.sort(key=lambda x: (x["table_name"], x["column_name"]))
        self.logger.info(
            f"Resolved {len(detailed_rows)} datasecurity share-level entries for DataModel '{datamodel_name}'"
        )

        return detailed_rows

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

    def add_datamodel_shares(self, datamodel_name, shares):
        """
        Adds share entries (users and groups) to a DataModel.

        Parameters:
            datamodel_name (str): Name of the DataModel to add shares to.
            shares (list): List of dictionaries containing share details. Each dictionary should have:
                - name: Name of the user or group
                - type: Type of the party (user or group)
                - permission: Permission level (EDIT, READ, USE)

        Returns:
            dict: Result of the share addition operation.
        """
        self.logger.debug(f"[START] Adding shares to DataModel '{datamodel_name}'")

        # Step 1: Get DataModel by name
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"error": f"DataModel '{datamodel_name}' not found."}

        datamodel_id = datamodel.get("oid")
        datamodel_type = datamodel.get("type")

        # Step 2: Get existing shares
        existing_shares = datamodel.get("shares", [])

        # Step 3: Fetch users
        self.logger.debug("Fetching all users for share resolution.")
        users_response = self.api_client.get('/api/v1/users')
        users_detail = []
        if users_response and users_response.status_code == 200:
            users_data = users_response.json()
            users_detail = [{"id": user["_id"], "email": user.get("email", "Unknown Email")} for user in users_data]
        else:
            self.logger.warning("Could not fetch users for share resolution.")

        # Step 4: Fetch groups
        self.logger.debug("Fetching all groups for share resolution.")
        groups_response = self.api_client.get('/api/v1/groups')
        groups_detail = []
        if groups_response and groups_response.status_code == 200:
            groups_data = groups_response.json()
            groups_detail = [{"id": group["_id"], "name": group.get("name", "Unknown Group")} for group in groups_data]
        else:
            self.logger.warning("Could not fetch groups for share resolution.")

        # Step 5: Prepare new shares with normalized permission
        reverse_permission_map = {"edit": "w", "read": "a", "use": "r"}
        new_shares = []

        for share in shares:
            name = share.get("name")
            share_type = share.get("type", "").lower()
            permission_raw = share.get("permission", "").lower()
            permission_short = reverse_permission_map.get(permission_raw, permission_raw)

            if share_type == "user":
                user = next((u for u in users_detail if u["email"] == name), None)
                if user:
                    new_shares.append({
                        "partyId": user["id"],
                        "type": "user",
                        "permission": permission_short
                    })
                else:
                    self.logger.warning(f"User '{name}' not found. Skipping share addition.")
            elif share_type == "group":
                group = next((g for g in groups_detail if g["name"] == name), None)
                if group:
                    new_shares.append({
                        "partyId": group["id"],
                        "type": "group",
                        "permission": permission_short
                    })
                else:
                    self.logger.warning(f"Group '{name}' not found. Skipping share addition.")
            else:
                self.logger.warning(f"Invalid share type '{share_type}' for '{name}'. Skipping share addition.")

        # Step 6: Combine existing and new shares
        self.logger.debug(f"Existing shares: {existing_shares}")
        self.logger.debug(f"New shares: {new_shares}")
        payload = existing_shares + new_shares

        # Step 7: Determine API endpoint
        if datamodel_type.upper() == "EXTRACT":
            return {"error": "Fixing Bug: Cannot add shares to EXTRACT DataModels. Will be fixed in V2."}
            endpoint = f"/api/elasticubes/localhost/{datamodel_id}/permissions"
        elif datamodel_type.upper() == "LIVE":
            endpoint = f"/api/v1/elasticubes/live/{datamodel_id}/permissions"
        else:
            self.logger.error(f"Unsupported DataModel type '{datamodel_type}' for '{datamodel_name}'.")
            return {"error": f"Unsupported DataModel type '{datamodel_type}' for '{datamodel_name}'."}

        # Step 8: Send POST request with payload
        self.logger.debug(f"Payload for adding shares to DataModel '{datamodel_name}': {payload}")
        response = self.api_client.patch(endpoint, data=payload)
        if response and response.status_code == 200:
            self.logger.info(f"Shares added successfully to DataModel '{datamodel_name}'")
            return response.json()
        else:
            error_text = response.text if response else "No response from API."
            self.logger.error(f"Failed to add shares to DataModel '{datamodel_name}'. Error: {error_text}")
            return {"error": f"Failed to add shares to DataModel '{datamodel_name}'."}

    def get_data(self, datamodel_name, table_name, query=None):
        """
        Retrieves data from a specific table in a DataModel and returns it as a list of dicts
        (row-based format) compatible with to_dataframe.

        Parameters:
            datamodel_name (str): Name of the DataModel.
            table_name (str): Name of the table to retrieve data from.
            query (str): Optional SQL query to filter the data.

        Returns:
            list: List of dictionaries where each dict represents a row.
        """
        self.logger.debug(f"[START] Retrieving data from DataModel '{datamodel_name}', Table '{table_name}'")

        if not datamodel_name or not table_name:
            self.logger.error("DataModel name and table name are required.")
            return []

        q = query if query else f"SELECT * FROM {table_name}"
        self.logger.debug(f"SQL Query: {q}")

        url = f"/api/datasources/{datamodel_name}/sql?query={q}"
        self.logger.debug(f"Resolved URL: {url}")

        response = self.api_client.get(url)

        if response and response.status_code == 200:
            raw = response.json()
            headers = raw.get("headers", [])
            values = raw.get("values", [])

            if not headers or not values:
                self.logger.warning("Empty data received.")
                return []

            # Convert to list of dicts
            rows = [dict(zip(headers, row)) for row in values]

            self.logger.info(f"Retrieved {len(rows)} rows from DataModel '{datamodel_name}', Table '{table_name}'")
            return rows

        else:
            error_text = response.text if response else "No response from API."
            self.logger.error(
                f"Failed to retrieve data from DataModel '{datamodel_name}', "
                f"Table '{table_name}'. Error: {error_text}"
            )
            return []

    def get_row_count(self, datamodel_name):
        """
        Retrieves the row count for each table in a specific DataModel
        and returns it in a flat row-based structure suitable for tabular representation.

        Parameters:
            datamodel_name (str): Name of the DataModel.

        Returns:
            list: List of dictionaries, each with 'table_name' and 'row_count'.
                Includes an additional row for total row count.
        """
        self.logger.debug(f"[START] Retrieving row count for DataModel '{datamodel_name}'")

        if not datamodel_name:
            self.logger.error("DataModel name is required.")
            return []

        # Step 1: Get DataModel by name
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return []

        table_names = []
        for dataset in datamodel.get("datasets", []):
            tables = dataset.get("schema", {}).get("tables", [])
            for table in tables:
                table_names.append(table.get("name"))
        self.logger.debug(f"Resolved table names: {table_names}")

        # Step 2: Get row count per table
        total_row_count = 0
        row_info = []

        for table_name in table_names:
            query = f"SELECT COUNT(*) FROM {table_name}"
            self.logger.debug(f"SQL Query for table '{table_name}': {query}")
            rows = self.get_data(datamodel_name, table_name, query=query)

            if not rows:
                self.logger.warning(f"No data retrieved for table '{table_name}'. Skipping.")
                continue

            if len(rows) == 1 and isinstance(rows[0], dict):
                row_count = rows[0].get("Column", 0)
                self.logger.debug(f"Row count for table '{table_name}': {row_count}")
                row_info.append({"table_name": table_name, "row_count": row_count})
                total_row_count += row_count
            else:
                self.logger.warning(f"Unexpected format for row count data in table '{table_name}'")

        # Step 3: Add total row count as a final row
        row_info.append({"table_name": "total_row_count", "row_count": total_row_count})
        self.logger.info(
            f"Completed row count collection for DataModel '{datamodel_name}'. "
            f"Total rows: {total_row_count}"
        )
        return row_info
