from __future__ import annotations

from typing import Any


class ConnectionsMixin:
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
            self.logger.error(f"Failed to retrieve connections. Status Code: {response.status_code}, Error: {response.text}")
            return {"error": f"Failed to retrieve connections. Status Code: {response.status_code}"}

        connections = response.json()
        if not connections:
            self.logger.warning(f"No connections found with name '{connection_name}'")
            return {"error": f"No connections found with name '{connection_name}'"}

        self.logger.info(f"Successfully retrieved connections with name '{connection_name}'")
        self.logger.debug(f"Connection details: {connections}")
        return connections

    def get_connections(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all connections.

        Sends ``GET /api/v2/connections`` and returns the full connection list.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            List of connection objects on success, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/v2/connections"
        self.logger.debug("Fetching all connections.")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error("GET request to retrieve connections failed: No response received.")
            return {"error": "No response received while retrieving connections."}

        if not response.ok:
            error_message = response.json() if response else "No response text available."
            self.logger.error(f"Failed to retrieve connections. Error: {error_message}")
            return {"error": f"Failed to retrieve connections. {error_message}"}

        connections = response.json()
        count = len(connections) if isinstance(connections, list) else 0
        self.logger.info(f"Successfully retrieved {count} connections.")
        return connections

    def update_connection(self, connection_id: str, connection_data: dict[str, Any]) -> dict[str, Any]:
        """Update an existing connection.

        Sends ``PATCH /api/v2/connections/{connection_id}``. Only fields present
        in ``connection_data`` are sent in the request body; omitted fields are
        not modified on the server. Use for connection remapping during
        migration.

        Parameters
        ----------
        connection_id : str
            Connection ``oid`` to update.
        connection_data : dict[str, Any]
            Fields to update (for example ``name``, ``parameters``,
            ``provider``). Supported keys depend on the Sisense connection type.

        Returns
        -------
        dict[str, Any]
            Updated connection object on success, or ``{"error": "..."}`` on
            failure.
        """
        if not connection_data:
            self.logger.error("update_connection requires at least one field in connection_data.")
            return {"error": "connection_data must contain at least one field to update."}

        endpoint = f"/api/v2/connections/{connection_id}"
        self.logger.debug(f"Updating connection {connection_id} — fields: {list(connection_data.keys())}")
        response = self.api_client.patch(endpoint, data=connection_data)

        if response is None:
            self.logger.error(f"PATCH request to update connection {connection_id} failed: No response received.")
            return {"error": f"No response received while updating connection ID '{connection_id}'"}

        if not response.ok:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text if response else "No response text available."
            self.logger.error(f"Failed to update connection {connection_id}. Error: {error_message}")
            return {"error": f"Failed to update connection '{connection_id}'. {error_message}"}

        updated = response.json()
        self.logger.info(f"Successfully updated connection {connection_id}.")
        return updated

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
        payload = {"provider": connection_provider, "connectionData": {"connection": {"Database": database_name}, "schema": schema_name, "table": table_name}}

        response = self.api_client.post(endpoint, data=payload)

        # Step 3: Handle response
        if response is None:
            self.logger.error(f"No response received while retrieving schema for table '{table_name}'")
            return {"error": "No response from API while retrieving table schema"}

        if not response.ok:
            self.logger.error(f"Failed to retrieve schema for table '{table_name}'. Status Code: {response.status_code}, Error: {response.text}")
            return {"error": f"Failed to retrieve table schema. Status Code: {response.status_code}"}

        schema = response.json()
        if not schema:
            self.logger.warning(f"No schema data found for table '{table_name}'")
            return {"error": f"No schema found for table '{table_name}'"}

        self.logger.info(f"Successfully retrieved schema for table '{table_name}'")
        self.logger.debug(f"Table schema details: {schema}")
        return schema

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
                        "EC2Instance": False,
                    },
                    "supportedModelTypes": ["LIVE", "EXTRACT"],
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
                    "supportedModelTypes": ["LIVE", "EXTRACT"],
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
                        "DB": connection_params.get("database", ""),
                    },
                    "supportedModelTypes": ["LIVE", "EXTRACT"],
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
                    "supportedModelTypes": ["LIVE", "EXTRACT"],
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
