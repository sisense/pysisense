from __future__ import annotations


class SecurityMixin:
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
            return [{"datamodel_name": datamodel_name, "table_name": "", "column_name": "", "data_type": ""}]

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
                datasecurity_info.append({"datamodel_name": datamodel_name, "table_name": table_name, "column_name": column_name, "data_type": data_type})
                seen.add(key)

        if not datasecurity_info:
            self.logger.info(f"No datasecurity rules found for DataModel '{datamodel_name}'")
            return [{"datamodel_name": datamodel_name, "table_name": "", "column_name": "", "data_type": ""}]

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
            return [
                {"datamodel_name": datamodel_name, "table_name": "", "column_name": "", "data_type": "", "value": "", "exclusionary": "", "share_type": "", "share_name": "", "rule_description": ""}
            ]

        datasecurity_data = datasecurity_response.json()
        self.logger.debug(f"Datasecurity data: {datasecurity_data}")

        # Step 4: Parse datasecurity rules
        detailed_rows = []

        if not datasecurity_data:
            self.logger.info(f"No datasecurity rules found for DataModel '{datamodel_name}'. Returning default row.")
            return [
                {"datamodel_name": datamodel_name, "table_name": "", "column_name": "", "data_type": "", "value": "", "exclusionary": "", "share_type": "", "share_name": "", "rule_description": ""}
            ]

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
                detailed_rows.append(
                    {
                        "datamodel_name": datamodel_name,
                        "table_name": table_name,
                        "column_name": column_name,
                        "data_type": data_type,
                        "value": value,
                        "exclusionary": exclusionary,
                        "share_type": "None",
                        "share_name": "None",
                        "rule_description": rule_description,
                    }
                )
            else:
                for share in shares:
                    share_type = share.get("type", "Unknown Type")
                    share_name = share.get("partyName", "Unknown Share")

                    if share_type == "default":
                        share_type = "Everyone"
                        share_name = "Everyone"

                    detailed_rows.append(
                        {
                            "datamodel_name": datamodel_name,
                            "table_name": table_name,
                            "column_name": column_name,
                            "data_type": data_type,
                            "value": value,
                            "exclusionary": exclusionary,
                            "share_type": share_type,
                            "share_name": share_name,
                            "rule_description": rule_description,
                        }
                    )

        detailed_rows.sort(key=lambda x: (x["table_name"], x["column_name"]))
        self.logger.info(f"Resolved {len(detailed_rows)} datasecurity share-level entries for DataModel '{datamodel_name}'")

        return detailed_rows
