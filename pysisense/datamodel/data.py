from __future__ import annotations


class DataMixin:
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
            rows = [dict(zip(headers, row, strict=False)) for row in values]

            self.logger.info(f"Retrieved {len(rows)} rows from DataModel '{datamodel_name}', Table '{table_name}'")
            return rows

        else:
            error_text = response.text if response else "No response from API."
            self.logger.error(f"Failed to retrieve data from DataModel '{datamodel_name}', Table '{table_name}'. Error: {error_text}")
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
        self.logger.info(f"Completed row count collection for DataModel '{datamodel_name}'. Total rows: {total_row_count}")
        return row_info
