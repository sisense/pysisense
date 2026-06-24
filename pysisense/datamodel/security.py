from __future__ import annotations

from typing import Any


class SecurityMixin:
    def _fetch_datasecurity(self, datamodel_name: str) -> tuple[str | None, list[dict[str, Any]] | None]:
        """Resolve a data model and fetch its raw datasecurity rules.

        Returns ``(resolved_name, rows)`` where ``resolved_name`` is ``None`` when the
        model cannot be resolved, and ``rows`` is ``None`` when the fetch failed.
        """
        # Step 1: Get datamodel object
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return None, None

        resolved_name = datamodel.get("title")
        datamodel_type = datamodel.get("type")

        # Step 2: Build API URL
        url = ""
        if datamodel_type.upper() == "EXTRACT":
            url = f"/api/elasticubes/localhost/{resolved_name}/datasecurity"
        elif datamodel_type.upper() == "LIVE":
            url = f"/api/v1/elasticubes/live/{resolved_name}/datasecurity"

        # Step 3: Fetch datasecurity
        self.logger.debug(f"Fetching datasecurity from '{url}'")
        datasecurity_response = self.api_client.get(url)
        if not datasecurity_response or datasecurity_response.status_code != 200:
            self.logger.warning(f"Could not fetch datasecurity for DataModel '{resolved_name}'.")
            return resolved_name, None

        datasecurity_data = datasecurity_response.json()
        self.logger.debug(f"Datasecurity data: {datasecurity_data}")
        return resolved_name, datasecurity_data

    def get_datasecurity(self, datamodel_name: str) -> list[dict[str, Any]]:
        """Retrieve datasecurity table and column entries for a given data model.

        Resolves the data model, fetches its datasecurity rules, and returns the
        unique table/column entries in a flat row format.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model to retrieve datasecurity for.

        Returns
        -------
        list[dict[str, Any]]
            List of dicts, each with ``"datamodel_name"``, ``"table_name"``,
            ``"column_name"``, and ``"data_type"``. If no rules exist, a single row
            is returned with empty values and the data model name.
        """
        self.logger.debug(f"[START] Resolving datasecurity info for DataModel '{datamodel_name}'")

        datamodel_name, datasecurity_data = self._fetch_datasecurity(datamodel_name)
        if datamodel_name is None:
            return []
        if datasecurity_data is None:
            return [{"datamodel_name": datamodel_name, "table_name": "", "column_name": "", "data_type": ""}]

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

    def get_datasecurity_detail(self, datamodel_name: str) -> list[dict[str, Any]]:
        """Retrieve detailed datasecurity rules for a data model, including share-level visibility.

        Each row represents a unique column-level rule and is repeated per share for
        clarity. Special handling is applied to interpret member values:

        - If ``members`` is an empty list and ``exclusionary`` is missing/null, it is
          interpreted as "Nothing".
        - If ``members`` is empty and ``exclusionary`` is ``False``, it is interpreted
          as "Everything".
        - If values exist and ``exclusionary`` is ``True``, it is treated as a
          restricted subset.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model to retrieve datasecurity rules for.

        Returns
        -------
        list[dict[str, Any]]
            List of dicts representing datasecurity rules in flat, share-resolved
            format, each with ``"datamodel_name"``, ``"table_name"``,
            ``"column_name"``, ``"data_type"``, ``"value"``, ``"exclusionary"``,
            ``"share_type"``, ``"share_name"``, and ``"rule_description"``. Returns a
            single default row when no rules exist or on failure.
        """
        self.logger.debug(f"[START] Resolving datasecurity info for DataModel '{datamodel_name}'")

        datamodel_name, datasecurity_data = self._fetch_datasecurity(datamodel_name)
        if datamodel_name is None:
            return []
        if datasecurity_data is None:
            return [
                {"datamodel_name": datamodel_name, "table_name": "", "column_name": "", "data_type": "", "value": "", "exclusionary": "", "share_type": "", "share_name": "", "rule_description": ""}
            ]

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

    def update_datasecurity(self, datamodel_name: str, datasecurity: list[dict[str, Any]]) -> dict[str, Any]:
        """Replace datasecurity rules on an EXTRACT (Elasticube) datamodel.

        Sends ``PUT /api/elasticubes/localhost/{datamodel_name}/datasecurity``
        with the full datasecurity payload. Use this for a standalone
        ``migrate_datasecurity`` phase after the datamodel exists on the target.

        Parameters
        ----------
        datamodel_name : str
            Title of the EXTRACT datamodel to update.
        datasecurity : list[dict[str, Any]]
            Complete datasecurity rule list in Sisense API format. Each rule
            typically includes fields such as ``table``, ``column``,
            ``datatype``, ``members``, ``exclusionary``, and ``shares``.

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.
        """
        if not isinstance(datasecurity, list):
            self.logger.error("update_datasecurity requires datasecurity to be a list.")
            return {"error": "datasecurity must be a list of rule objects."}

        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"error": datamodel["error"]}

        title = datamodel.get("title") or datamodel_name
        datamodel_type = datamodel.get("type", "")

        if datamodel_type.upper() != "EXTRACT":
            msg = f"update_datasecurity only supports EXTRACT datamodels; '{title}' is type '{datamodel_type}'."
            self.logger.error(msg)
            return {"error": msg}

        endpoint = f"/api/elasticubes/localhost/{title}/datasecurity"
        self.logger.debug(f"Updating datasecurity for EXTRACT datamodel '{title}' — {len(datasecurity)} rule(s)")
        response = self.api_client.put(endpoint, data=datasecurity)

        if response is None:
            self.logger.error(f"PUT request to update datasecurity for '{title}' failed: No response received.")
            return {"error": f"No response received while updating datasecurity for '{title}'."}

        if not response.ok:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text
            self.logger.error(f"Failed to update datasecurity for '{title}'. Error: {error_message}")
            return {"error": f"Failed to update datasecurity for '{title}'. {error_message}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully updated datasecurity for EXTRACT datamodel '{title}'.")
        return result

    def set_live_datasecurity_add_many(self, datamodel_name: str, rules: list[dict[str, Any]]) -> dict[str, Any]:
        """Add multiple datasecurity rules to a LIVE datamodel.

        Sends ``POST /api/v1/elasticubes/live/{datamodel_name}/datasecurity/addMany``
        with a bulk rule payload.

        Parameters
        ----------
        datamodel_name : str
            Title of the LIVE datamodel to update.
        rules : list[dict[str, Any]]
            Datasecurity rules to add in Sisense API format. Each rule
            typically includes fields such as ``table``, ``column``,
            ``datatype``, ``members``, ``exclusionary``, and ``shares``.

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.
        """
        if not isinstance(rules, list):
            self.logger.error("set_live_datasecurity_add_many requires rules to be a list.")
            return {"error": "rules must be a list of rule objects."}

        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"error": datamodel["error"]}

        title = datamodel.get("title") or datamodel_name
        datamodel_type = datamodel.get("type", "")

        if datamodel_type.upper() != "LIVE":
            msg = f"set_live_datasecurity_add_many only supports LIVE datamodels; '{title}' is type '{datamodel_type}'."
            self.logger.error(msg)
            return {"error": msg}

        endpoint = f"/api/v1/elasticubes/live/{title}/datasecurity/addMany"
        self.logger.debug(f"Adding datasecurity rules to LIVE datamodel '{title}' — {len(rules)} rule(s)")
        response = self.api_client.post(endpoint, data=rules)

        if response is None:
            self.logger.error(f"POST request to add datasecurity rules for '{title}' failed: No response received.")
            return {"error": f"No response received while adding datasecurity rules for '{title}'."}

        if not response.ok:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text
            self.logger.error(f"Failed to add datasecurity rules for '{title}'. Error: {error_message}")
            return {"error": f"Failed to add datasecurity rules for '{title}'. {error_message}"}

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully added datasecurity rules to LIVE datamodel '{title}'.")
        return result
