from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message

# Fields Sisense manages server-side on datasecurity rules; the API rejects
# them on write, so they are stripped before POSTing rules back.
_DATASECURITY_SERVER_FIELDS = frozenset({"_id", "created", "lastModified", "importedIdIdentifier"})


class SecurityMixin:
    def _build_datasecurity_url(self, resolved_name: str, datamodel_type: str) -> str | None:
        """Build the datasecurity endpoint for a resolved data model name/type, or ``None`` if unsupported."""
        normalized_type = (datamodel_type or "").upper()
        if normalized_type == "EXTRACT":
            return f"/api/elasticubes/localhost/{resolved_name}/datasecurity"
        if normalized_type == "LIVE":
            return f"/api/v1/elasticubes/live/{resolved_name}/datasecurity"
        return None

    def _fetch_datasecurity_rows(self, url: str) -> tuple[list[dict[str, Any]] | None, str | None]:
        """Fetch and parse raw datasecurity rows from a resolved endpoint.

        Returns ``(rows, error_message)``. ``rows`` is ``None`` when the fetch
        or parse failed, in which case ``error_message`` describes why.
        """
        self.logger.debug(f"Fetching datasecurity from '{url}'")
        response = self.api_client.get(url)
        if response is None or response.status_code != 200:
            return None, "Failed to fetch datasecurity rules."

        try:
            rows = response.json()
        except Exception:
            return None, "Invalid JSON returned while fetching datasecurity rules."

        self.logger.debug(f"Datasecurity data: {rows}")
        return rows, None

    def _fetch_datasecurity(self, datamodel_name: str) -> tuple[str | None, list[dict[str, Any]] | None]:
        """Resolve a data model and fetch its raw datasecurity rules.

        Returns ``(resolved_name, rows)`` where ``resolved_name`` is ``None`` when the
        model cannot be resolved, and ``rows`` is ``None`` when the fetch failed.
        """
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return None, None

        resolved_name = datamodel.get("title")
        datamodel_type = datamodel.get("type")
        url = self._build_datasecurity_url(resolved_name, datamodel_type) or ""

        rows, err = self._fetch_datasecurity_rows(url)
        if err:
            self.logger.warning(f"Could not fetch datasecurity for DataModel '{resolved_name}'.")
            return resolved_name, None

        return resolved_name, rows

    def get_datasecurity_raw(self, datamodel_name: str, datamodel_type: str | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve the raw, unprocessed datasecurity rules for a data model.

        Unlike ``get_datasecurity``/``get_datasecurity_detail``, this returns
        each rule exactly as the API provides it — including ``members``,
        ``exclusionary``, and raw ``shares`` — with no flattening,
        deduplication, or share-name resolution. Intended for callers that
        need to round-trip a full rule definition, for example migrating
        rules between environments.

        Parameters
        ----------
        datamodel_name : str
            Name (title) of the data model to retrieve raw datasecurity
            rules for.
        datamodel_type : str or None, optional
            The data model's type (``"extract"`` or ``"live"``), if already
            known. When provided, the datasecurity endpoint is built
            directly from ``datamodel_name`` and this type, skipping the
            data model resolve call. When omitted, the data model is
            resolved by name first (matching ``get_datasecurity``).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The raw list of datasecurity rule objects from the API, or
            ``{"error": "..."}`` on failure (including when the data model
            cannot be resolved).
        """
        if datamodel_type is not None:
            url = self._build_datasecurity_url(datamodel_name, datamodel_type)
            if url is None:
                msg = f"Unsupported datamodel_type '{datamodel_type}' for '{datamodel_name}'."
                self.logger.error(msg)
                return {"ok": False, "error": msg}

            rows, err = self._fetch_datasecurity_rows(url)
            if err:
                msg = f"{err} ('{datamodel_name}')"
                self.logger.error(msg)
                return {"ok": False, "error": msg}
            return rows

        resolved_name, datasecurity_data = self._fetch_datasecurity(datamodel_name)
        if resolved_name is None:
            return {"ok": False, "error": f"DataModel '{datamodel_name}' not found."}
        if datasecurity_data is None:
            return {"ok": False, "error": f"Failed to fetch raw datasecurity rules for '{resolved_name}'."}
        return datasecurity_data

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
            ``"column_name"``, and ``"data_type"``. Returns an empty list when
            the model has no rules, cannot be resolved, or the rules cannot be
            fetched (failure details are logged) — the row count always equals
            the number of secured columns.
        """
        self.logger.debug(f"[START] Resolving datasecurity info for DataModel '{datamodel_name}'")

        datamodel_name, datasecurity_data = self._fetch_datasecurity(datamodel_name)
        if datamodel_name is None:
            return []
        if datasecurity_data is None:
            return []

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
            return []

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
            ``"share_type"``, ``"share_name"``, and ``"rule_description"``. Returns
            an empty list when the model has no rules, cannot be resolved, or the
            rules cannot be fetched (failure details are logged) — the row count
            always equals the number of rule/share combinations.
        """
        self.logger.debug(f"[START] Resolving datasecurity info for DataModel '{datamodel_name}'")

        datamodel_name, datasecurity_data = self._fetch_datasecurity(datamodel_name)
        if datamodel_name is None:
            return []
        if datasecurity_data is None:
            return []

        # Step 4: Parse datasecurity rules
        detailed_rows = []

        if not datasecurity_data:
            self.logger.info(f"No datasecurity rules found for DataModel '{datamodel_name}'.")
            return []

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
        """Add datasecurity rules to an EXTRACT (Elasticube) datamodel.

        Sends ``POST /api/elasticubes/localhost/{datamodel_name}/datasecurity``
        with the rule list. The API **adds** the given rules (bulk); it does not
        replace existing ones — to replace a column's rules, remove them first
        with :meth:`delete_datasecurity`. Use this for a standalone
        ``migrate_datasecurity`` phase after the datamodel exists on the target.

        Server-managed fields (``_id``, ``created``, ``lastModified``,
        ``importedIdIdentifier``) are stripped from each rule automatically, so
        rules read back via ``get_datasecurity_raw`` can be re-submitted as-is.

        Parameters
        ----------
        datamodel_name : str
            Title of the EXTRACT datamodel to update.
        datasecurity : list[dict[str, Any]]
            Datasecurity rule list in Sisense API format. Each rule includes
            ``table``, ``column``, ``datatype``, ``members`` (list of strings),
            ``exclusionary``, ``shares``, and ``allMembers``.

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.

        Notes
        -----
        The Elasticube must be **built and running** — datasecurity writes are
        rejected by the API for unbuilt (draft) cubes.
        """
        if not isinstance(datasecurity, list):
            self.logger.error("update_datasecurity requires datasecurity to be a list.")
            return {"ok": False, "error": "datasecurity must be a list of rule objects."}

        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"ok": False, "error": datamodel["error"]}

        title = datamodel.get("title") or datamodel_name
        datamodel_type = datamodel.get("type", "")

        if datamodel_type.upper() != "EXTRACT":
            msg = f"update_datasecurity only supports EXTRACT datamodels; '{title}' is type '{datamodel_type}'."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        # Strip server-managed fields so read-back rules can be re-submitted.
        payload = [{k: v for k, v in rule.items() if k not in _DATASECURITY_SERVER_FIELDS} for rule in datasecurity]

        endpoint = f"/api/elasticubes/localhost/{title}/datasecurity"
        self.logger.debug(f"Adding datasecurity rules to EXTRACT datamodel '{title}' — {len(payload)} rule(s)")
        response = self.api_client.post(endpoint, data=payload)

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to update datasecurity for '{title}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

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
            requires ``table``, ``column``, ``datatype``, ``members`` (list of
            strings), ``exclusionary``, ``shares``, ``allMembers``, ``live``,
            and ``fullname`` (``"live:{title}"``). ``live`` and ``fullname``
            are filled in automatically when omitted; server-managed fields
            (``_id``, ``created``, ``lastModified``, ``importedIdIdentifier``)
            are stripped automatically.

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.

        Notes
        -----
        The LIVE datamodel must be **published** — the API answers
        ``"Elasticube has not been found"`` for unpublished (draft) live
        models.
        """
        if not isinstance(rules, list):
            self.logger.error("set_live_datasecurity_add_many requires rules to be a list.")
            return {"ok": False, "error": "rules must be a list of rule objects."}

        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"ok": False, "error": datamodel["error"]}

        title = datamodel.get("title") or datamodel_name
        datamodel_type = datamodel.get("type", "")

        if datamodel_type.upper() != "LIVE":
            msg = f"set_live_datasecurity_add_many only supports LIVE datamodels; '{title}' is type '{datamodel_type}'."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        # Fill derivable required fields and strip server-managed ones so
        # read-back rules can be re-submitted as-is.
        payload = []
        for rule in rules:
            cleaned = {k: v for k, v in rule.items() if k not in _DATASECURITY_SERVER_FIELDS}
            cleaned.setdefault("live", True)
            cleaned.setdefault("fullname", f"live:{title}")
            payload.append(cleaned)

        endpoint = f"/api/v1/elasticubes/live/{title}/datasecurity/addMany"
        self.logger.debug(f"Adding datasecurity rules to LIVE datamodel '{title}' — {len(payload)} rule(s)")
        response = self.api_client.post(endpoint, data=payload)

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to add datasecurity rules for '{title}'", self.api_client)
            if "has not been found" in failure["error"]:
                failure["error"] += " (the LIVE datamodel must be published — draft live models are not registered with the datasecurity API)"
            self.logger.error(failure["error"])
            return failure

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully added datasecurity rules to LIVE datamodel '{title}'.")
        return result

    def delete_datasecurity(self, datamodel_name: str, table: str, column: str) -> dict[str, Any]:
        """Delete all datasecurity rules for one table/column of a datamodel.

        Sends ``DELETE {datasecurity_endpoint}/{table}/{column}`` using the
        endpoint flavor for the model's type (EXTRACT or LIVE). Combined with
        ``update_datasecurity`` / ``set_live_datasecurity_add_many`` (which
        add rules), this enables replace semantics: delete the column's rules,
        then add the new ones.

        Parameters
        ----------
        datamodel_name : str
            Title of the datamodel.
        table : str
            Table name the rules apply to.
        column : str
            Column name the rules apply to.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success (the API answers 200 or 204), or
            ``{"error": "..."}`` on failure.
        """
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"ok": False, "error": datamodel["error"]}

        title = datamodel.get("title") or datamodel_name
        base_url = self._build_datasecurity_url(title, datamodel.get("type", ""))
        if base_url is None:
            msg = f"delete_datasecurity does not support datamodel type '{datamodel.get('type')}'."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        endpoint = f"{base_url}/{table}/{column}"
        self.logger.debug(f"Deleting datasecurity rules for '{title}' — {table}.{column}")
        response = self.api_client.delete(endpoint)

        if response is None or response.status_code not in (200, 204):
            failure = _extract_error_message(response, f"Failed to delete datasecurity rules for '{title}' ({table}.{column})", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Deleted datasecurity rules for '{title}' — {table}.{column}.")
        return {"success": True}
