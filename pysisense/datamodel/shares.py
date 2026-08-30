from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class SharesMixin:
    def _fetch_users_and_groups_detail_lists(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Fetch users and groups as ``{"id": ..., "email"/"name": ...}`` detail lists.

        Shared by ``get_datamodel_shares`` and ``add_datamodel_shares`` for
        resolving share ``partyId``s to readable names.

        Returns
        -------
        tuple[list[dict[str, Any]], list[dict[str, Any]]]
            ``(users_detail, groups_detail)``. Either list is empty if its
            corresponding API call fails.
        """
        self.logger.debug("Fetching all users for share resolution.")
        users_response = self.api_client.get("/api/v1/users")
        users_detail = []
        if users_response and users_response.status_code == 200:
            users_data = users_response.json()
            users_detail = [{"id": user["_id"], "email": user.get("email", "Unknown Email"), "active": user.get("active")} for user in users_data]
        else:
            self.logger.warning("Could not fetch users for share resolution.")

        self.logger.debug("Fetching all groups for share resolution.")
        groups_response = self.api_client.get("/api/v1/groups")
        groups_detail = []
        if groups_response and groups_response.status_code == 200:
            groups_data = groups_response.json()
            groups_detail = [{"id": group["_id"], "name": group.get("name", "Unknown Group")} for group in groups_data]
        else:
            self.logger.warning("Could not fetch groups for share resolution.")

        return users_detail, groups_detail

    def get_datamodel_shares(self, datamodel_name: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all share entries (users and groups) for a given data model.

        Resolves user and group identifiers to names/emails and returns the shares
        in a flat row format. Permission codes are mapped to ``"EDIT"``, ``"READ"``,
        or ``"USE"``.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model to retrieve shares for.

        Returns
        -------
        list[dict[str, Any]]
            List of dicts, each with ``"datamodel_name"``, ``"datamodel_id"``,
            ``"party_name"``, ``"party_type"``, and ``"permission"`` — an empty
            list means the model genuinely has no shares. On failure, returns
            the standard ``{"ok": False, "error": "...", ...}`` dict.
        """
        self.logger.debug(f"[START] Resolving share info for DataModel '{datamodel_name}'")

        # Step 1: Get datamodel object
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return datamodel

        datamodel_id = datamodel.get("oid")

        # Step 2: Fetch users and groups for share resolution
        users_detail, groups_detail = self._fetch_users_and_groups_detail_lists()

        # Step 3: Parse shares
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

            resolved_shares.append({"datamodel_name": datamodel_name, "datamodel_id": datamodel_id, "party_name": name, "party_type": party_type, "permission": permission})

        self.logger.info(f"Resolved {len(resolved_shares)} share entries for DataModel '{datamodel_name}'")
        return resolved_shares

    def add_datamodel_shares(self, datamodel_name: str, shares: list[dict[str, Any]]) -> dict[str, Any]:
        """Add share entries (users and groups) to a data model.

        Resolves each share's user email or group name to its identifier, merges
        the new shares with the existing ones, and submits the combined share list.

        Parameters
        ----------
        datamodel_name : str
            Name of the data model to add shares to.
        shares : list[dict[str, Any]]
            List of share definitions to add. Each dictionary should include:
            ``name`` (user email or group name), ``type`` (``"user"`` or
            ``"group"``), and ``permission`` (one of ``"EDIT"``, ``"READ"``,
            ``"USE"``).

        Returns
        -------
        dict[str, Any]
            API response on success, or the standard
            ``{"ok": False, "error": "...", ...}`` dict on failure (including
            when none of the given shares can be resolved to an existing user
            or group — no changes are written in that case).

        Notes
        -----
        Both model types key share entries by ``partyId``, but use different
        endpoints and verbs: EXTRACT permissions are written via
        ``PUT /api/elasticubes/localhost/{title}/permissions`` (by title),
        LIVE via ``PATCH /api/v1/elasticubes/live/{oid}/permissions`` (by
        oid). A share for a party that already has one updates that party's
        permission instead of duplicating the entry (EXTRACT path). Shares
        for inactive users are skipped with a warning — Sisense accepts the
        write but silently drops such entries, so submitting them would
        report success for a share that never lands.
        """
        self.logger.debug(f"[START] Adding shares to DataModel '{datamodel_name}'")

        # Step 1: Get DataModel by name
        datamodel = self.get_datamodel(datamodel_name)
        if "error" in datamodel:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"ok": False, "error": f"DataModel '{datamodel_name}' not found."}

        datamodel_id = datamodel.get("oid")
        datamodel_type = datamodel.get("type")
        datamodel_title = datamodel.get("title", datamodel_name)

        # Step 2: Fetch users and groups for share resolution
        users_detail, groups_detail = self._fetch_users_and_groups_detail_lists()

        # Step 3: Prepare new shares with normalized permission
        reverse_permission_map = {"edit": "w", "read": "a", "use": "r"}
        new_shares = []

        for share in shares:
            name = share.get("name")
            share_type = share.get("type", "").lower()
            permission_raw = share.get("permission", "").lower()
            permission_short = reverse_permission_map.get(permission_raw, permission_raw)

            if share_type == "user":
                user = next((u for u in users_detail if u["email"] == name), None)
                if user is None:
                    self.logger.warning(f"User '{name}' not found. Skipping share addition.")
                elif user.get("active") is False:
                    # Live-verified: Sisense silently drops permission entries
                    # for inactive users (HTTP 200, entry never lands) — skip
                    # loudly instead of letting the write pretend it worked.
                    self.logger.warning(f"User '{name}' is inactive — Sisense ignores shares for inactive users. Skipping share addition.")
                else:
                    new_shares.append({"partyId": user["id"], "type": "user", "permission": permission_short})
            elif share_type == "group":
                group = next((g for g in groups_detail if g["name"] == name), None)
                if group:
                    new_shares.append({"partyId": group["id"], "type": "group", "permission": permission_short})
                else:
                    self.logger.warning(f"Group '{name}' not found. Skipping share addition.")
            else:
                self.logger.warning(f"Invalid share type '{share_type}' for '{name}'. Skipping share addition.")

        if not new_shares:
            # Writing the existing shares back unchanged would read as success
            # while adding nothing — fail loudly instead.
            error_msg = f"None of the given shares could be resolved for DataModel '{datamodel_name}' — no changes made."
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}

        # Step 4: Route by model type — both endpoints key entries by
        # "partyId", but EXTRACT is a PUT by title and LIVE a PATCH by oid.
        self.logger.debug(f"New shares: {new_shares} (type={datamodel_type})")

        if datamodel_type.upper() == "EXTRACT":
            existing_raw = self.get_datamodel_permissions_extract(datamodel_title)
            if isinstance(existing_raw, dict):
                return existing_raw

            combined = list(existing_raw)
            for share in new_shares:
                party_id = share["partyId"]
                existing_entry = next((e for e in combined if e.get("partyId") == party_id), None)
                if existing_entry is not None:
                    self.logger.debug(f"Party '{party_id}' already has a share — updating its permission to '{share['permission']}'.")
                    existing_entry["permission"] = share["permission"]
                else:
                    combined.append(share)

            result = self.update_datamodel_permissions_extract(datamodel_title, combined)
            if result.get("ok") is False or "error" in result:
                return result
            self.logger.info(f"Shares added successfully to DataModel '{datamodel_name}'")
            return result

        elif datamodel_type.upper() == "LIVE":
            endpoint = f"/api/v1/elasticubes/live/{datamodel_id}/permissions"
        else:
            self.logger.error(f"Unsupported DataModel type '{datamodel_type}' for '{datamodel_name}'.")
            return {"ok": False, "error": f"Unsupported DataModel type '{datamodel_type}' for '{datamodel_name}'."}

        # Step 5 (LIVE): merge with the schema's existing shares and PATCH
        existing_shares = datamodel.get("shares", [])
        self.logger.debug(f"Existing shares: {existing_shares}")
        payload = existing_shares + new_shares

        self.logger.debug(f"Payload for adding shares to DataModel '{datamodel_name}': {payload}")
        response = self.api_client.patch(endpoint, data=payload)
        if response and response.status_code == 200:
            self.logger.info(f"Shares added successfully to DataModel '{datamodel_name}'")
            return response.json()
        else:
            failure = _extract_error_message(response, f"Failed to add shares to DataModel '{datamodel_name}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

    def get_datamodel_permissions_extract(self, datamodel_title: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve raw share entries for an EXTRACT (Elasticube) data model.

        Sends ``GET /api/elasticubes/localhost/{datamodel_title}/permissions``
        and returns the raw ``shares`` list — each entry keyed by
        ``partyId`` and not resolved to a user/group name. Intended for
        callers that need to round-trip shares as-is (for example, migrating
        them between environments). Use ``get_datamodel_shares`` instead for
        a resolved, human-readable view.

        Parameters
        ----------
        datamodel_title : str
            Title of the EXTRACT data model.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The raw list of share objects from the API, or
            ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/elasticubes/localhost/{datamodel_title}/permissions"
        self.logger.debug(f"GET {endpoint}")
        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to fetch permissions for EXTRACT datamodel '{datamodel_title}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            payload = response.json()
        except Exception:
            msg = f"Invalid JSON returned while fetching permissions for '{datamodel_title}'."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        shares = payload.get("shares", []) if isinstance(payload, dict) else []
        self.logger.info(f"Retrieved {len(shares)} raw share(s) for EXTRACT datamodel '{datamodel_title}'.")
        return shares

    def get_datamodel_permissions_live(self, datamodel_id: str) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve raw share entries for a LIVE data model.

        Sends ``GET /api/v1/elasticubes/live/{datamodel_id}/permissions`` and
        returns the raw share list — each entry keyed by ``partyId`` and not
        resolved to a user/group name. Intended for callers that need to
        round-trip shares as-is (for example, migrating them between
        environments).

        Parameters
        ----------
        datamodel_id : str
            OID of the LIVE data model.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The raw list of share objects from the API, or
            ``{"error": "..."}`` on failure.
        """
        endpoint = f"/api/v1/elasticubes/live/{datamodel_id}/permissions"
        self.logger.debug(f"GET {endpoint}")
        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to fetch permissions for LIVE datamodel '{datamodel_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            payload = response.json()
        except Exception:
            msg = f"Invalid JSON returned while fetching permissions for '{datamodel_id}'."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        shares = payload if isinstance(payload, list) else []
        self.logger.info(f"Retrieved {len(shares)} raw share(s) for LIVE datamodel '{datamodel_id}'.")
        return shares

    def update_datamodel_permissions_extract(self, datamodel_title: str, shares: list[dict[str, Any]]) -> dict[str, Any]:
        """Replace share entries for an EXTRACT (Elasticube) data model.

        Sends ``PUT /api/elasticubes/localhost/{datamodel_title}/permissions``
        with the full raw share list (each entry keyed by ``partyId``). Use
        ``add_datamodel_shares`` instead for name/email-based share
        management.

        Uses ``PUT`` because that is what the EXTRACT permissions endpoint
        requires — the LIVE counterpart, ``update_datamodel_permissions_live``,
        requires ``PATCH`` instead. This is an API difference between the two
        endpoints, not an inconsistency between the two methods.

        Parameters
        ----------
        datamodel_title : str
            Title of the EXTRACT data model.
        shares : list[dict[str, Any]]
            Raw share objects, each with ``partyId``, ``type`` (``"user"`` or
            ``"group"``), and ``permission``.

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.
        """
        if not isinstance(shares, list):
            self.logger.error("update_datamodel_permissions_extract requires shares to be a list.")
            return {"ok": False, "error": "shares must be a list of share objects."}

        endpoint = f"/api/elasticubes/localhost/{datamodel_title}/permissions"
        self.logger.debug(f"PUT {endpoint} — {len(shares)} share(s)")
        response = self.api_client.put(endpoint, data=shares)

        if response is None or response.status_code not in (200, 201):
            failure = _extract_error_message(response, f"Failed to update permissions for EXTRACT datamodel '{datamodel_title}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully updated permissions for EXTRACT datamodel '{datamodel_title}'.")
        return result

    def update_datamodel_permissions_live(self, datamodel_id: str, shares: list[dict[str, Any]]) -> dict[str, Any]:
        """Replace share entries for a LIVE data model.

        Sends ``PATCH /api/v1/elasticubes/live/{datamodel_id}/permissions``
        with the full raw share list (each entry keyed by ``partyId``). The
        LIVE model must already be published — publish it first with
        ``deploy_datamodel`` if it has never been built.

        Uses ``PATCH`` because that is what the LIVE permissions endpoint
        requires — the EXTRACT counterpart, ``update_datamodel_permissions_extract``,
        requires ``PUT`` instead. This is an API difference between the two
        endpoints, not an inconsistency between the two methods.

        Parameters
        ----------
        datamodel_id : str
            OID of the LIVE data model.
        shares : list[dict[str, Any]]
            Raw share objects, each with ``partyId``, ``type`` (``"user"`` or
            ``"group"``), and ``permission``.

        Returns
        -------
        dict[str, Any]
            API response on success, or ``{"error": "..."}`` on failure.
        """
        if not isinstance(shares, list):
            self.logger.error("update_datamodel_permissions_live requires shares to be a list.")
            return {"ok": False, "error": "shares must be a list of share objects."}

        endpoint = f"/api/v1/elasticubes/live/{datamodel_id}/permissions"
        self.logger.debug(f"PATCH {endpoint} — {len(shares)} share(s)")
        response = self.api_client.patch(endpoint, data=shares)

        if response is None or response.status_code not in (200, 201):
            failure = _extract_error_message(response, f"Failed to update permissions for LIVE datamodel '{datamodel_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            result = response.json()
        except Exception:
            result = {"success": True}

        self.logger.info(f"Successfully updated permissions for LIVE datamodel '{datamodel_id}'.")
        return result
