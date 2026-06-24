from __future__ import annotations

from typing import Any


class SharesMixin:
    def get_datamodel_shares(self, datamodel_name: str) -> list[dict[str, Any]]:
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
            ``"party_name"``, ``"party_type"``, and ``"permission"``. Returns an
            empty list on failure.
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
        users_response = self.api_client.get("/api/v1/users")
        users_detail = []
        if users_response and users_response.status_code == 200:
            users_data = users_response.json()
            users_detail = [{"id": user["_id"], "email": user.get("email", "Unknown Email")} for user in users_data]
        else:
            self.logger.warning("Could not fetch users for share resolution.")

        # Step 3: Fetch all groups
        self.logger.debug("Fetching all groups for share resolution.")
        groups_response = self.api_client.get("/api/v1/groups")
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
            API response on success, or ``{"error": "..."}`` on failure.
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
        users_response = self.api_client.get("/api/v1/users")
        users_detail = []
        if users_response and users_response.status_code == 200:
            users_data = users_response.json()
            users_detail = [{"id": user["_id"], "email": user.get("email", "Unknown Email")} for user in users_data]
        else:
            self.logger.warning("Could not fetch users for share resolution.")

        # Step 4: Fetch groups
        self.logger.debug("Fetching all groups for share resolution.")
        groups_response = self.api_client.get("/api/v1/groups")
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
                    new_shares.append({"partyId": user["id"], "type": "user", "permission": permission_short})
                else:
                    self.logger.warning(f"User '{name}' not found. Skipping share addition.")
            elif share_type == "group":
                group = next((g for g in groups_detail if g["name"] == name), None)
                if group:
                    new_shares.append({"partyId": group["id"], "type": "group", "permission": permission_short})
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
