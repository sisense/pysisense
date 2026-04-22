from __future__ import annotations


class OwnershipMixin:
    def change_folder_and_dashboard_ownership(self, executing_user, folder_name, new_owner_name, original_owner_rule="edit", change_dashboard_ownership=True):
        """
        Method to change the ownership of folders and optionally dashboards.
        This method changes the ownership of a target folder and the entire
        tree structure surrounding it, including subfolders, sibling folders,
        and parent folders.
        Optionally, it will also change the ownership of dashboards associated
        with these folders.

        Parameters:
            user_name (str): The user running the tool. This is necessary for
            API access checks.
            folder_name (str): The target folder whose ownership needs to be
            changed.
            new_owner_name (str): The new owner to whom the folder (and
            optionally dashboards) ownership will be transferred.
            original_owner_rule (str, optional): Specifies the ownership rule
            to set original owner after changing ownership('edit' or 'view').
            Default is 'edit'.
            change_dashboard_ownership (bool, optional): Specifies whether to
            also change the ownership of dashboards in the folder tree. Default
            is True.
        """
        folder_details = set()
        dashboard_details = set()
        total_folders_changed = 0
        total_dashboards_changed = 0

        self.logger.info("Starting folder and dashboard traversal...")
        self.logger.debug(f"Looking for folder '{folder_name}' to change ownership to '{new_owner_name}'")

        matching_folders = []
        oid_to_parent_map = {}

        # Check if the executing user exists and retrieve their USER_ID
        user_info = self.get_user(executing_user)
        if not user_info or "USER_ID" not in user_info:
            error_msg = f"User '{executing_user}' not found or USER_ID missing."
            self.logger.error(error_msg)
            return {"error": error_msg}
        user_id = user_info["USER_ID"]

        # Check if the new owner exists and retrieve their USER_ID
        new_owner = self.get_user(new_owner_name)
        if not new_owner or "USER_ID" not in new_owner:
            error_msg = f"New owner '{new_owner_name}' not found or USER_ID missing."
            self.logger.error(error_msg)
            return {"error": error_msg}
        new_owner_id = new_owner["USER_ID"]

        # Build a parent map and collect matching folders
        def build_folder_map_and_find_matches(folders, parent=None):
            for folder in folders:
                oid_to_parent_map[folder["oid"]] = parent
                self.logger.debug(f"Checking folder '{folder['name']}' (ID: {folder['oid']})")

                if folder["name"] == folder_name:
                    self.logger.info(f"Found target folder: {folder['name']} (ID: {folder['oid']})")
                    matching_folders.append(folder)
                    traverse_folder(folder)

                if "folders" in folder and folder["folders"]:
                    build_folder_map_and_find_matches(folder["folders"], folder)

        # Traverse folder and dashboards
        def traverse_folder(folder):
            if (folder["oid"], folder["name"]) not in folder_details:
                folder_details.add((folder["oid"], folder["name"]))
                self.logger.info(f"Folder found: {folder['name']} (ID: {folder['oid']})")

                for dash in folder.get("dashboards", []):
                    if (dash["oid"], dash["title"]) not in dashboard_details:
                        dashboard_details.add((dash["oid"], dash["title"]))
                        self.logger.info(f"Dashboard found: {dash['title']} (ID: {dash['oid']})")

                for subfolder in folder.get("folders", []):
                    traverse_folder(subfolder)
            else:
                if not folder.get("folders"):
                    self.logger.debug(f"No subfolders in folder - {folder['name']}")

        # Traverse a folder's parent and siblings
        def traverse_parents_and_siblings(folder):
            parent = oid_to_parent_map.get(folder["oid"])
            if parent:
                self.logger.info(f"Parent folder: {parent['name']} (ID: {parent['oid']})")
                traverse_folder(parent)

                for sibling in parent.get("folders", []):
                    if sibling["oid"] != folder["oid"]:
                        traverse_folder(sibling)

        # Entry point to fetch and process folders
        def get_folder_details():
            self.logger.debug("Fetching all folders from API")
            response = self.api_client.get("/api/v1/navver")
            response = response.json()

            if not response or "folders" not in response:
                self.logger.error("No folders found in the API response or invalid response.")
                return False

            self.logger.info(f"Searching for folders named '{folder_name}'...")
            build_folder_map_and_find_matches(response["folders"])

            if not matching_folders:
                self.logger.warning(f"No folders named '{folder_name}' found.")
                return False

            for folder in matching_folders:
                traverse_parents_and_siblings(folder)

            self.logger.info(f"Total target folders matched: {len(matching_folders)}")
            return True

        folder_found = get_folder_details()

        if folder_found:
            self.logger.info("Collected Folder Details:")
            for folder_id, folder_name in folder_details:
                self.logger.info(f"Folder: {folder_name} (ID: {folder_id})")

            self.logger.info("Collected Dashboard Details:")
            for dash_id, dash_name in dashboard_details:
                self.logger.info(f"Dashboard: {dash_name} (ID: {dash_id})")
        else:
            self.logger.warning("Folder not found, moving to search dashboards and grant access step...")
            limit = 50
            skip = 0
            dashboards = []
            while True:
                self.logger.debug(f"Fetching dashboards (limit={limit}, skip={skip})")
                dashboard_response = self.api_client.post(
                    "/api/v1/dashboards/searches",
                    data={"queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True}, "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}},
                )
                dashboard_response = dashboard_response.json()

                if not dashboard_response or len(dashboard_response.get("items", [])) == 0:
                    self.logger.debug("No more dashboards found.")
                    break
                else:
                    dashboards.extend(dashboard_response["items"])
                    skip += limit

            all_folder_ids = {dic["parentFolder"] for dic in dashboards if "parentFolder" in dic and dic["parentFolder"]}
            self.logger.debug(f"Collected parent folder IDs from dashboards: {all_folder_ids}")

            folder_response = self.api_client.get("/api/v1/folders")
            folder_response = folder_response.json()
            user_folder_ids = {folder["oid"] for folder in folder_response if "oid" in folder}
            self.logger.debug(f"Collected user-accessible folder IDs: {user_folder_ids}")

            diff = all_folder_ids - user_folder_ids
            self.logger.info(f"Folders the user does not have access to: {diff}")

            for dash in dashboards:
                if "parentFolder" in dash and dash["parentFolder"] in diff:
                    payload = dash["shares"]
                    payload.append({"shareId": user_id, "type": "user", "rule": "edit", "subscribe": False})
                    self.logger.debug(f"Sharing dashboard {dash['title']} (ID: {dash['oid']}) with {executing_user}")
                    share_response = self.api_client.post(f"/api/shares/dashboard/{dash['oid']}?adminAccess=true", data={"sharesTo": payload})
                    share_response = share_response.json()
                    if share_response:
                        self.logger.info(f"Dashboard '{dash['title']}' shared with {executing_user}")
                    else:
                        self.logger.error(f"Failed to share dashboard '{dash['title']}': {share_response}")

            self.logger.info("Retrying folder and dashboard traversal after granting access...")
            folder_found = get_folder_details()

            if folder_found:
                self.logger.info("Collected Folder Details after granting access:")
                for folder_id, folder_name in folder_details:
                    self.logger.info(f"Folder: {folder_name} (ID: {folder_id})")

                self.logger.info("Collected Dashboard Details after granting access:")
                for dash_id, dash_name in dashboard_details:
                    self.logger.info(f"Dashboard: {dash_name} (ID: {dash_id})")
            else:
                self.logger.warning(f"Folder '{folder_name}' not found after attempting to grant access. Exiting...")
                return

        # Change ownership logic
        if folder_details or (change_dashboard_ownership and dashboard_details):
            self.logger.info("Changing folder and dashboard owners...")

            # Change folder owners
            self.logger.debug(f"Folders to be changed: {folder_details}")
            self.logger.info(f"Changing ownership for {len(folder_details)} folders and {len(dashboard_details)} dashboards.")
            for folder_id, folder_name in folder_details:
                data = {"owner": new_owner_id}
                self.logger.debug(f"Changing owner for folder {folder_name} (ID: {folder_id}) with data: {data}")

                response = self.api_client.patch(f"/api/v1/folders/{folder_id}", data=data)
                response = response.json()

                # Log response
                self.logger.debug(f"API response for folder change: {response}")

                if response and response.get("owner") == new_owner_id:
                    self.logger.info(f"Folder '{folder_name}' owner changed to {new_owner_name}")
                    total_folders_changed += 1
                else:
                    self.logger.error(f"Failed to change folder owner for '{folder_name}'.")

            # Change dashboard owners if enabled
            if change_dashboard_ownership:
                for dash_id, dash_name in dashboard_details:
                    current_dashboard = self.api_client.get(f"/api/v1/dashboards/{dash_id}")
                    current_dashboard = current_dashboard.json()
                    if not current_dashboard:
                        self.logger.error(f"Dashboard with ID '{dash_id}' not found. Skipping.")
                        continue

                    current_owner_id = current_dashboard.get("owner")

                    if current_owner_id == new_owner_id:
                        self.logger.info(f"Dashboard '{dash_name}' is already owned by {new_owner_name}, no action needed.")
                    else:
                        if current_owner_id == user_id:
                            data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                            response = self.api_client.post(f"/api/v1/dashboards/{dash_id}/change_owner", data=data)
                            response = response.json()
                        else:
                            data = {"ownerId": new_owner_id, "originalOwnerRule": original_owner_rule}
                            response = self.api_client.post(f"/api/v1/dashboards/{dash_id}/change_owner?adminAccess=true", data=data)
                            response = response.json()

                        if response:
                            self.logger.info(f"Dashboard '{dash_name}' owner changed to {new_owner_name}")
                            total_dashboards_changed += 1
                        else:
                            self.logger.error(f"Failed to change dashboard owner for '{dash_name}'.")

            # Log total changes
            self.logger.info(f"Ownership changed for {total_folders_changed} folders and {total_dashboards_changed} dashboards.")
            return {"total_folders_changed": total_folders_changed, "total_dashboards_changed": total_dashboards_changed}
        else:
            self.logger.info("No folders or dashboards to change ownership. Exiting.")
            return None
