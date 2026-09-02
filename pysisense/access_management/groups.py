from __future__ import annotations

from typing import Any

from typing_extensions import deprecated

from ..utils import _extract_error_message

# Groups Sisense auto-populates with every user on the instance. In the
# all-groups view they duplicate get_users_all() and drown the real
# memberships, so they are omitted by default. Asking for one by name still
# returns it — nothing is unreachable.
_UNIVERSAL_GROUPS = frozenset({"Everyone", "All users in system"})


class GroupsMixin:
    @deprecated("use get_groups")
    def get_group(self, name: str) -> dict[str, Any]:
        """Retrieve group details by name.

        Deprecated alias kept for backward compatibility (behavior frozen) —
        prefer :meth:`get_groups` with its optional ``name`` filter, which
        returns the raw group objects.

        Looks up a group by its name and returns its ID, name, and default
        role.

        Parameters
        ----------
        name : str
            The name of the group to be retrieved.

        Returns
        -------
        dict[str, Any]
            A dictionary with ``GROUP_ID``, ``GROUP_NAME``, and ``defaultRole``,
            or ``{"error": "..."}`` if retrieval fails or the group is not found.
        """
        self.logger.debug(f"Starting 'get_group' method for group name: {name}")

        # Make the API call to fetch groups by name
        response = self.api_client.get(f"/api/v1/groups?name={name}")

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to retrieve groups for name '{name}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            response_data = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse group response JSON.")
            return {"ok": False, "error": f"Failed to parse group response JSON: {str(e)}"}

        if not response_data:
            self.logger.warning(f"No group found with name '{name}'")
            return {"ok": False, "error": f"No group found with name '{name}'"}

        group = response_data[0]
        group_id = group.get("_id")
        group_name = group.get("name")

        if not group_id or not group_name:
            self.logger.error(f"Incomplete group data for name '{name}'")
            return {"ok": False, "error": f"Group '{name}' found but missing expected fields"}

        self.logger.debug(f"Group '{name}' found. ID: {group_id}")
        return {"GROUP_ID": group_id, "GROUP_NAME": group_name, "defaultRole": group.get("defaultRole", "")}

    def get_groups(self, name: str | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve groups — one named group, or all of them.

        Fetches the groups defined on the Sisense server. With ``name`` the
        API filters server-side to that group; without it, every group is
        returned. One row per group.

        Parameters
        ----------
        name : str or None, optional
            Group name to filter by. Omit for all groups.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of raw group objects as returned by the API (each with
            ``_id``, ``name``, ``defaultRole``, and related fields). Without
            ``name``, an empty list means the server genuinely has no groups.
            With ``name``, the filter is an exact-match lookup — an unknown
            name returns ``{"ok": False, "error": "..."}`` naming it, never
            an empty list. Returns the standard error dict on failure.
        """
        self.logger.debug(f"Starting 'get_groups' method (name={name!r}).")

        # params= so a name with spaces or reserved characters is URL-encoded.
        response = self.api_client.get("/api/v1/groups", params={"name": name} if name is not None else None)

        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve groups", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            groups = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse groups response JSON.")
            return {"ok": False, "error": f"Failed to parse groups response JSON: {str(e)}"}

        if name is not None and not groups:
            # The name filter is an exact-match dereference (live-verified) —
            # an unknown name must fail loudly naming the reference, not
            # return [] as if the listing were genuinely empty.
            error_msg = f"Group '{name}' not found."
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}

        self.logger.debug(f"Retrieved {len(groups or [])} group(s).")
        return groups or []

    def create_groups_bulk(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]] | dict[str, Any]:
        """Create multiple groups in a single bulk request.

        Sends the provided group definitions to the bulk group creation
        endpoint.

        Parameters
        ----------
        groups : list[dict[str, Any]]
            Group definitions to create. Each dictionary should use
            canonical Sisense group fields, at minimum ``name``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The list of created group objects on success, or
            ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Starting 'create_groups_bulk' method for {len(groups)} group(s).")

        response = self.api_client.post("/api/v1/groups/bulk", data=groups)

        if response is None:
            self.logger.error("No response received while creating groups in bulk.")
            return {"ok": False, "error": "No response received while creating groups in bulk."}

        if response.status_code != 201:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text or "Unknown error"
            self.logger.error(f"Failed to create groups in bulk. Error: {error_message}")
            return {"ok": False, "error": f"Failed to create groups in bulk. {error_message}"}

        try:
            created_groups = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse bulk group creation response JSON.")
            return {"ok": False, "error": f"Failed to parse bulk group creation response JSON: {str(e)}"}

        self.logger.info(f"Successfully created {len(created_groups or [])} group(s).")
        return created_groups or []

    def delete_group(self, group_id: str) -> dict[str, Any]:
        """Delete a group by ID.

        Sends a DELETE request to remove the group from the Sisense server.

        Parameters
        ----------
        group_id : str
            The ID of the group to delete.

        Returns
        -------
        dict[str, Any]
            A success message dict if successful, or ``{"error": "..."}``
            on failure.
        """
        self.logger.debug(f"Starting 'delete_group' method for group ID: {group_id}")

        response = self.api_client.delete(f"/api/v1/groups/{group_id}")

        if response and response.status_code == 204:
            self.logger.info(f"Group (ID: {group_id}) deleted. No content returned.")
            return {"message": "Group deleted successfully."}

        if response and response.ok:
            try:
                response_data = response.json()
            except Exception:
                response_data = {"message": "Group deleted, but no JSON body returned."}
            self.logger.info(f"Group (ID: {group_id}) deleted.")
            return response_data

        failure = _extract_error_message(response, f"Failed to delete group (ID: {group_id})", self.api_client)
        self.logger.error(failure["error"])
        return failure

    def _get_groups_expanded(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Fetch every group with its members expanded, as the API returns them.

        ``GET /api/v1/groups?expand=users`` is the group-side membership source
        the Sisense UI shows; ``users_per_group`` and the canonical user rows
        both read from it. Returns the raw group list (dict entries only), or
        a failure dict.
        """
        response = self.api_client.get("/api/v1/groups", params={"expand": "users"})
        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve group memberships", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            groups = response.json() or []
        except Exception as e:
            self.logger.exception("Failed to parse group memberships response JSON.")
            return {"ok": False, "error": f"Failed to parse group memberships response JSON: {str(e)}"}

        return [group for group in groups if isinstance(group, dict)]

    def users_per_group(self, group_name: str | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve group memberships — one group's members, or every membership.

        Returns one flat row per (group, user) membership. With ``group_name``
        the rows are that group's members; without it, every membership on the
        instance is returned (ask ``get_groups`` for the per-group view).
        ``Everyone`` and ``All users in system`` are omitted from the
        all-groups view: Sisense fills both with every user, so they restate
        ``get_users_all`` rather than describing group structure. Naming
        either one still returns its members.

        The rule across the SDK: **targeted questions give complete answers;
        only the all-groups view filters.** So ``users_per_group("Everyone")``
        and ``get_user(email)["GROUPS"]`` both report every group a user is
        in, while ``users_per_group()`` omits the two universal ones. Do not
        derive one person's groups from the all-groups view — it will be
        missing those two. Use :meth:`get_user` for that.

        Membership is read from the **group** side
        (``GET /api/v1/groups?expand=users``), which is the same source the
        Sisense UI shows. Sisense resolves the auto-generated groups
        (``Admins``, ``All users in system``) on the group side only — their
        members do not appear in any user's own ``groups`` field — so reading
        from the user side would report them as empty while the UI shows
        members.

        Parameters
        ----------
        group_name : str or None, optional
            The name of the group whose members to list. Omit for all
            memberships. A name that matches no group returns
            ``{"error": "..."}`` naming it — never a silent empty list.
            Naming ``Everyone`` or ``All users in system`` returns their
            members — an explicit request is always honored, even though the
            all-groups view omits them.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            One row per (group, user) membership, each with ``GROUP_ID``,
            ``GROUP_NAME``, ``USER_ID``, ``USER_NAME``, ``EMAIL``,
            ``FIRST_NAME``, ``LAST_NAME``, ``IS_ACTIVE``, ``ROLE_ID``,
            ``ROLE_NAME`` and ``ROLE_DISPLAY_NAME`` (both the name the Sisense
            UI shows), and ``ROLE_RAW_NAME`` (the raw Sisense value). A group
            with no members contributes no rows, so the row count always equals
            the real membership count.
            Returns ``{"error": "..."}`` on failure or unknown ``group_name``.
        """
        self.logger.debug(f"Starting 'users_per_group' method (group_name={group_name!r}).")

        groups = self._get_groups_expanded()
        if isinstance(groups, dict):
            return groups

        # A filtered request for a group that doesn't exist must fail loudly —
        # an empty list would read as "the group has no members". The expanded
        # listing already holds every group, so no extra lookup is needed.
        if group_name is not None and not any(group.get("name") == group_name for group in groups):
            error_msg = f"Group '{group_name}' not found."
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}

        # The expanded group payload carries each member's identity but only a
        # raw roleId, so join against the expanded user list for the role
        # vocabularies. A user missing from that list (or a group listing a
        # stale member) still yields a row — with blank role fields rather than
        # being dropped, so counts stay honest.
        users_by_id: dict[str, dict[str, Any]] = {}
        raw_users = self._get_users_raw()
        if isinstance(raw_users, dict):
            return raw_users
        for user in raw_users:
            if isinstance(user, dict) and user.get("_id"):
                users_by_id[user["_id"]] = self._user_row(user)

        memberships: list[dict[str, Any]] = []
        for group in groups:
            gname = group.get("name", "")
            if group_name is not None:
                if gname != group_name:
                    continue
            elif gname in _UNIVERSAL_GROUPS:
                # Sisense fills these with every user, so in the all-groups
                # view they duplicate get_users_all() and would be most of the
                # output. Naming one still returns its members.
                continue
            for member in group.get("users") or []:
                if not isinstance(member, dict):
                    continue
                row = users_by_id.get(member.get("_id"), {})
                memberships.append(
                    {
                        "GROUP_ID": group.get("_id", ""),
                        "GROUP_NAME": gname,
                        "USER_ID": member.get("_id", ""),
                        "USER_NAME": member.get("userName", row.get("USER_NAME", "")),
                        "EMAIL": member.get("email", row.get("EMAIL", "")),
                        "FIRST_NAME": member.get("firstName", row.get("FIRST_NAME", "")),
                        "LAST_NAME": member.get("lastName", row.get("LAST_NAME", "")),
                        "IS_ACTIVE": member.get("active", row.get("IS_ACTIVE", False)),
                        "ROLE_ID": member.get("roleId", row.get("ROLE_ID", "")),
                        "ROLE_NAME": row.get("ROLE_NAME", ""),
                        "ROLE_DISPLAY_NAME": row.get("ROLE_DISPLAY_NAME", ""),
                        "ROLE_RAW_NAME": row.get("ROLE_RAW_NAME", ""),
                    }
                )

        self.logger.info(f"Resolved {len(memberships)} membership row(s) (group_name={group_name!r}).")
        return memberships

    @deprecated("use users_per_group")
    def users_per_group_all(self) -> list[dict[str, Any]]:
        """Retrieve all groups mapped to the users belonging to them.

        Deprecated alias kept for backward compatibility (behavior frozen) —
        prefer :meth:`users_per_group` with no argument, which returns flat
        one-row-per-membership results without the exclusions and synthetic
        groups below.

        Groups like ``Everyone`` and ``All users in system`` are excluded.
        Groups with no users are still included, with an empty user list.

        Its ``"Admins"`` entry is derived from users' **roles**
        (``sysAdmin``/``dataAdmin``/``admin``) rather than from group
        membership, and is created even when the instance has no such group.
        :meth:`users_per_group` instead reads real group-side membership for
        every group, matching the counts the Sisense UI shows.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries, where each dictionary contains a group name
            and the list of usernames in that group. An empty list is returned
            on failure.
        """
        EXCLUDED_GROUPS = {"Everyone", "All users in system"}

        self.logger.debug("Starting to retrieve all groups and their users.")

        # Step 1: Fetch all groups
        group_data = self.get_groups()
        if isinstance(group_data, dict) and "error" in group_data:
            self.logger.error("Failed to retrieve groups from API.")
            return []

        self.logger.debug(f"Retrieved {len(group_data)} groups.")

        # Step 2: Fetch all users (canonical rows)
        all_users = self.get_users_all()
        if isinstance(all_users, dict) or not all_users:
            self.logger.error("No users returned from 'get_users_all' method.")
            return []

        self.logger.debug(f"Retrieved {len(all_users)} users.")

        # Step 3: Build the initial group dictionary
        groups_dict = {group["name"]: [] for group in group_data if group["name"] not in EXCLUDED_GROUPS}
        if "Admins" not in groups_dict:
            groups_dict["Admins"] = []  # Ensure 'Admins' group exists

        self.logger.debug(f"Initialized groups dictionary with {len(groups_dict)} entries (excluding excluded groups).")

        # Step 4: Populate group membership from users (frozen output: the
        # canonical rows now include Everyone, so the old exclusion filter is
        # applied here to keep this deprecated alias's behavior unchanged).
        for user in all_users:
            for group in user.get("GROUPS", []):
                if group in EXCLUDED_GROUPS:
                    continue
                if group not in groups_dict:
                    self.logger.debug(f"Skipping user '{user.get('USER_NAME')}' group '{group}' not in current group list")
                    continue
                groups_dict[group].append(user["USER_NAME"])
                self.logger.debug(f"Added user '{user['USER_NAME']}' to group '{group}'")

        # Step 5: Add users with admin-like roles to 'Admins' (display names —
        # the vocabulary this alias always matched against).
        for user in all_users:
            if user.get("ROLE_DISPLAY_NAME") in ["sysAdmin", "dataAdmin", "admin"]:
                groups_dict["Admins"].append(user["USER_NAME"])
                self.logger.debug(f"Added user '{user['USER_NAME']}' to Admins group based on role.")

        # Step 6: Prepare final result
        result = [{"group": group_name, "username": usernames} for group_name, usernames in groups_dict.items()]

        if result:
            self.logger.info(f"Resolved {len(result)} group entries.")
        else:
            self.logger.error("No groups or users found.")

        return result
