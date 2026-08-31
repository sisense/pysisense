from __future__ import annotations

import re
from typing import Any

from typing_extensions import deprecated

from ..payloads import CreateUserPayload, UpdateUserPayload
from ..utils import _extract_error_message

# Raw Sisense role name -> the display name shown in the Sisense UI.
# Single source of truth: canonical user rows carry BOTH vocabularies
# (ROLE_NAME/ROLE_DISPLAY_NAME aliased, ROLE_RAW_NAME raw) so no consumer has
# to guess which vocabulary a field contains.
_ROLE_DISPLAY_ALIASES = {
    "consumer": "viewer",
    "super": "sysAdmin",
    "contributor": "dashboardDesigner",
}


def _normalize_role_key(value: Any) -> str:
    """Collapse a role name to a comparison key: uppercase, alphanumerics only.

    Lets ``"sys admin"``, ``"Sys-Admin"`` and ``" sysAdmin "`` all compare equal
    so callers (and the humans typing at them) are not held to exact casing or
    spacing.
    """
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


# Display/human role name -> raw Sisense role name, both normalized. Consulted
# ONLY after the instance's real roles fail to match, so a real role always
# wins over an alias. The three canonical entries are derived from
# _ROLE_DISPLAY_ALIASES (single source of truth); the rest are human phrasings.
#
# Deliberately absent: "ADMIN"/"ADMINISTRATOR" (a Sisense instance can have a
# real `admin` role distinct from `super`, so guessing would silently
# over-privilege) and "DATADESIGNER" (`dataDesigner` is its own role, NOT a
# synonym for `contributor`). Both resolve via the real-role lookup, or fail
# loudly listing the available roles.
_ROLE_WRITE_ALIASES = {
    **{_normalize_role_key(display): _normalize_role_key(raw) for raw, display in _ROLE_DISPLAY_ALIASES.items()},
    _normalize_role_key("systemAdmin"): _normalize_role_key("super"),
    _normalize_role_key("systemAdministrator"): _normalize_role_key("super"),
    _normalize_role_key("designer"): _normalize_role_key("contributor"),
}


class UsersMixin:
    def _fetch_expanded_users(self) -> Any:
        """Fetch the users list with ``groups`` and ``role`` expanded; returns the raw response."""
        return self.api_client.get("/api/v1/users", params={"expand": "groups,role"})

    def _get_users_raw(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Fetch and parse the expanded users list, returned exactly as the API provides it.

        Internal fidelity layer: raw field names (``_id``, ``userName``), raw
        role names, full group objects, nothing filtered. Used by the
        canonical readers and by cross-environment migration flows that need
        unmodified identifiers. Returns the raw list, or a failure dict.
        """
        response = self._fetch_expanded_users()

        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve users", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            users = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse users response JSON.")
            return {"ok": False, "error": f"Failed to parse users response JSON: {str(e)}"}

        self.logger.debug(f"Retrieved {len(users or [])} user(s).")
        return users or []

    def _resolve_role_id(self, role_name: Any) -> str | dict[str, Any]:
        """Resolve a role name to its Sisense role ID, accepting either vocabulary.

        Matching is case-, space- and punctuation-insensitive, and happens in a
        deliberate order: the instance's **real** roles first, aliases second.
        That ordering is a safety property — an instance may define ``admin``,
        ``dataAdmin``, ``dataDesigner``, ``tenantAdmin`` or ``custom_*`` roles,
        and each must resolve to itself rather than to a same-sounding alias.

        Parameters
        ----------
        role_name : Any
            Role name in either vocabulary — raw (``"super"``), UI display
            (``"sysAdmin"``), or a human phrasing (``"system admin"``).

        Returns
        -------
        str | dict[str, Any]
            The resolved role ID, or the standard
            ``{"ok": False, "error": "...", ...}`` dict when the roles cannot be
            fetched or the name matches nothing (the error names the roles the
            instance actually has).
        """
        response = self.api_client.get("/api/roles")
        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve roles", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            roles = response.json() or []
        except Exception as e:
            self.logger.exception("Failed to parse roles response JSON.")
            return {"ok": False, "error": f"Failed to parse roles response JSON: {str(e)}"}

        # Build the lookup from what this instance actually defines. displayName
        # is opportunistic — not every Sisense version returns it.
        by_key: dict[str, str] = {}
        available: list[str] = []
        for role in roles:
            if not isinstance(role, dict) or not role.get("_id"):
                continue
            raw_name = role.get("name")
            if raw_name:
                by_key.setdefault(_normalize_role_key(raw_name), role["_id"])
                available.append(str(raw_name))
            display = role.get("displayName")
            if display:
                by_key.setdefault(_normalize_role_key(display), role["_id"])

        requested = _normalize_role_key(role_name)

        # 1) A real role always wins.
        role_id = by_key.get(requested)

        # 2) Only then fall back to the display/human alias table.
        if role_id is None:
            aliased = _ROLE_WRITE_ALIASES.get(requested)
            if aliased:
                role_id = by_key.get(aliased)

        if role_id is None:
            error_msg = f"Role '{role_name}' not found. Available roles: {', '.join(sorted(available)) or 'none'}."
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}

        self.logger.debug(f"Resolved role '{role_name}' to ID '{role_id}'.")
        return role_id

    def _user_row(self, user: dict[str, Any]) -> dict[str, Any]:
        """Build the canonical user row from one raw expanded user object.

        Canonical row shape (shared by ``get_user`` and ``get_users_all``):
        ``USER_ID``, ``USER_NAME``, ``EMAIL``, ``FIRST_NAME``, ``LAST_NAME``,
        ``IS_ACTIVE``, ``ROLE_ID``, ``ROLE_NAME`` and ``ROLE_DISPLAY_NAME``
        (both the name the Sisense UI shows), ``ROLE_RAW_NAME`` (the raw
        Sisense value), ``GROUP_IDS``, ``GROUPS`` (group names, unfiltered —
        includes ``Everyone``).
        """
        role_obj = user.get("role") or {}
        role_name_raw = role_obj.get("name") or ""
        groups_obj = [g for g in (user.get("groups") or []) if isinstance(g, dict)]
        return {
            "USER_ID": user.get("_id", ""),
            "USER_NAME": user.get("userName", ""),
            "EMAIL": user.get("email", ""),
            "FIRST_NAME": user.get("firstName", ""),
            "LAST_NAME": user.get("lastName", ""),
            "IS_ACTIVE": user.get("active", False),
            "ROLE_ID": role_obj.get("_id", ""),
            # ROLE_NAME keeps its 1.x meaning (the name the UI shows) so that
            # role comparisons written against 1.x keep working — that break
            # would have been silent. ROLE_DISPLAY_NAME says the same thing
            # unambiguously; ROLE_RAW_NAME carries Sisense's own value.
            "ROLE_NAME": _ROLE_DISPLAY_ALIASES.get(role_name_raw, role_name_raw),
            "ROLE_DISPLAY_NAME": _ROLE_DISPLAY_ALIASES.get(role_name_raw, role_name_raw),
            "ROLE_RAW_NAME": role_name_raw,
            "GROUP_IDS": [g.get("_id", "") for g in groups_obj],
            "GROUPS": [g.get("name", "") for g in groups_obj],
        }

    def _map_user_role_and_groups(self, user: dict[str, Any], apply_role_alias: bool = True) -> tuple[str | None, str | None, list[str], list[str]]:
        """Resolve a single expanded user's role/group IDs and names.

        Parameters
        ----------
        user : dict[str, Any]
            An expanded user object (``role`` and ``groups`` as objects, not
            raw IDs).
        apply_role_alias : bool, optional
            When ``True`` (default), the raw Sisense role name (e.g.
            ``"consumer"``) is mapped to its public alias (``"viewer"``).
            When ``False``, the raw role name is returned unchanged.

        Returns ``(role_id, role_name, group_ids, group_names)``.
        """
        role_mapping = _ROLE_DISPLAY_ALIASES

        role_obj = user.get("role") or {}
        groups_obj = user.get("groups") or []

        role_id = role_obj.get("_id")
        role_name_raw = role_obj.get("name")
        role_name = role_mapping.get(role_name_raw, role_name_raw) if apply_role_alias else role_name_raw

        group_ids = []
        group_names = []
        for g in groups_obj:
            if not isinstance(g, dict):
                continue
            gid = g.get("_id")
            gname = g.get("name")
            if gid:
                group_ids.append(gid)
            if gname:
                group_names.append(gname)

        return role_id, role_name, group_ids, group_names

    @deprecated("use get_user")
    def get_user_with_role_and_group_names(self, user_name: str) -> dict[str, Any]:
        """Retrieve a single user by email/username with role and group details.

        Deprecated alias kept for backward compatibility (behavior frozen) —
        prefer :meth:`get_user`, which returns the canonical user row.

        Fetches the expanded users list and returns the matching user enriched
        with both the role and group IDs and their resolved names.

        Parameters
        ----------
        user_name : str
            The email or username of the user to be retrieved. (format: email)

        Returns
        -------
        dict[str, Any]
            User details including ``USER_ID``, ``USER_NAME``, ``FIRST_NAME``,
            ``LAST_NAME``, ``EMAIL``, ``IS_ACTIVE``, ``ROLE_ID``, ``ROLE_NAME``,
            ``GROUP_IDS`` (list of group IDs), and ``GROUP_NAMES`` (list of group
            names), or ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Getting user with role and group IDs/names for: {user_name}")

        # Reuse expanded users endpoint to get role & group objects
        response = self._fetch_expanded_users()

        if response is None or not response.ok:
            failure = _extract_error_message(response, f"Failed to retrieve users from API for username: {user_name}", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            users = response.json()
        except Exception as exc:
            self.logger.exception("Error decoding JSON response for user list in get_user_with_role_and_group_names.")
            return {"ok": False, "error": f"Failed to decode API response: {exc}"}

        for user in users:
            try:
                if user.get("email") != user_name:
                    continue

                role_id, role_name, group_ids, group_names = self._map_user_role_and_groups(user)

                result = {
                    "USER_ID": user.get("_id"),
                    "USER_NAME": user.get("userName"),
                    "FIRST_NAME": user.get("firstName"),
                    "LAST_NAME": user.get("lastName", ""),
                    "EMAIL": user.get("email"),
                    "IS_ACTIVE": user.get("active"),
                    "ROLE_ID": role_id,
                    "ROLE_NAME": role_name,
                    "GROUP_IDS": group_ids,
                    "GROUP_NAMES": group_names,
                }

                self.logger.info(f"Found user '{user_name}' with role and group IDs/names.")
                return result

            except Exception as exc:
                self.logger.exception(f"Error processing user object in get_user_with_role_and_group_names: {exc}")

        self.logger.warning(f"User with username '{user_name}' not found in get_user_with_role_and_group_names.")
        return {"ok": False, "error": f"User '{user_name}' not found."}

    @deprecated("use get_users_all")
    def get_users_with_role_names_and_group_names(self) -> list[dict[str, Any]]:
        """Retrieve all users enriched with role names and group names.

        Deprecated alias kept for backward compatibility (behavior frozen) —
        prefer :meth:`get_users_all`, which returns the canonical user rows.

        Fetches users with ``groups`` and ``role`` expanded in a single call
        and resolves each user's role/group IDs and names from the expanded
        objects. Role names are returned exactly as stored by Sisense (e.g.
        ``"consumer"``), not the public alias — use
        ``get_user_with_role_and_group_names`` for a single user if the
        aliased name (``"viewer"``) is needed instead.

        Returns
        -------
        list[dict[str, Any]]
            A list where each entry contains ``USER_ID``, ``USER_NAME``,
            ``FIRST_NAME``, ``LAST_NAME``, ``EMAIL``, ``IS_ACTIVE``, ``ROLE_ID``,
            ``ROLE_NAME``, ``GROUP_IDS``, and ``GROUP_NAMES``. If any API call
            fails, a single-item list with an ``error`` key is returned.
        """
        self.logger.debug("Fetching users with expanded role/group objects to enrich with names.")

        response = self._fetch_expanded_users()
        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve users from API", self.api_client)
            self.logger.error(failure["error"])
            return [failure]

        try:
            users_raw = response.json()
        except Exception as exc:
            self.logger.exception("Failed to parse users response JSON.")
            return [{"ok": False, "error": f"Failed to parse users response JSON: {exc}"}]

        enriched_users: list[dict[str, Any]] = []

        for user in users_raw:
            if not isinstance(user, dict):
                self.logger.warning(f"Skipping unexpected user entry (not a dict): {user}")
                continue

            role_id, role_name, group_ids, group_names = self._map_user_role_and_groups(user, apply_role_alias=False)

            enriched_users.append(
                {
                    "USER_ID": user.get("_id"),
                    "USER_NAME": user.get("userName"),
                    "FIRST_NAME": user.get("firstName"),
                    "LAST_NAME": user.get("lastName", ""),
                    "EMAIL": user.get("email"),
                    "IS_ACTIVE": user.get("active"),
                    "ROLE_ID": role_id,
                    "ROLE_NAME": role_name,
                    "GROUP_IDS": group_ids,
                    "GROUP_NAMES": group_names,
                }
            )

        self.logger.info(f"Resolved users with role and group names. Total users processed: {len(enriched_users)}")
        return enriched_users

    @deprecated("use get_users_all")
    def get_users_expanded(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all users with raw, unmodified role and group objects.

        Deprecated alias kept for backward compatibility (behavior frozen) —
        prefer :meth:`get_users_all`: its canonical rows carry the raw
        ``ROLE_NAME``, ``GROUP_IDS``, and unfiltered ``GROUPS`` that
        previously required this method.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The raw list of user objects, or ``{"error": "..."}`` if
            retrieval fails.
        """
        self.logger.debug("Starting 'get_users_expanded' method.")
        return self._get_users_raw()

    def create_users_bulk(self, users: list[dict[str, Any]]) -> list[dict[str, Any]] | dict[str, Any]:
        """Create multiple users in a single bulk request.

        Sends the provided user definitions to the bulk user creation
        endpoint. Each entry must already carry a resolved ``roleId`` and
        ``groups`` (list of group IDs) — no name-to-ID resolution is
        performed by this method.

        Parameters
        ----------
        users : list[dict[str, Any]]
            User definitions to create. Each dictionary should use canonical
            Sisense user fields (at minimum ``email``, ``firstName``, and
            ``roleId``).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The list of created user objects on success, or
            ``{"error": "..."}`` on failure.
        """
        self.logger.debug(f"Starting 'create_users_bulk' method for {len(users)} user(s).")

        response = self.api_client.post("/api/v1/users/bulk", data=users)

        if response is None:
            self.logger.error("No response received while creating users in bulk.")
            return {"ok": False, "error": "No response received while creating users in bulk."}

        if response.status_code != 201:
            try:
                error_message = response.json()
            except Exception:
                error_message = response.text or "Unknown error"
            self.logger.error(f"Failed to create users in bulk. Error: {error_message}")
            return {"ok": False, "error": f"Failed to create users in bulk. {error_message}"}

        try:
            created_users = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse bulk user creation response JSON.")
            return {"ok": False, "error": f"Failed to parse bulk user creation response JSON: {str(e)}"}

        self.logger.info(f"Successfully created {len(created_users or [])} user(s).")
        return created_users or []

    def get_user(self, user_email: str) -> dict[str, Any]:
        """
        Retrieve one named user by email address, in the canonical user row shape.

        Fetches users with expanded ``groups`` and ``role`` data and returns the
        record matching the provided email address.

        Changed in 2.0: ``ROLE_NAME`` held the display name in 1.x (that value
        is now ``ROLE_DISPLAY_NAME``); ``GROUPS`` still holds the group names
        and is joined by the new ``GROUP_IDS``. See ``docs/migration-2.0.md``.

        Parameters
        ----------
        user_email : str
            Email address of the user to retrieve. **Required** — this method
            always answers "one named user"; use ``get_users_all`` for every
            user. (format: email)

        Returns
        -------
        dict[str, Any]
            The canonical user row: ``USER_ID``, ``USER_NAME``, ``EMAIL``,
            ``FIRST_NAME``, ``LAST_NAME``, ``IS_ACTIVE``, ``ROLE_ID``,
            ``ROLE_NAME`` (the raw Sisense value, e.g. ``"consumer"``),
            ``ROLE_DISPLAY_NAME`` (the name the Sisense UI shows, e.g.
            ``"viewer"``), ``GROUP_IDS``, and ``GROUPS`` (unfiltered —
            includes ``Everyone``). Returns ``{"error": "..."}`` when the user
            is not found or the API call fails.
        """
        self.logger.debug("Getting user with email: %s", user_email)

        users = self._get_users_raw()
        if isinstance(users, dict):
            return users

        for user in users:
            try:
                if user.get("email") == user_email:
                    self.logger.info("Found user: %s", user_email)
                    return self._user_row(user)
            except Exception as exc:
                self.logger.exception(
                    "Error processing user object for email %s. Exception: %s",
                    user_email,
                    str(exc),
                )

        self.logger.warning("User with email '%s' not found.", user_email)
        return {"ok": False, "error": f"User '{user_email}' not found."}

    def get_my_user(self) -> dict[str, Any]:
        """Retrieve the currently logged-in user for the API token.

        Sends ``GET /api/users/loggedin``. Use this to resolve migration user
        identity (email, username, internal ID) for the authenticated admin
        token.

        Returns
        -------
        dict[str, Any]
            The logged-in user object from the API, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/users/loggedin"
        self.logger.debug("Fetching logged-in user identity.")
        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to retrieve logged-in user", self.api_client)
            self.logger.error(failure["error"])
            return failure

        user = response.json()
        self.logger.info("Successfully retrieved logged-in user identity.")
        return user

    def get_roles(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all Sisense roles.

        Sends ``GET /api/roles``. Returns the raw role list used to build role
        name-to-ID maps (for example in multi-tenant migration workflows).

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of role objects on success, or ``{"error": "..."}`` on
            failure.
        """
        endpoint = "/api/roles"
        self.logger.debug("Fetching roles from API.")
        response = self.api_client.get(endpoint)

        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve roles", self.api_client)
            self.logger.error(failure["error"])
            return failure

        roles = response.json()
        count = len(roles) if isinstance(roles, list) else 0
        self.logger.info(f"Successfully retrieved {count} roles.")
        return roles

    def change_user_password(self, user_id: str, password: str) -> dict[str, Any]:
        """Change a user's password.

        Sends ``PATCH /api/users/{user_id}`` with only ``password`` in the
        request body. Other user fields are not modified.

        Parameters
        ----------
        user_id : str
            Internal user ID (``_id``) to update.
        password : str
            New password for the user. Must not be empty.

        Returns
        -------
        dict[str, Any]
            The updated user object on success, or ``{"error": "..."}`` on
            failure.
        """
        if not password:
            self.logger.error("Password change rejected: password must not be empty.")
            return {"ok": False, "error": "Password must not be empty."}

        endpoint = f"/api/users/{user_id}"
        self.logger.debug(f"Changing password for user ID {user_id}")
        response = self.api_client.patch(endpoint, data={"password": password})

        if response is None:
            self.logger.error(f"PATCH request to change password for user {user_id} failed: No response received.")
            return {"ok": False, "error": f"No response received while changing password for user ID '{user_id}'"}

        if not response.ok:
            try:
                error_message = response.json().get("error", "Unknown error")
            except Exception:
                error_message = "Unknown error"
            self.logger.error(f"Failed to change password for user {user_id}. Error: {error_message}")
            return {"ok": False, "error": error_message}

        try:
            response_data = response.json()
        except Exception:
            response_data = {"success": True}

        self.logger.info(f"Successfully changed password for user ID {user_id}.")
        return response_data

    def get_users_all(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve every user, one canonical user row each.

        Reports exactly what Sisense stores: group memberships are unfiltered
        (``Everyone`` is included — consumers that want to hide a universal
        group can drop it; a consumer that never received it cannot put it
        back), and ``ROLE_NAME`` carries the raw Sisense value with the
        UI-facing name in ``ROLE_DISPLAY_NAME``.

        Changed in 2.0: ``ROLE_NAME`` held the display name in 1.x (that value
        is now ``ROLE_DISPLAY_NAME``), ``GROUPS`` is joined by the new
        ``GROUP_IDS``, and ``Everyone`` is no longer filtered out of
        ``GROUPS``. See ``docs/migration-2.0.md``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            One row per user, each with ``USER_ID``, ``USER_NAME``, ``EMAIL``,
            ``FIRST_NAME``, ``LAST_NAME``, ``IS_ACTIVE``, ``ROLE_ID``,
            ``ROLE_NAME`` (raw, e.g. ``"consumer"``), ``ROLE_DISPLAY_NAME``
            (UI name, e.g. ``"viewer"``), ``GROUP_IDS``, and ``GROUPS``
            (unfiltered). Returns ``{"error": "..."}`` on failure.
        """
        self.logger.debug("Getting all users")

        users = self._get_users_raw()
        if isinstance(users, dict):
            return users

        data_list = []
        for user in users:
            try:
                data_list.append(self._user_row(user))
            except Exception as e:
                self.logger.exception(f"Error processing user {user.get('email', 'Unknown')}: {str(e)}")

        # An instance with zero users is an empty (honest) result, not an error.
        self.logger.info(f"Found {len(data_list)} users")
        return data_list

    def create_user(self, user_data: CreateUserPayload) -> dict[str, Any]:
        """Create a new user in Sisense.

        Validates that the required fields are present, then resolves the role
        name and group names to their corresponding IDs and sends a POST
        request to create the user. Group names in ``groups`` are resolved to
        group IDs.

        The ``role`` field accepts either vocabulary — the raw Sisense name
        (``"consumer"``, ``"super"``, ``"contributor"``) or the name the UI
        shows (``"viewer"``, ``"sysAdmin"``, ``"dashboardDesigner"``) — and is
        matched ignoring case, spaces and punctuation, so ``"sys admin"`` and
        ``"sysAdmin"`` are equivalent. Roles the instance defines beyond these
        (for example ``dataDesigner``, ``dataAdmin``, ``admin`` or custom
        roles) are matched by their own name and are never treated as synonyms
        for a similar-sounding role. An unmatched name returns a failure dict
        listing the roles the instance actually has.

        Parameters
        ----------
        user_data : CreateUserPayload
            User details, using canonical Sisense payload field names:

            - ``email`` : str — the user's email address. **Required.**
            - ``role`` : str — role name to assign, in either vocabulary
              (``"viewer"`` or ``"consumer"``, ``"sysAdmin"`` or ``"super"``,
              ``"dashboardDesigner"``/``"designer"`` or ``"contributor"``, or
              any other role the instance defines); resolved to ``roleId``.
              **Required.**
            - ``userName`` : str — the user's login name (optional).
            - ``firstName`` : str — the user's first name (optional).
            - ``lastName`` : str — the user's last name (optional).
            - ``groups`` : list[str] — group names to assign, resolved to IDs
              (optional).
            - ``password`` : str — initial password; if omitted, the user
              receives an email to set one (optional).
            - ``preferences`` : dict — user preference settings (optional).

        Returns
        -------
        dict[str, Any]
            The created user object returned by the API if successful, or a
            dictionary with an ``error`` key if the operation fails. Missing
            required fields are rejected up front, before any API call.
        """
        self.logger.debug(f"Creating user with data: {user_data}")

        # Validate required fields up front — fail with a clear message before
        # any API call instead of failing mid-flow at role resolution.
        if not isinstance(user_data, dict):
            self.logger.error("create_user requires user_data to be a dict.")
            return {"ok": False, "error": "user_data must be a dictionary."}
        missing = [field for field in ("email", "role") if not user_data.get(field)]
        if missing:
            error_msg = f"create_user requires {' and '.join(f'{f!r}' for f in missing)} in user_data — got fields: {sorted(user_data.keys()) or 'none'}"
            self.logger.error(error_msg)
            return {"ok": False, "error": error_msg}

        # Step 1: Resolve roleId from the role name (either vocabulary)
        resolved_role = self._resolve_role_id(user_data.get("role"))
        if isinstance(resolved_role, dict):
            return resolved_role

        user_data["roleId"] = resolved_role
        user_data.pop("role", None)

        # Step 3: Resolve group IDs from group names (if provided)
        group_names = user_data.get("groups", [])
        if group_names:
            user_data["groups"] = [group.upper() for group in group_names]

            group_response = self.api_client.get("/api/v1/groups")
            if not group_response or not group_response.ok:
                self.logger.error("Failed to fetch groups from API")
                return {"ok": False, "error": "Failed to fetch groups from API"}

            groups_mapping = [{"id": group["_id"], "name": group["name"].upper()} for group in group_response.json()]
            self.logger.debug(f"Groups mapping: {groups_mapping}")

            updated_groups = []
            for group_name in user_data["groups"]:
                for group in groups_mapping:
                    if group["name"] == group_name:
                        updated_groups.append(group["id"])
                        break
                else:
                    error_msg = f"Group '{group_name}' not found in groups_mapping"
                    self.logger.error(error_msg)
                    return {"ok": False, "error": error_msg}

            user_data["groups"] = updated_groups
        else:
            user_data["groups"] = []

        # Step 4: Send POST request to create the user
        self.logger.debug(f"Final user data for API call: {user_data}")
        response = self.api_client.post("/api/v1/users", data=user_data)

        if response and response.ok:
            response_data = response.json()
            self.logger.info(f"User created successfully: {response_data}")
            return response_data
        else:
            try:
                error_json = response.json()
                error_message = error_json["error"].get("message", str(error_json["error"])) if isinstance(error_json, dict) and "error" in error_json else error_json.get("error", str(error_json))
            except Exception:
                error_message = "No response body or invalid JSON"

            self.logger.error(f"Failed to create user. Error: {error_message}")
            return {"ok": False, "error": error_message}

    def update_user(self, user_email: str, user_data: UpdateUserPayload) -> dict[str, Any]:
        """
        Update an existing Sisense user identified by their email address.

        This method finds the user by email and performs a partial update (PATCH).
        All update fields MUST be provided inside the ``user_data`` dictionary. Do not
        pass update fields at the top level.

        Parameters
        ----------
        user_email : str
            Email address of the user to update (used to locate the user). (format: email)
        user_data : UpdateUserPayload
            Dictionary of fields to update. Only include fields you want to change.

            Supported fields
            ----------------
            - email : str
                Update the user's email address.
            - userName : str
                Update the user's username/login name.
            - firstName : str
                Update the user's first name.
            - lastName : str
                Update the user's last name.
            - role : str
                Role name in either vocabulary — raw (``"consumer"``, ``"super"``,
                ``"contributor"``), the UI name (``"viewer"``, ``"sysAdmin"``,
                ``"dashboardDesigner"``), or any other role the instance defines
                (``dataDesigner``, ``dataAdmin``, custom roles). Matched ignoring
                case, spaces and punctuation. This is resolved to ``roleId`` before
                sending the API request.
            - groups : list[str]
                List of group names to apply. Group names are resolved to group IDs before
                sending the API request. If ``groups`` is explicitly provided as an empty
                list (``[]``), group memberships are cleared (tenant defaults may still apply).

        Returns
        -------
        dict[str, Any]
            The updated user payload when successful. If the operation fails, returns a
            dictionary with an ``error`` key.
        """
        self.logger.debug("Updating user with email: %s", user_email)

        user = self.get_user(user_email)
        if not user:
            self.logger.error("User with email '%s' not found.", user_email)
            return {"ok": False, "error": f"User with email '{user_email}' not found."}

        # Step 1: Resolve role if provided (either vocabulary)
        if "role" in user_data:
            resolved_role = self._resolve_role_id(user_data["role"])
            if isinstance(resolved_role, dict):
                return resolved_role

            user_data["roleId"] = resolved_role
            user_data.pop("role", None)

        # Step 2: Resolve groups only if explicitly provided
        if "groups" in user_data:
            group_names = user_data.get("groups") or []

            # If caller explicitly passed an empty list, they intend to clear groups
            if not group_names:
                user_data["groups"] = []
            else:
                normalized_group_names = [str(g).upper() for g in group_names]

                group_response = self.api_client.get("/api/v1/groups")
                if not group_response or not group_response.ok:
                    status = group_response.status_code if group_response else "No response"
                    self.logger.error(
                        "Failed to fetch groups from API. Status Code: %s",
                        status,
                    )
                    return {"ok": False, "error": "Failed to fetch groups from API."}

                groups_mapping = [{"id": group["_id"], "name": str(group["name"]).upper()} for group in group_response.json()]
                self.logger.debug("Groups mapping: %s", groups_mapping)

                updated_groups = []
                for group_name in normalized_group_names:
                    for group in groups_mapping:
                        if group["name"] == group_name:
                            updated_groups.append(group["id"])
                            break
                    else:
                        error_msg = f"Group '{group_name}' not found in groups_mapping"
                        self.logger.error(error_msg)
                        return {"ok": False, "error": error_msg}

                user_data["groups"] = updated_groups

        self.logger.debug("Final updated user data for API call: %s", user_data)
        response = self.api_client.patch(
            f"/api/v1/users/{user['USER_ID']}",
            data=user_data,
        )

        if response and response.ok:
            response_data = response.json()
            self.logger.info("User updated successfully: %s", response_data)
            return response_data

        failure = _extract_error_message(response, "Failed to update user", self.api_client)
        self.logger.error(failure["error"])
        return failure

    def delete_user(self, user_name: str) -> dict[str, Any]:
        """Delete a user by their email (username).

        Resolves the user by email/username and sends a DELETE request to remove
        the account.

        Parameters
        ----------
        user_name : str
            The email or username of the user to be deleted. (format: email)

        Returns
        -------
        dict[str, Any]
            A success message dict from the API if successful, or an
            ``{"error": "..."}`` dict on failure.
        """
        self.logger.debug(f"Starting 'delete_user' method for username: {user_name}")

        # Reuse the get_user method to fetch user details
        self.logger.debug(f"Fetching user details for '{user_name}' using 'get_user' method.")
        user = self.get_user(user_name)
        self.logger.debug(f"User details fetched: {user}")

        # If user is not found, log and return error
        if not user or "error" in user:
            error_msg = f"User '{user_name}' not found. Cannot proceed with deletion."
            self.logger.error(error_msg)
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return {"ok": False, "error": error_msg}
        # support both formats just in case
        user_id = user.get("_id") or user.get("USER_ID")
        if not user_id:
            self.logger.error(f"User object for '{user_name}' is missing ID field. Cannot proceed.")
            return {"ok": False, "error": (f"User '{user_name}' found but no ID field present.")}

        self.logger.debug(f"User '{user_name}' found. Proceeding to delete user with ID: {user_id}")

        # Send the DELETE request
        response = self.api_client.delete(f"/api/v1/users/{user['USER_ID']}")

        if response and response.status_code == 204:
            self.logger.info(f"User '{user_name}' (ID: {user['USER_ID']}) deleted. No content returned.")
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return {"message": "User deleted successfully."}

        elif response and response.ok:
            try:
                response_data = response.json()
            except Exception:
                response_data = {"message": "User deleted, but no JSON body returned."}
            self.logger.info(f"User '{user_name}' (ID: {user['USER_ID']}) deleted.")
            self.logger.debug(f"API response: {response_data}")
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return response_data

        else:
            try:
                error_message = response.json().get("error", "Unknown error")
            except Exception:
                error_message = "No response body or invalid JSON"
            self.logger.error(f"Failed to delete user '{user_name}' (ID: {user['USER_ID']}). Error: {error_message}")
            self.logger.debug(f"Completed 'delete_user' method for username: {user_name}")
            return {"ok": False, "error": error_message}
