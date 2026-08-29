from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from ..access_management import AccessManagement

_EXCLUDED_GROUP_NAMES = frozenset({"Everyone", "All users in system"})
_CUSTOM_ROLE_PREFIX = "custom_"


def _empty_summary(ok: bool = False, status: str = "failed") -> dict[str, Any]:
    return {
        "ok": ok,
        "status": status,
        "succeeded": [],
        "skipped": [],
        "failed": [],
        "source_count": 0,
        "succeeded_count": 0,
        "skipped_count": 0,
        "failed_count": 0,
    }


def _normalize_role_name(
    role_name: str,
    *,
    ignore_custom_roles: bool,
    is_target_multitenant: bool,
    target_operating_system: str,
) -> str:
    """Apply the same source-role-name transformations as the legacy merge tool."""
    name = role_name

    if ignore_custom_roles and name and name.startswith(_CUSTOM_ROLE_PREFIX):
        name = name[len(_CUSTOM_ROLE_PREFIX) :]

    # Multi-tenant targets replace the top-level admin roles with the tenant-scoped role.
    if is_target_multitenant and name in ("super", "admin"):
        name = "tenantAdmin"

    # Windows targets do not have a "tenantAdmin" role; it maps back to "admin".
    if target_operating_system == "windows" and name == "tenantAdmin":
        name = "admin"

    return name


def _build_role_id_map(
    source_roles: list[dict[str, Any]],
    target_roles: list[dict[str, Any]],
    *,
    ignore_custom_roles: bool,
    target_operating_system: str,
) -> tuple[dict[str, str], set[str]]:
    """Resolve source role IDs to target role IDs.

    Returns ``(role_id_map, unmapped_role_names)`` where ``role_id_map`` maps
    a source role's ``_id`` to the matching target role's ``_id``, and
    ``unmapped_role_names`` lists source role names with no target match.
    """
    target_by_name: dict[str, str] = {r["name"]: r["_id"] for r in target_roles if r.get("name") and r.get("_id")}
    is_target_multitenant = "tenantAdmin" in target_by_name

    role_id_map: dict[str, str] = {}
    unmapped_role_names: set[str] = set()

    for role in source_roles:
        src_id = role.get("_id")
        src_name = role.get("name")
        if not src_id or not src_name:
            continue

        normalized = _normalize_role_name(
            src_name,
            ignore_custom_roles=ignore_custom_roles,
            is_target_multitenant=is_target_multitenant,
            target_operating_system=target_operating_system,
        )

        target_id = target_by_name.get(normalized)

        if target_id is None and ignore_custom_roles:
            for tname, tid in target_by_name.items():
                if tname.startswith(_CUSTOM_ROLE_PREFIX) and tname[len(_CUSTOM_ROLE_PREFIX) :] == normalized:
                    target_id = tid
                    break

        if target_id is not None:
            role_id_map[src_id] = target_id
        else:
            unmapped_role_names.add(src_name)

    return role_id_map, unmapped_role_names


def _build_user_payload(
    user: dict[str, Any],
    role_id: str,
    group_name_to_id: dict[str, str],
) -> dict[str, Any]:
    group_ids: list[str] = []
    for group in user.get("groups") or []:
        if not isinstance(group, dict):
            continue
        name = group.get("name")
        if not name or name in _EXCLUDED_GROUP_NAMES:
            continue
        target_group_id = group_name_to_id.get(name)
        if target_group_id:
            group_ids.append(target_group_id)

    return {
        "email": user.get("email"),
        "userName": user.get("userName"),
        "firstName": user.get("firstName"),
        "lastName": user.get("lastName") or " ",
        "roleId": role_id,
        "groups": group_ids,
        "preferences": user.get("preferences") or {"localeId": "en-US"},
    }


class UsersMergeMixin:
    def migrate_users(
        self,
        user_emails: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        ignore_custom_roles: bool = False,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific users from source to target.

        Fetches the requested users from the source environment, resolves
        their role and group assignments to target environment IDs, and
        creates (or replaces) them on the target via the bulk user endpoint.
        Conflict detection is based on the user's ``email`` field.

        Role resolution mirrors the legacy Win2Linux merge tool: when the
        target environment is multi-tenant (its role list includes
        ``tenantAdmin``), source ``super``/``admin`` roles are mapped to
        ``tenantAdmin``; when the target is a Windows deployment, source
        ``tenantAdmin`` is mapped back to ``admin``.

        Parameters
        ----------
        user_emails : list[str] or None, default None
            Email addresses of the users to migrate. If ``None``, every user
            on the source is migrated. (format: email)
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a user with the same ``email`` already
            exists on the target.

            - ``"skip"`` — leave the existing user unchanged.
            - ``"overwrite"`` — delete the existing user then recreate from
              source.
            - ``"duplicate"`` — always create, regardless of conflicts.
        ignore_custom_roles : bool, default False
            When ``True``, strips a ``custom_`` prefix from source role names
            before matching them against target roles (and matches target
            roles with the same prefix stripped).
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``email``
            - ``skipped`` : list[dict] — each has ``email`` and ``reason``
            - ``failed`` : list[dict] — each has ``email`` and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting user migration from source to target."})
        self.logger.info("Starting user migration from source to target.")

        summary = _empty_summary()

        src_access = AccessManagement(api_client=self.source_client)
        tgt_access = AccessManagement(api_client=self.target_client)

        # Step 1: Fetch all source users
        self._emit(emit, {"type": "progress", "step": "fetch_source_users", "message": "Fetching users from the source environment."})
        self.logger.debug("Fetching users from source.")
        source_result = src_access.get_users_expanded()
        if isinstance(source_result, dict) and "error" in source_result:
            raw_error = source_result["error"]
            self.logger.error("Failed to fetch users from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_users", "message": "Failed to fetch users from source.", "raw_error": raw_error})
            return summary

        all_source_users: list[dict[str, Any]] = source_result
        self.logger.debug("Found %s user(s) on source.", len(all_source_users))

        # Step 2: Filter to the requested emails
        if user_emails is not None:
            wanted: set[str] = set(user_emails)
            users_to_migrate = [u for u in all_source_users if u.get("email") in wanted]
            for missing_email in wanted - {u.get("email") for u in users_to_migrate}:
                self.logger.warning("User '%s' not found on source.", missing_email)
                summary["failed"].append({"email": missing_email, "reason": "Not found on source."})
        else:
            users_to_migrate = all_source_users

        summary["source_count"] = len(users_to_migrate)

        if not users_to_migrate:
            self.logger.info("No matching users found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No users to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_users", "message": "Fetched source users.", "count": len(users_to_migrate)})

        # Step 3: Fetch target users, roles, and groups for conflict detection and ID mapping
        self._emit(emit, {"type": "progress", "step": "fetch_target_mappings", "message": "Fetching users, roles, and groups from the target environment."})
        target_result = tgt_access.get_users_expanded()
        target_users: list[dict[str, Any]] = [] if isinstance(target_result, dict) and "error" in target_result else target_result
        target_by_email: dict[str, dict[str, Any]] = {u["email"]: u for u in target_users if u.get("email")}

        source_roles_result = src_access.get_roles()
        source_roles: list[dict[str, Any]] = [] if isinstance(source_roles_result, dict) and "error" in source_roles_result else source_roles_result

        target_roles_result = tgt_access.get_roles()
        if isinstance(target_roles_result, dict) and "error" in target_roles_result:
            raw_error = target_roles_result["error"]
            self.logger.error("Failed to fetch roles from target: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_target_mappings", "message": "Failed to fetch roles from target.", "raw_error": raw_error})
            return summary
        target_roles: list[dict[str, Any]] = target_roles_result

        target_groups_result = tgt_access.get_groups()
        target_groups: list[dict[str, Any]] = [] if isinstance(target_groups_result, dict) and "error" in target_groups_result else target_groups_result
        group_name_to_id: dict[str, str] = {g["name"]: g["_id"] for g in target_groups if g.get("name") and g.get("_id")}

        role_id_map, unmapped_role_names = _build_role_id_map(
            source_roles,
            target_roles,
            ignore_custom_roles=ignore_custom_roles,
            target_operating_system=self.target_client.operating_system,
        )
        if unmapped_role_names:
            self.logger.warning("No matching target role found for source role(s): %s", sorted(unmapped_role_names))
            self._emit(
                emit,
                {
                    "type": "warning",
                    "step": "fetch_target_mappings",
                    "message": "Some source roles have no matching target role.",
                    "unmapped_role_names": sorted(unmapped_role_names),
                },
            )

        self.logger.debug("Found %s user(s), %s role(s), %s group(s) on target.", len(target_users), len(target_roles), len(target_groups))
        self._emit(emit, {"type": "progress", "step": "fetch_target_mappings", "message": "Fetched target mappings.", "user_count": len(target_users)})

        # Step 4: Resolve conflicts and build the bulk payload
        bulk_payload: list[dict[str, Any]] = []
        pending_emails: list[str] = []

        for user in users_to_migrate:
            email = user.get("email")

            if not email:
                self.logger.warning("Skipping a user — missing email field.")
                summary["skipped"].append({"email": None, "reason": "Missing email field."})
                continue

            existing = target_by_email.get(email)

            if existing and action == "skip":
                self.logger.info("Skipping '%s' — already exists on target.", email)
                summary["skipped"].append({"email": email, "reason": "Already exists on target."})
                self._emit(emit, {"type": "progress", "step": "migrate_user", "message": f"Skipped '{email}' (already exists).", "action": "skip"})
                continue

            role_id = role_id_map.get((user.get("role") or {}).get("_id"))
            if role_id is None:
                role_name = (user.get("role") or {}).get("name")
                reason = f"No matching target role found for source role '{role_name}'."
                self.logger.warning("Skipping user '%s' — %s", email, reason)
                summary["failed"].append({"email": email, "reason": reason})
                continue

            self._emit(emit, {"type": "progress", "step": "migrate_user", "message": f"Queuing '{email}' for migration.", "action": action})

            if existing and action == "overwrite":
                self.logger.info("Deleting existing user '%s' on target.", email)
                del_result = tgt_access.delete_user(email)
                if isinstance(del_result, dict) and "error" in del_result:
                    self.logger.warning("Could not delete existing user '%s': %s — proceeding with create.", email, del_result["error"])

            bulk_payload.append(_build_user_payload(user, role_id, group_name_to_id))
            pending_emails.append(email)

        if not bulk_payload:
            summary["skipped_count"] = len(summary["skipped"])
            summary["failed_count"] = len(summary["failed"])
            ok = summary["source_count"] > 0 and summary["failed_count"] == 0
            summary["ok"] = ok
            summary["status"] = "success" if ok else "failed"
            self._emit(emit, {"type": "completed", "step": "done", "message": "Finished user migration.", "status": summary["status"]})
            return summary

        # Step 5: Bulk-create the eligible users on target
        self.logger.info("Sending bulk migration request for %s user(s).", len(bulk_payload))
        self.logger.debug("Payload for bulk migration: %s", bulk_payload)
        self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Sending bulk migration request.", "count": len(bulk_payload)})
        create_result = tgt_access.create_users_bulk(bulk_payload)

        if isinstance(create_result, dict) and "error" in create_result:
            raw_error = create_result["error"]
            self.logger.error("Bulk migration failed: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "bulk_post", "message": "Bulk migration failed.", "raw_error": raw_error})
            for email in pending_emails:
                summary["failed"].append({"email": email, "reason": f"Bulk create failed: {raw_error}"})
        else:
            self.logger.info("Bulk migration succeeded.")
            self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Bulk migration request succeeded."})
            for email in pending_emails:
                summary["succeeded"].append({"email": email})

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "User migration complete. source=%s succeeded=%s skipped=%s failed=%s",
            summary["source_count"],
            summary["succeeded_count"],
            summary["skipped_count"],
            summary["failed_count"],
        )
        self._emit(
            emit,
            {
                "type": "completed",
                "step": "done",
                "message": "Finished user migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_users(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        ignore_custom_roles: bool = False,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all eligible users from source to target.

        Fetches every user from the source environment, excludes users with
        the built-in ``super`` role (the source and target super admin
        accounts are expected to already exist independently on each
        environment), and — when the source environment exposes tenant
        information — restricts migration to users belonging to the system
        tenant, then delegates to ``migrate_users``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every user.
        ignore_custom_roles : bool, default False
            When ``True``, strips a ``custom_`` prefix from source role names
            before matching them against target roles.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_users``.

        Notes
        -----
        If the source environment does not expose tenant information (single
        tenant on-premises deployments), tenant-based filtering is skipped and
        every non-``super`` user is treated as eligible.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full user migration from source to target."})
        self.logger.info("Starting full user migration from source to target.")

        src_access = AccessManagement(api_client=self.source_client)

        self._emit(emit, {"type": "progress", "step": "fetch_source_users", "message": "Fetching all users from source."})
        source_result = src_access.get_users_expanded()
        if isinstance(source_result, dict) and "error" in source_result:
            raw_error = source_result["error"]
            self.logger.error("Failed to fetch users from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_users", "message": "Failed to fetch users from source.", "raw_error": raw_error})
            return _empty_summary()

        all_source_users: list[dict[str, Any]] = source_result
        self.logger.info("Found %s user(s) on source.", len(all_source_users))
        self._emit(emit, {"type": "progress", "step": "fetch_source_users", "message": "Fetched users from source.", "count": len(all_source_users)})

        if not all_source_users:
            self.logger.info("No users found on source. Nothing to migrate.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No users found on source.", "status": "noop"})
            return _empty_summary(ok=True, status="noop")

        # Resolve the system tenant, when available, so multi-tenant users are excluded.
        self._emit(emit, {"type": "progress", "step": "fetch_system_tenant", "message": "Resolving system tenant on the source environment."})
        tenants_result = src_access.get_tenants()
        system_tenant_id: str | None = None
        if isinstance(tenants_result, list):
            for tenant in tenants_result:
                if isinstance(tenant, dict) and tenant.get("name") == "system":
                    system_tenant_id = tenant.get("_id")
                    break
        self._emit(emit, {"type": "progress", "step": "fetch_system_tenant", "message": "Resolved system tenant.", "system_tenant_id": system_tenant_id})

        eligible_emails = [
            u["email"] for u in all_source_users if u.get("email") and (u.get("role") or {}).get("name") != "super" and (system_tenant_id is None or u.get("tenantId") == system_tenant_id)
        ]

        if not eligible_emails:
            self.logger.info("No eligible users found for migration after filtering.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No eligible users to migrate.", "status": "noop"})
            return _empty_summary(ok=True, status="noop")

        return self.migrate_users(user_emails=eligible_emails, action=action, ignore_custom_roles=ignore_custom_roles, emit=emit)
