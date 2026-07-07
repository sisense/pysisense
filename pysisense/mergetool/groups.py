from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from ..access_management import AccessManagement

_PAYLOAD_FIELDS_TO_STRIP = frozenset({"_id", "created", "lastUpdated", "tenantId"})
_EXCLUDED_ALL_GROUPS_NAMES = frozenset({"Admins", "All users in system", "Everyone"})


def _build_group_payload(group: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in group.items() if k not in _PAYLOAD_FIELDS_TO_STRIP}


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


class GroupsMergeMixin:
    def migrate_groups(
        self,
        group_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific groups from source to target.

        Fetches the requested groups from the source environment and creates
        (or replaces) them on the target via the bulk group endpoint. Conflict
        detection is based on the group's ``name`` field.

        Parameters
        ----------
        group_names : list[str] or None, default None
            Group names to migrate. If ``None``, every group on the source is
            migrated.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a group with the same ``name`` already
            exists on the target.

            - ``"skip"`` — leave the existing group unchanged.
            - ``"overwrite"`` — delete the existing group then recreate from
              source. **Warning:** this can disrupt user/group associations
              still referencing the deleted group on the target.
            - ``"duplicate"`` — always create, regardless of conflicts.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``name``
            - ``skipped`` : list[dict] — each has ``name`` and ``reason``
            - ``failed`` : list[dict] — each has ``name`` and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting group migration from source to target."})
        self.logger.info("Starting group migration from source to target.")

        summary = _empty_summary()

        src_access = AccessManagement(api_client=self.source_client)
        tgt_access = AccessManagement(api_client=self.target_client)

        # Step 1: Fetch all source groups
        self._emit(emit, {"type": "progress", "step": "fetch_source_groups", "message": "Fetching groups from the source environment."})
        self.logger.debug("Fetching groups from source.")
        source_result = src_access.get_groups()
        if isinstance(source_result, dict) and "error" in source_result:
            raw_error = source_result["error"]
            self.logger.error("Failed to fetch groups from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_groups", "message": "Failed to fetch groups from source.", "raw_error": raw_error})
            return summary

        all_source_groups: list[dict[str, Any]] = source_result
        self.logger.debug("Found %s group(s) on source.", len(all_source_groups))

        # Step 2: Filter to the requested names
        if group_names is not None:
            wanted: set[str] = set(group_names)
            groups_to_migrate = [g for g in all_source_groups if g.get("name") in wanted]
            for missing_name in wanted - {g.get("name") for g in groups_to_migrate}:
                self.logger.warning("Group '%s' not found on source.", missing_name)
                summary["failed"].append({"name": missing_name, "reason": "Not found on source."})
        else:
            groups_to_migrate = all_source_groups

        summary["source_count"] = len(groups_to_migrate)

        if not groups_to_migrate:
            self.logger.info("No matching groups found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No groups to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_groups", "message": "Fetched source groups.", "count": len(groups_to_migrate)})

        # Step 3: Fetch target groups for conflict detection
        self._emit(emit, {"type": "progress", "step": "fetch_target_groups", "message": "Fetching groups from the target environment."})
        target_result = tgt_access.get_groups()
        target_groups: list[dict[str, Any]] = [] if isinstance(target_result, dict) and "error" in target_result else target_result
        target_by_name: dict[str, dict[str, Any]] = {g["name"]: g for g in target_groups if g.get("name")}
        self.logger.debug("Found %s group(s) on target.", len(target_groups))
        self._emit(emit, {"type": "progress", "step": "fetch_target_groups", "message": "Fetched target groups.", "count": len(target_groups)})

        # Step 4: Resolve conflicts and build the bulk payload
        bulk_payload: list[dict[str, Any]] = []
        pending_names: list[str] = []

        for group in groups_to_migrate:
            name = group.get("name")

            if not name:
                self.logger.warning("Skipping a group — missing name field.")
                summary["skipped"].append({"name": None, "reason": "Missing name field."})
                continue

            existing = target_by_name.get(name)

            if existing and action == "skip":
                self.logger.info("Skipping '%s' — already exists on target.", name)
                summary["skipped"].append({"name": name, "reason": "Already exists on target."})
                self._emit(emit, {"type": "progress", "step": "migrate_group", "message": f"Skipped '{name}' (already exists).", "action": "skip"})
                continue

            self._emit(emit, {"type": "progress", "step": "migrate_group", "message": f"Queuing '{name}' for migration.", "action": action})

            if existing and action == "overwrite":
                existing_id = existing.get("_id")
                if existing_id:
                    self.logger.info("Deleting existing group '%s' (id=%s) on target.", name, existing_id)
                    del_result = tgt_access.delete_group(existing_id)
                    if isinstance(del_result, dict) and "error" in del_result:
                        self.logger.warning("Could not delete existing group '%s': %s — proceeding with create.", name, del_result["error"])

            bulk_payload.append(_build_group_payload(group))
            pending_names.append(name)

        if not bulk_payload:
            summary["skipped_count"] = len(summary["skipped"])
            summary["failed_count"] = len(summary["failed"])
            ok = summary["source_count"] > 0 and summary["failed_count"] == 0
            summary["ok"] = ok
            summary["status"] = "success" if ok else "failed"
            self._emit(emit, {"type": "completed", "step": "done", "message": "Finished group migration.", "status": summary["status"]})
            return summary

        # Step 5: Bulk-create the eligible groups on target
        self.logger.info("Sending bulk migration request for %s group(s).", len(bulk_payload))
        self.logger.debug("Payload for bulk migration: %s", bulk_payload)
        self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Sending bulk migration request.", "count": len(bulk_payload)})
        create_result = tgt_access.create_groups_bulk(bulk_payload)

        if isinstance(create_result, dict) and "error" in create_result:
            raw_error = create_result["error"]
            self.logger.error("Bulk migration failed: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "bulk_post", "message": "Bulk migration failed.", "raw_error": raw_error})
            for name in pending_names:
                summary["failed"].append({"name": name, "reason": f"Bulk create failed: {raw_error}"})
        else:
            self.logger.info("Bulk migration succeeded.")
            self._emit(emit, {"type": "progress", "step": "bulk_post", "message": "Bulk migration request succeeded."})
            for name in pending_names:
                summary["succeeded"].append({"name": name})

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "Group migration complete. source=%s succeeded=%s skipped=%s failed=%s",
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
                "message": "Finished group migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_groups(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all eligible groups from source to target.

        Fetches every group from the source environment, excludes the
        built-in ``Admins``, ``All users in system``, and ``Everyone`` groups,
        and — when the source environment exposes tenant information —
        restricts migration to groups belonging to the system tenant, then
        delegates to ``migrate_groups``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every group.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_groups``.

        Notes
        -----
        If the source environment does not expose tenant information (single
        tenant on-premises deployments), tenant-based filtering is skipped and
        every non-excluded group is treated as eligible.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full group migration from source to target."})
        self.logger.info("Starting full group migration from source to target.")

        src_access = AccessManagement(api_client=self.source_client)

        self._emit(emit, {"type": "progress", "step": "fetch_source_groups", "message": "Fetching all groups from source."})
        source_result = src_access.get_groups()
        if isinstance(source_result, dict) and "error" in source_result:
            raw_error = source_result["error"]
            self.logger.error("Failed to fetch groups from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_groups", "message": "Failed to fetch groups from source.", "raw_error": raw_error})
            return _empty_summary()

        all_source_groups: list[dict[str, Any]] = source_result
        self.logger.info("Found %s group(s) on source.", len(all_source_groups))
        self._emit(emit, {"type": "progress", "step": "fetch_source_groups", "message": "Fetched groups from source.", "count": len(all_source_groups)})

        if not all_source_groups:
            self.logger.info("No groups found on source. Nothing to migrate.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No groups found on source.", "status": "noop"})
            return _empty_summary(ok=True, status="noop")

        # Resolve the system tenant, when available, so multi-tenant groups are excluded.
        self._emit(emit, {"type": "progress", "step": "fetch_system_tenant", "message": "Resolving system tenant on the source environment."})
        tenants_result = src_access.get_tenants()
        system_tenant_id: str | None = None
        if isinstance(tenants_result, list):
            for tenant in tenants_result:
                if isinstance(tenant, dict) and tenant.get("name") == "system":
                    system_tenant_id = tenant.get("_id")
                    break
        self._emit(emit, {"type": "progress", "step": "fetch_system_tenant", "message": "Resolved system tenant.", "system_tenant_id": system_tenant_id})

        eligible_names = [g["name"] for g in all_source_groups if g.get("name") and g["name"] not in _EXCLUDED_ALL_GROUPS_NAMES and (system_tenant_id is None or g.get("tenantId") == system_tenant_id)]

        if not eligible_names:
            self.logger.info("No eligible groups found for migration after filtering.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No eligible groups to migrate.", "status": "noop"})
            return _empty_summary(ok=True, status="noop")

        return self.migrate_groups(group_names=eligible_names, action=action, emit=emit)
