from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, Literal

from ..access_management import AccessManagement
from ..dashboard import Dashboard
from ..folder import Folder
from .folder import _build_oid_to_folder, _build_path_map

_LOCAL_DATASOURCE_ADDRESS = "LocalHost"


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


def _update_datasource_references(obj: Any) -> int:
    """Recursively repoint embedded datasource references to the local Elasticube."""
    updates_made = 0

    if isinstance(obj, dict):
        if "address" in obj and ("title" in obj or "fullname" in obj) and obj.get("address") != _LOCAL_DATASOURCE_ADDRESS:
            obj["address"] = _LOCAL_DATASOURCE_ADDRESS
            if "title" in obj:
                obj["fullname"] = f"{_LOCAL_DATASOURCE_ADDRESS}/{obj['title']}"
            elif "fullname" in obj and "/" in obj["fullname"]:
                obj["fullname"] = f"{_LOCAL_DATASOURCE_ADDRESS}/{obj['fullname'].split('/', 1)[1]}"
            updates_made += 1

        for value in obj.values():
            updates_made += _update_datasource_references(value)

    elif isinstance(obj, list):
        for item in obj:
            updates_made += _update_datasource_references(item)

    return updates_made


def _resolve_share_entries(
    shares: list[dict[str, Any]],
    user_id_to_email: dict[str, str],
    group_id_to_name: dict[str, str],
) -> list[dict[str, Any]]:
    """Convert exported dashboard shares into name-based entries for add_dashboard_shares."""
    resolved: list[dict[str, Any]] = []
    for share in shares:
        share_type = share.get("type")
        share_id = share.get("shareId")
        name = user_id_to_email.get(share_id) if share_type == "user" else group_id_to_name.get(share_id) if share_type == "group" else None
        if name:
            resolved.append({"name": name, "type": share_type, "rule": share.get("rule", "view")})
    return resolved


def _migrate_one_dashboard(
    src_dashboard: Dashboard,
    tgt_dashboard: Dashboard,
    dashboard: dict[str, Any],
    action: str,
    target_oids: set[str],
    user_id_to_email: dict[str, str],
    email_to_target_id: dict[str, str],
    group_id_to_name: dict[str, str],
    src_path_map: dict[str, str],
    tgt_path_to_oid: dict[str, str],
    summary: dict[str, Any],
    logger: Any,
    progress: Callable[[dict[str, Any]], None],
) -> None:
    """Migrate a single dashboard, mutating ``summary`` in place.

    Safe to call concurrently — dashboards are independent of each other.
    """
    source_oid = dashboard.get("oid")
    title = dashboard.get("title", source_oid or "Unknown")

    if not source_oid:
        logger.warning("Skipping dashboard '%s' — missing oid field.", title)
        summary["skipped"].append({"title": title, "source_oid": None, "reason": "Missing oid field."})
        return

    if source_oid in target_oids and action == "skip":
        logger.info("Skipping '%s' — already exists on target.", title)
        summary["skipped"].append({"title": title, "source_oid": source_oid, "reason": "Already exists on target."})
        progress({"type": "progress", "step": "migrate_dashboard", "message": f"Skipped '{title}' (already exists).", "action": "skip"})
        return

    progress({"type": "progress", "step": "migrate_dashboard", "message": f"Migrating '{title}'.", "source_oid": source_oid, "action": action})

    exported = src_dashboard.export_dashboard(source_oid)
    if "error" in exported:
        reason = exported["error"]
        logger.error("Failed to export dashboard '%s': %s", title, reason)
        summary["failed"].append({"title": title, "source_oid": source_oid, "reason": f"Export failed: {reason}"})
        progress({"type": "error", "step": "migrate_dashboard", "message": f"Export failed for '{title}'.", "reason": reason})
        return

    updates_made = _update_datasource_references(exported)
    logger.debug("Dashboard '%s': updated %s datasource reference(s).", title, updates_made)

    import_response = tgt_dashboard.import_dashboards_bulk([exported], action=action)
    if "error" in import_response:
        reason = import_response["error"]
        logger.error("Failed to import dashboard '%s': %s", title, reason)
        summary["failed"].append({"title": title, "source_oid": source_oid, "reason": f"Import failed: {reason}"})
        progress({"type": "error", "step": "migrate_dashboard", "message": f"Import failed for '{title}'.", "reason": reason})
        return

    succeeded_entries = import_response.get("succeded") or import_response.get("succeeded") or []
    if not succeeded_entries:
        reason = "Import did not report success."
        logger.error("Failed to import dashboard '%s': %s", title, reason)
        summary["failed"].append({"title": title, "source_oid": source_oid, "reason": reason})
        progress({"type": "error", "step": "migrate_dashboard", "message": f"Import failed for '{title}'.", "reason": reason})
        return

    new_oid = succeeded_entries[0].get("oid", source_oid)

    # Owner remap
    owner_email = user_id_to_email.get(exported.get("owner"))
    target_owner_id = email_to_target_id.get(owner_email) if owner_email else None
    if target_owner_id:
        tgt_dashboard.change_dashboard_owner(new_oid, target_owner_id)
    elif exported.get("owner"):
        logger.warning("Could not resolve target owner for dashboard '%s' — leaving owner unchanged.", title)

    # Share remap
    resolved_shares = _resolve_share_entries(exported.get("shares") or [], user_id_to_email, group_id_to_name)
    if resolved_shares:
        tgt_dashboard.add_dashboard_shares(new_oid, resolved_shares)

    # Folder placement
    parent_folder_oid = exported.get("parentFolder")
    if parent_folder_oid:
        folder_path = src_path_map.get(parent_folder_oid)
        target_folder_oid = tgt_path_to_oid.get(folder_path) if folder_path else None
        if target_folder_oid:
            tgt_dashboard.move_dashboard_to_folder(new_oid, target_folder_oid)
        else:
            logger.warning("Target folder for dashboard '%s' not found — leaving it at the root. Migrate folders first.", title)

    logger.info("Successfully migrated dashboard '%s'.", title)
    summary["succeeded"].append({"title": title, "oid": new_oid, "source_oid": source_oid})
    progress({"type": "progress", "step": "migrate_dashboard", "message": f"Migrated '{title}'.", "action": action})


class DashboardMergeMixin:
    def migrate_dashboards(
        self,
        dashboard_ids: list[str] | None = None,
        dashboard_names: list[str] | None = None,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate specific dashboards from source to target.

        Exports each dashboard from the source environment, repoints its
        embedded datasource references to the target's local Elasticube, and
        imports it into the target via the bulk import endpoint, which
        matches dashboards by ``oid`` and applies ``action`` natively.
        After a successful import, the dashboard's owner and shares are
        remapped from source to target users/groups (matched by email/name),
        and the dashboard is moved into the target folder whose path matches
        its source parent folder's path — if that folder has already been
        migrated with ``migrate_folders``. Conflict detection for the
        ``action`` parameter itself is based on the dashboard's ``oid``.

        Parameters
        ----------
        dashboard_ids : list[str] or None, default None
            Dashboard OIDs to migrate. Provide either this or
            ``dashboard_names``.
        dashboard_names : list[str] or None, default None
            Dashboard titles to migrate. Provide either this or
            ``dashboard_ids``.
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy when a dashboard with the same ``oid`` already
            exists on the target.

            - ``"skip"`` — leave the existing dashboard unchanged.
            - ``"overwrite"`` — replace the existing dashboard with the
              source version.
            - ``"duplicate"`` — always create, regardless of conflicts.
        concurrency : int, default 1
            Maximum number of dashboards to migrate concurrently, run via a
            background thread pool (``asyncio.to_thread``) since the
            underlying HTTP client is synchronous. Dashboards are independent
            of each other, so any value is safe. Values <= 1 (the default)
            process dashboards one at a time.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys. When
            ``concurrency`` is greater than 1, this callback may be invoked
            from multiple worker threads concurrently.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``title``, ``oid``, and
              ``source_oid``
            - ``skipped`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``failed`` : list[dict] — each has ``title``, ``source_oid``,
              and ``reason``
            - ``source_count`` : int
            - ``succeeded_count`` : int
            - ``skipped_count`` : int
            - ``failed_count`` : int

        Raises
        ------
        ValueError
            If both or neither of ``dashboard_ids`` and ``dashboard_names``
            are provided.

        Notes
        -----
        If called from code that is already running an asyncio event loop,
        ``concurrency`` greater than 1 falls back to sequential processing (a
        nested event loop cannot be started) and logs a warning.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting dashboard migration from source to target."})

        if dashboard_ids and dashboard_names:
            msg = "Provide either 'dashboard_ids' or 'dashboard_names', not both."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)
        if not dashboard_ids and not dashboard_names:
            msg = "Provide either 'dashboard_ids' or 'dashboard_names'."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)

        self.logger.info("Starting dashboard migration from source to target.")

        summary = _empty_summary()

        src_dashboard = Dashboard(api_client=self.source_client)
        tgt_dashboard = Dashboard(api_client=self.target_client)

        # Step 1: Fetch all source dashboards
        self._emit(emit, {"type": "progress", "step": "fetch_source_dashboards", "message": "Fetching dashboards from the source environment."})
        self.logger.debug("Fetching dashboards from source.")
        src_response = src_dashboard.get_all_dashboards()
        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch dashboards from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_dashboards", "message": "Failed to fetch dashboards from source.", "raw_error": raw_error})
            return summary

        all_source_dashboards: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        self.logger.debug("Found %s dashboard(s) on source.", len(all_source_dashboards))

        # Step 2: Filter to the requested ids or names
        if dashboard_ids:
            wanted: set[str] = set(dashboard_ids)
            dashboards_to_migrate = [d for d in all_source_dashboards if d.get("oid") in wanted]
            for missing_id in wanted - {d.get("oid") for d in dashboards_to_migrate}:
                self.logger.warning("Dashboard oid '%s' not found on source.", missing_id)
                summary["failed"].append({"title": None, "source_oid": missing_id, "reason": "Not found on source."})
        else:
            wanted_names: set[str] = set(dashboard_names)
            dashboards_to_migrate = [d for d in all_source_dashboards if d.get("title") in wanted_names]
            for missing_name in wanted_names - {d.get("title") for d in dashboards_to_migrate}:
                self.logger.warning("Dashboard '%s' not found on source.", missing_name)
                summary["failed"].append({"title": missing_name, "source_oid": None, "reason": "Not found on source."})

        summary["source_count"] = len(dashboards_to_migrate)

        if not dashboards_to_migrate:
            self.logger.info("No matching dashboards found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No dashboards to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_dashboards", "message": "Fetched source dashboards.", "count": len(dashboards_to_migrate)})

        # Step 3: Fetch target dashboards for conflict detection
        self._emit(emit, {"type": "progress", "step": "fetch_target_dashboards", "message": "Fetching dashboards from the target environment."})
        tgt_response = tgt_dashboard.get_all_dashboards()
        target_dashboards: list[dict[str, Any]] = [] if isinstance(tgt_response, dict) and "error" in tgt_response else tgt_response
        target_oids: set[str] = {d["oid"] for d in target_dashboards if d.get("oid")}
        self.logger.debug("Found %s dashboard(s) on target.", len(target_dashboards))
        self._emit(emit, {"type": "progress", "step": "fetch_target_dashboards", "message": "Fetched target dashboards.", "count": len(target_dashboards)})

        # Step 4: Fetch users/groups on both sides, for owner and share resolution
        self._emit(emit, {"type": "progress", "step": "fetch_mappings", "message": "Fetching users, groups, and folders for owner/share/folder resolution."})
        src_access = AccessManagement(api_client=self.source_client)
        tgt_access = AccessManagement(api_client=self.target_client)

        src_users_result = src_access._get_users_raw()
        src_users: list[dict[str, Any]] = [] if isinstance(src_users_result, dict) and "error" in src_users_result else src_users_result
        user_id_to_email: dict[str, str] = {u["_id"]: u["email"] for u in src_users if u.get("_id") and u.get("email")}

        tgt_users_result = tgt_access._get_users_raw()
        tgt_users: list[dict[str, Any]] = [] if isinstance(tgt_users_result, dict) and "error" in tgt_users_result else tgt_users_result
        email_to_target_id: dict[str, str] = {u["email"]: u["_id"] for u in tgt_users if u.get("email") and u.get("_id")}

        src_groups_result = src_access.get_groups()
        src_groups: list[dict[str, Any]] = [] if isinstance(src_groups_result, dict) and "error" in src_groups_result else src_groups_result
        group_id_to_name: dict[str, str] = {g["_id"]: g["name"] for g in src_groups if g.get("_id") and g.get("name")}

        # Step 5: Fetch folder trees on both sides, for parent-folder placement
        src_folder = Folder(api_client=self.source_client)
        tgt_folder = Folder(api_client=self.target_client)

        src_folders_result = src_folder.get_folders("flat")
        src_folders: list[dict[str, Any]] = src_folders_result if isinstance(src_folders_result, list) else []
        src_path_map = _build_path_map(_build_oid_to_folder(src_folders))

        tgt_folders_result = tgt_folder.get_folders("flat")
        tgt_folders: list[dict[str, Any]] = tgt_folders_result if isinstance(tgt_folders_result, list) else []
        tgt_path_map = _build_path_map(_build_oid_to_folder(tgt_folders))
        tgt_path_to_oid: dict[str, str] = {path: oid for oid, path in tgt_path_map.items() if path}

        # Step 6: Migrate each dashboard — independent of each other, so the
        # whole set can run concurrently when concurrency > 1.
        progress = functools.partial(self._emit, emit)

        def _worker(dashboard: dict[str, Any]) -> None:
            _migrate_one_dashboard(
                src_dashboard,
                tgt_dashboard,
                dashboard,
                action,
                target_oids,
                user_id_to_email,
                email_to_target_id,
                group_id_to_name,
                src_path_map,
                tgt_path_to_oid,
                summary,
                self.logger,
                progress,
            )

        self._run_concurrently(dashboards_to_migrate, _worker, concurrency, "dashboards")

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "Dashboard migration complete. source=%s succeeded=%s skipped=%s failed=%s",
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
                "message": "Finished dashboard migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_dashboards(
        self,
        action: Literal["skip", "overwrite", "duplicate"] = "skip",
        concurrency: int = 1,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate all dashboards from source to target.

        Fetches every dashboard from the source environment and delegates to
        ``migrate_dashboards``.

        Parameters
        ----------
        action : {"skip", "overwrite", "duplicate"}, default "skip"
            Conflict strategy applied to every dashboard.
        concurrency : int, default 1
            Same as in ``migrate_dashboards``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_dashboards``.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full dashboard migration from source to target."})
        self.logger.info("Starting full dashboard migration from source to target.")

        src_dashboard = Dashboard(api_client=self.source_client)

        self._emit(emit, {"type": "progress", "step": "fetch_source_dashboards", "message": "Fetching all dashboards from source."})
        src_response = src_dashboard.get_all_dashboards()

        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch dashboards from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_dashboards", "message": "Failed to fetch dashboards from source.", "raw_error": raw_error})
            return _empty_summary()

        all_dashboards: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        dashboard_ids = [d["oid"] for d in all_dashboards if d.get("oid")]

        self.logger.info("Found %s dashboard(s) on source.", len(all_dashboards))
        self._emit(emit, {"type": "progress", "step": "fetch_source_dashboards", "message": "Fetched dashboards from source.", "count": len(all_dashboards)})

        if not dashboard_ids:
            self.logger.info("No dashboards found on source. Nothing to migrate.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No dashboards found on source.", "status": "noop"})
            return _empty_summary(ok=True, status="noop")

        return self.migrate_dashboards(dashboard_ids=dashboard_ids, action=action, concurrency=concurrency, emit=emit)
