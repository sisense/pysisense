from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ..access_management import AccessManagement
from ..datamodel import DataModel


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


def _resolve_datasecurity_shares(
    shares: list[dict[str, Any]],
    party_key: str,
    user_id_to_email: dict[str, str],
    email_to_target_id: dict[str, str],
    group_id_to_name: dict[str, str],
    group_name_to_target_id: dict[str, str],
) -> list[dict[str, Any]]:
    """Convert a rule's source user/group shares into target-environment ids.

    ``party_key`` is ``"partyId"`` for LIVE data models and ``"party"`` for
    EXTRACT data models — the two datasecurity write endpoints identify the
    shared user/group under a different field name.
    """
    resolved: list[dict[str, Any]] = []
    for share in shares:
        if not isinstance(share, dict):
            continue
        share_type = share.get("type")

        if share_type == "default":
            resolved.append({"type": "default"})
            continue

        source_party_id = share.get("partyId") or share.get("party")
        target_party_id: str | None = None
        if share_type == "user":
            email = user_id_to_email.get(source_party_id)
            target_party_id = email_to_target_id.get(email) if email else None
        elif share_type == "group":
            name = group_id_to_name.get(source_party_id)
            target_party_id = group_name_to_target_id.get(name) if name else None

        if target_party_id:
            resolved.append({"type": share_type, party_key: target_party_id})
    return resolved


def _build_datasecurity_rule(
    rule: dict[str, Any],
    party_key: str,
    user_id_to_email: dict[str, str],
    email_to_target_id: dict[str, str],
    group_id_to_name: dict[str, str],
    group_name_to_target_id: dict[str, str],
) -> dict[str, Any]:
    """Rebuild a raw source datasecurity rule into a write-ready payload with resolved shares."""
    new_rule: dict[str, Any] = {
        "table": rule.get("table"),
        "column": rule.get("column"),
        "datatype": rule.get("datatype"),
        "members": rule.get("members", []),
        "exclusionary": rule.get("exclusionary", False),
    }
    if "allMembers" in rule:
        new_rule["allMembers"] = rule["allMembers"]

    new_rule["shares"] = _resolve_datasecurity_shares(
        rule.get("shares", []),
        party_key,
        user_id_to_email,
        email_to_target_id,
        group_id_to_name,
        group_name_to_target_id,
    )
    return new_rule


class DatasecurityMergeMixin:
    def migrate_datasecurity(
        self,
        datamodel_ids: list[str] | None = None,
        datamodel_names: list[str] | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate row-level datasecurity rules for specific data models from source to target.

        Fetches each data model's raw datasecurity rules from the source,
        remaps the user/group shares on every rule to their target-environment
        ids (matched by email for users and by name for groups), and writes
        the resolved rules onto the already-existing target data model.
        Requires the target data model to already exist — run
        ``migrate_datamodels``/``migrate_all_datamodels`` first.

        Parameters
        ----------
        datamodel_ids : list[str] or None, default None
            Data model OIDs to migrate. Provide either this or
            ``datamodel_names``.
        datamodel_names : list[str] or None, default None
            Data model titles to migrate. Provide either this or
            ``datamodel_ids``.
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback. Each invocation receives a dict with at
            least ``type``, ``step``, and ``message`` keys.

        Returns
        -------
        dict[str, Any]
            - ``ok`` : bool
            - ``status`` : "success" | "failed" | "noop"
            - ``succeeded`` : list[dict] — each has ``title``, ``source_oid``,
              and ``rule_count``
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
            If both or neither of ``datamodel_ids`` and ``datamodel_names``
            are provided.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting datasecurity migration from source to target."})

        if datamodel_ids and datamodel_names:
            msg = "Provide either 'datamodel_ids' or 'datamodel_names', not both."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)
        if not datamodel_ids and not datamodel_names:
            msg = "Provide either 'datamodel_ids' or 'datamodel_names'."
            self._emit(emit, {"type": "error", "step": "validation", "message": msg})
            raise ValueError(msg)

        self.logger.info("Starting datasecurity migration from source to target.")

        summary = _empty_summary()

        src_datamodel = DataModel(api_client=self.source_client)
        tgt_datamodel = DataModel(api_client=self.target_client)

        # Step 1: Fetch all source datamodels
        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetching datamodels from the source environment."})
        self.logger.debug("Fetching datamodels from source.")
        src_response = src_datamodel.get_all_datamodel()
        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch datamodels from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_datamodels", "message": "Failed to fetch datamodels from source.", "raw_error": raw_error})
            return summary

        all_source_datamodels: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        self.logger.debug("Found %s datamodel(s) on source.", len(all_source_datamodels))

        # Step 2: Filter to the requested ids or names
        if datamodel_ids:
            wanted: set[str] = set(datamodel_ids)
            datamodels_to_migrate = [d for d in all_source_datamodels if d.get("oid") in wanted]
            for missing_id in wanted - {d.get("oid") for d in datamodels_to_migrate}:
                self.logger.warning("Datamodel oid '%s' not found on source.", missing_id)
                summary["failed"].append({"title": None, "source_oid": missing_id, "reason": "Not found on source."})
        else:
            wanted_names: set[str] = set(datamodel_names)
            datamodels_to_migrate = [d for d in all_source_datamodels if d.get("title") in wanted_names]
            for missing_name in wanted_names - {d.get("title") for d in datamodels_to_migrate}:
                self.logger.warning("Datamodel '%s' not found on source.", missing_name)
                summary["failed"].append({"title": missing_name, "source_oid": None, "reason": "Not found on source."})

        summary["source_count"] = len(datamodels_to_migrate)

        if not datamodels_to_migrate:
            self.logger.info("No matching datamodels found on source.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No datamodels to migrate.", "status": "noop"})
            summary["ok"] = True
            summary["status"] = "noop"
            return summary

        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetched source datamodels.", "count": len(datamodels_to_migrate)})

        # Step 3: Fetch target datamodels — datasecurity can only be written onto a model that already exists
        self._emit(emit, {"type": "progress", "step": "fetch_target_datamodels", "message": "Fetching datamodels from the target environment."})
        tgt_response = tgt_datamodel.get_all_datamodel()
        target_datamodels: list[dict[str, Any]] = [] if isinstance(tgt_response, dict) and "error" in tgt_response else tgt_response
        target_titles: set[str] = {d["title"] for d in target_datamodels if d.get("title")}
        self.logger.debug("Found %s datamodel(s) on target.", len(target_datamodels))
        self._emit(emit, {"type": "progress", "step": "fetch_target_datamodels", "message": "Fetched target datamodels.", "count": len(target_datamodels)})

        # Step 4: Fetch user/group mappings for share resolution
        self._emit(emit, {"type": "progress", "step": "fetch_mappings", "message": "Fetching users and groups for share resolution."})
        src_access = AccessManagement(api_client=self.source_client)
        tgt_access = AccessManagement(api_client=self.target_client)

        src_users_result = src_access.get_users_expanded()
        src_users: list[dict[str, Any]] = [] if isinstance(src_users_result, dict) and "error" in src_users_result else src_users_result
        user_id_to_email: dict[str, str] = {u["_id"]: u["email"] for u in src_users if u.get("_id") and u.get("email")}

        tgt_users_result = tgt_access.get_users_expanded()
        tgt_users: list[dict[str, Any]] = [] if isinstance(tgt_users_result, dict) and "error" in tgt_users_result else tgt_users_result
        email_to_target_id: dict[str, str] = {u["email"]: u["_id"] for u in tgt_users if u.get("email") and u.get("_id")}

        src_groups_result = src_access.get_groups()
        src_groups: list[dict[str, Any]] = [] if isinstance(src_groups_result, dict) and "error" in src_groups_result else src_groups_result
        group_id_to_name: dict[str, str] = {g["_id"]: g["name"] for g in src_groups if g.get("_id") and g.get("name")}

        tgt_groups_result = tgt_access.get_groups()
        tgt_groups: list[dict[str, Any]] = [] if isinstance(tgt_groups_result, dict) and "error" in tgt_groups_result else tgt_groups_result
        group_name_to_target_id: dict[str, str] = {g["name"]: g["_id"] for g in tgt_groups if g.get("name") and g.get("_id")}

        # Step 5: Migrate datasecurity for each datamodel
        for datamodel in datamodels_to_migrate:
            source_oid = datamodel.get("oid")
            title = datamodel.get("title")
            dm_type = (datamodel.get("type") or "").lower()

            if not source_oid or not title:
                self.logger.warning("Skipping a datamodel — missing oid or title field.")
                summary["skipped"].append({"title": title, "source_oid": source_oid, "reason": "Missing oid or title field."})
                continue

            if title not in target_titles:
                self.logger.info("Skipping '%s' — data model not found on target.", title)
                summary["skipped"].append({"title": title, "source_oid": source_oid, "reason": "Data model not found on target. Migrate the data model first."})
                self._emit(emit, {"type": "progress", "step": "migrate_datasecurity", "message": f"Skipped '{title}' (not found on target).", "action": "skip"})
                continue

            if dm_type not in ("extract", "live"):
                self.logger.warning("Skipping '%s' — unsupported datamodel type '%s'.", title, dm_type)
                summary["skipped"].append({"title": title, "source_oid": source_oid, "reason": f"Unsupported datamodel type '{dm_type}'."})
                continue

            self._emit(emit, {"type": "progress", "step": "migrate_datasecurity", "message": f"Migrating datasecurity for '{title}'.", "source_oid": source_oid})

            raw_result = src_datamodel.get_datasecurity_raw(title, datamodel_type=dm_type)
            if isinstance(raw_result, dict) and "error" in raw_result:
                reason = f"Failed to fetch datasecurity rules for '{title}' from source: {raw_result['error']}"
                self.logger.error(reason)
                summary["failed"].append({"title": title, "source_oid": source_oid, "reason": reason})
                self._emit(emit, {"type": "error", "step": "migrate_datasecurity", "message": reason})
                continue

            source_rules: list[dict[str, Any]] = raw_result if isinstance(raw_result, list) else []

            if not source_rules:
                self.logger.info("No datasecurity rules found for '%s' on source.", title)
                summary["skipped"].append({"title": title, "source_oid": source_oid, "reason": "No datasecurity rules found on source."})
                self._emit(emit, {"type": "progress", "step": "migrate_datasecurity", "message": f"No datasecurity rules to migrate for '{title}'."})
                continue

            party_key = "partyId" if dm_type == "live" else "party"
            new_rules = [_build_datasecurity_rule(rule, party_key, user_id_to_email, email_to_target_id, group_id_to_name, group_name_to_target_id) for rule in source_rules]

            write_result = tgt_datamodel.set_live_datasecurity_add_many(title, new_rules) if dm_type == "live" else tgt_datamodel.update_datasecurity(title, new_rules)

            if isinstance(write_result, dict) and "error" in write_result:
                reason = f"Failed to write datasecurity rules: {write_result['error']}"
                self.logger.error("Failed to migrate datasecurity for '%s': %s", title, write_result["error"])
                summary["failed"].append({"title": title, "source_oid": source_oid, "reason": reason})
                self._emit(emit, {"type": "error", "step": "migrate_datasecurity", "message": f"Failed to migrate datasecurity for '{title}'.", "reason": reason})
                continue

            self.logger.info("Migrated %s datasecurity rule(s) for '%s'.", len(new_rules), title)
            summary["succeeded"].append({"title": title, "source_oid": source_oid, "rule_count": len(new_rules)})
            self._emit(emit, {"type": "progress", "step": "migrate_datasecurity", "message": f"Migrated datasecurity for '{title}'.", "count": len(new_rules)})

        # Final summary
        summary["succeeded_count"] = len(summary["succeeded"])
        summary["skipped_count"] = len(summary["skipped"])
        summary["failed_count"] = len(summary["failed"])
        ok = summary["source_count"] > 0 and summary["failed_count"] == 0
        summary["ok"] = ok
        summary["status"] = "success" if ok else ("noop" if summary["source_count"] == 0 else "failed")

        self.logger.info(
            "Datasecurity migration complete. source=%s succeeded=%s skipped=%s failed=%s",
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
                "message": "Finished datasecurity migration.",
                "status": summary["status"],
                "source_count": summary["source_count"],
                "succeeded_count": summary["succeeded_count"],
                "skipped_count": summary["skipped_count"],
                "failed_count": summary["failed_count"],
            },
        )
        return summary

    def migrate_all_datasecurity(
        self,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        """Migrate datasecurity rules for all data models from source to target.

        Fetches every data model from the source environment and delegates
        to ``migrate_datasecurity``. Data models not yet present on the
        target are skipped — run ``migrate_all_datamodels`` first.

        Parameters
        ----------
        emit : Callable[[dict[str, Any]], None], optional
            Optional progress callback.

        Returns
        -------
        dict[str, Any]
            Same structure as ``migrate_datasecurity``.
        """
        self._emit(emit, {"type": "started", "step": "init", "message": "Starting full datasecurity migration from source to target."})
        self.logger.info("Starting full datasecurity migration from source to target.")

        src_datamodel = DataModel(api_client=self.source_client)

        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetching all datamodels from source."})
        src_response = src_datamodel.get_all_datamodel()

        if isinstance(src_response, dict) and "error" in src_response:
            raw_error = src_response["error"]
            self.logger.error("Failed to fetch datamodels from source: %s", raw_error)
            self._emit(emit, {"type": "error", "step": "fetch_source_datamodels", "message": "Failed to fetch datamodels from source.", "raw_error": raw_error})
            return _empty_summary()

        all_datamodels: list[dict[str, Any]] = src_response if isinstance(src_response, list) else []
        datamodel_ids = [d["oid"] for d in all_datamodels if d.get("oid")]

        self.logger.info("Found %s datamodel(s) on source.", len(all_datamodels))
        self._emit(emit, {"type": "progress", "step": "fetch_source_datamodels", "message": "Fetched datamodels from source.", "count": len(all_datamodels)})

        if not datamodel_ids:
            self.logger.info("No datamodels found on source. Nothing to migrate.")
            self._emit(emit, {"type": "completed", "step": "done", "message": "No datamodels found on source.", "status": "noop"})
            return _empty_summary(ok=True, status="noop")

        return self.migrate_datasecurity(datamodel_ids=datamodel_ids, emit=emit)
