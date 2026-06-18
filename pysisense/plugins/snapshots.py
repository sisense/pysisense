from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class PluginsSnapshotsMixin:
    def save_snapshot(self) -> dict[str, Any]:
        """Capture the current plugin enable/disable state as a snapshot.

        Fetches all plugins and records which ones are currently enabled.
        The returned dict can be stored by the caller and later passed to
        :meth:`restore_snapshot` to bring the instance back to this state.

        Returns
        -------
        dict[str, Any]
            A snapshot dict with keys:

            - ``created`` (str): ISO 8601 UTC timestamp of when the snapshot was taken.
            - ``plugins`` (list[str]): Sorted list of ``folderName`` values for all
              currently enabled plugins.

            Returns ``{"error": "..."}`` if the plugin list could not be fetched.
        """
        all_plugins = self.get_all_plugins()
        if all_plugins and "error" in all_plugins[0]:
            return all_plugins[0]

        enabled_folders = sorted(p["folderName"] for p in all_plugins if p.get("isEnabled"))
        snapshot = {
            "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "plugins": enabled_folders,
        }

        self.logger.info(f"Snapshot captured — {len(enabled_folders)} plugin(s) enabled")
        return snapshot

    def restore_snapshot(self, snapshot: dict[str, Any], bulk: bool = True) -> dict[str, Any]:
        """Restore plugin states to exactly match a previously saved snapshot.

        Compares the snapshot's plugin list against the live instance state and
        computes the minimal set of changes needed — enabling plugins that should
        be on, disabling plugins that should be off. Plugins already in the
        correct state are skipped.

        Parameters
        ----------
        snapshot : dict[str, Any]
            A snapshot dict as returned by :meth:`save_snapshot`, containing at
            minimum a ``"plugins"`` key with a list of ``folderName`` values for
            the plugins that should be enabled. All other plugins will be disabled.
        bulk : bool, optional
            When ``True`` (default), all enable and disable changes are sent in a
            single PATCH request. When ``False``, one PATCH request is made per
            plugin change.

        Returns
        -------
        dict[str, Any]
            A summary dict with keys:

            - ``enabled`` (list[str]): ``folderName`` values that were enabled.
            - ``disabled`` (list[str]): ``folderName`` values that were disabled.
            - ``already_set`` (int): Count of plugins already in the correct state.
            - ``not_in_instance`` (list[str]): Snapshot entries with no matching plugin.
            - ``errors`` (list[str]): ``folderName`` values where the PATCH failed
              (non-bulk mode only).

            Returns ``{"error": "..."}`` if the plugin list could not be fetched or
            the snapshot is missing the ``"plugins"`` key.
        """
        if "plugins" not in snapshot:
            msg = "Snapshot is missing the required 'plugins' key"
            self.logger.error(msg)
            return {"error": msg}

        snapshot_folders: set[str] = set(snapshot["plugins"])
        created = snapshot.get("created", "unknown")
        self.logger.debug(f"Restoring snapshot from {created} — {len(snapshot_folders)} plugin(s) should be enabled")

        all_plugins = self.get_all_plugins()
        if all_plugins and "error" in all_plugins[0]:
            return all_plugins[0]

        by_folder = {p["folderName"]: p for p in all_plugins}

        to_enable = sorted(f for f in snapshot_folders if f in by_folder and not by_folder[f].get("isEnabled"))
        to_disable = sorted(p["folderName"] for p in all_plugins if p.get("isEnabled") and p["folderName"] not in snapshot_folders)
        not_in_instance = sorted(f for f in snapshot_folders if f not in by_folder)
        already_set = len(all_plugins) - len(to_enable) - len(to_disable)

        self.logger.debug(f"Restore plan — enable: {len(to_enable)}, disable: {len(to_disable)}, already set: {already_set}, not in instance: {len(not_in_instance)}")

        result: dict[str, Any] = {
            "enabled": [],
            "disabled": [],
            "already_set": already_set,
            "not_in_instance": not_in_instance,
            "errors": [],
        }

        updates = [{"folderName": f, "isEnabled": True} for f in to_enable] + [{"folderName": f, "isEnabled": False} for f in to_disable]

        if not updates:
            self.logger.info("Restore complete — no changes needed")
            return result

        if bulk:
            self.logger.debug(f"Sending bulk PATCH with {len(updates)} update(s)")
            patch_result = self._patch_plugins(updates)
            if "error" in patch_result:
                return patch_result
            result["enabled"] = to_enable
            result["disabled"] = to_disable
        else:
            for update in updates:
                folder = update["folderName"]
                patch_result = self._patch_plugins([update])
                if "error" in patch_result:
                    self.logger.error(f"Failed to update plugin '{folder}': {patch_result['error']}")
                    result["errors"].append(folder)
                elif update["isEnabled"]:
                    result["enabled"].append(folder)
                else:
                    result["disabled"].append(folder)

        self.logger.info(
            f"Restore complete — enabled: {len(result['enabled'])}, disabled: {len(result['disabled'])}, "
            f"already set: {already_set}, not in instance: {len(not_in_instance)}, errors: {len(result['errors'])}"
        )
        return result
