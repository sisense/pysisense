from __future__ import annotations

from typing import Any

_PAGE_SIZE = 20


class PluginsCoreMixin:
    def get_all_plugins(self) -> list[dict[str, Any]]:
        """Retrieve all plugins installed on the Sisense instance.

        Fetches the complete plugin list using paginated requests to
        ``GET /api/v1/plugins``. All pages are collected and returned as a
        single flat list.

        Returns
        -------
        list[dict[str, Any]]
            A list of plugin objects, each containing at minimum:

            - ``name`` (str): API identifier for the plugin.
            - ``folderName`` (str): Filesystem folder name (e.g. ``"plugin-MyPlugin"``).
            - ``isEnabled`` (bool): Whether the plugin is currently active.

            Returns ``[{"error": "..."}]`` on failure.
        """
        endpoint = "/api/v1/plugins"
        plugins: list[dict[str, Any]] = []
        skip = 0

        while True:
            self.logger.debug(f"Fetching plugins — limit={_PAGE_SIZE} skip={skip}")
            response = self.api_client.get(endpoint, params={"limit": _PAGE_SIZE, "skip": skip})

            if response is None or response.status_code != 200:
                status = response.status_code if response is not None else "no response"
                self.logger.error(f"Failed to fetch plugins at skip={skip} — status {status}")
                return [{"error": f"Failed to fetch plugins (status {status})"}]

            data = response.json()
            page = data.get("plugins", [])
            plugins.extend(page)
            skip += len(page)
            total = data.get("count", 0)

            self.logger.debug(f"Fetched {skip} / {total} plugins")
            if skip >= total or not page:
                break

        self.logger.info(f"Retrieved {len(plugins)} plugins")
        return plugins

    def get_plugin(self, plugin: str) -> dict[str, Any]:
        """Get a single plugin by its name or folder name.

        Fetches all plugins and returns the first one whose ``name`` or
        ``folderName`` matches the given string (case-insensitive, with or
        without the ``"plugin-"`` prefix).

        Parameters
        ----------
        plugin : str
            Plugin name or ``folderName`` to look up. The ``"plugin-"`` prefix
            is optional and is stripped before matching.

        Returns
        -------
        dict[str, Any]
            The plugin object on success, or ``{"error": "..."}`` if the plugin
            was not found or the request failed.
        """
        all_plugins = self.get_all_plugins()
        if all_plugins and "error" in all_plugins[0]:
            return all_plugins[0]

        match = self._find_plugin(all_plugins, plugin)
        if match is None:
            msg = f"Plugin '{plugin}' not found"
            self.logger.error(msg)
            return {"error": msg}

        self.logger.info(f"Found plugin '{plugin}' — folderName={match.get('folderName')}")
        return match

    def enable_plugin(self, plugin: str) -> dict[str, Any]:
        """Enable a single plugin by name or folder name.

        Looks up the plugin from the live plugin list then sends a PATCH
        request to enable it. If the plugin is already enabled, no API call
        is made.

        Parameters
        ----------
        plugin : str
            Plugin name or ``folderName`` to enable. The ``"plugin-"`` prefix
            is optional.

        Returns
        -------
        dict[str, Any]
            ``{"folderName": str, "isEnabled": True, "changed": bool}`` on
            success, or ``{"error": "..."}`` on failure.
        """
        return self._set_single_plugin_state(plugin, enabled=True)

    def disable_plugin(self, plugin: str) -> dict[str, Any]:
        """Disable a single plugin by name or folder name.

        Looks up the plugin from the live plugin list then sends a PATCH
        request to disable it. If the plugin is already disabled, no API call
        is made.

        Parameters
        ----------
        plugin : str
            Plugin name or ``folderName`` to disable. The ``"plugin-"`` prefix
            is optional.

        Returns
        -------
        dict[str, Any]
            ``{"folderName": str, "isEnabled": False, "changed": bool}`` on
            success, or ``{"error": "..."}`` on failure.
        """
        return self._set_single_plugin_state(plugin, enabled=False)

    def enable_plugins(self, plugins: str | list[str], bulk: bool = True) -> dict[str, Any]:
        """Enable one or more plugins by name or folder name.

        Fetches the current plugin list, identifies which of the requested
        plugins need to be enabled, and applies the changes. Plugins already
        enabled are skipped without making an API call.

        Parameters
        ----------
        plugins : str or list[str]
            One or more plugin names or folder names. The ``"plugin-"`` prefix
            is optional and is stripped before matching.
        bulk : bool, optional
            When ``True`` (default), all updates are sent in a single PATCH
            request. When ``False``, one PATCH request is made per plugin.

        Returns
        -------
        dict[str, Any]
            A summary dict with keys:

            - ``changed`` (list[str]): ``folderName`` values that were enabled.
            - ``already_enabled`` (list[str]): ``folderName`` values already enabled.
            - ``not_found`` (list[str]): Requested names that matched no plugin.
            - ``errors`` (list[str]): ``folderName`` values where the PATCH failed
              (non-bulk mode only).
        """
        return self._set_plugins_state(plugins, enabled=True, bulk=bulk)

    def disable_plugins(self, plugins: str | list[str], bulk: bool = True) -> dict[str, Any]:
        """Disable one or more plugins by name or folder name.

        Fetches the current plugin list, identifies which of the requested
        plugins need to be disabled, and applies the changes. Plugins already
        disabled are skipped without making an API call.

        Parameters
        ----------
        plugins : str or list[str]
            One or more plugin names or folder names. The ``"plugin-"`` prefix
            is optional and is stripped before matching.
        bulk : bool, optional
            When ``True`` (default), all updates are sent in a single PATCH
            request. When ``False``, one PATCH request is made per plugin.

        Returns
        -------
        dict[str, Any]
            A summary dict with keys:

            - ``changed`` (list[str]): ``folderName`` values that were disabled.
            - ``already_disabled`` (list[str]): ``folderName`` values already disabled.
            - ``not_found`` (list[str]): Requested names that matched no plugin.
            - ``errors`` (list[str]): ``folderName`` values where the PATCH failed
              (non-bulk mode only).
        """
        return self._set_plugins_state(plugins, enabled=False, bulk=bulk)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _normalize_name(self, name: str) -> str:
        """Lowercase and strip the optional ``"plugin-"`` prefix."""
        lower = name.lower()
        return lower[len("plugin-"):] if lower.startswith("plugin-") else lower

    def _find_plugin(self, all_plugins: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
        """Return the first plugin whose name or folderName matches (case-insensitive, prefix-stripped)."""
        needle = self._normalize_name(name)
        for p in all_plugins:
            if self._normalize_name(p.get("name", "")) == needle or self._normalize_name(p.get("folderName", "")) == needle:
                return p
        return None

    def _patch_plugins(self, updates: list[dict[str, Any]]) -> dict[str, Any]:
        """Send a PATCH to ``/api/v1/plugins`` with the given updates list.

        Parameters
        ----------
        updates : list[dict[str, Any]]
            Each item must contain ``folderName`` (str) and ``isEnabled`` (bool).

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on HTTP 200/204, or ``{"error": "..."}`` on failure.
        """
        response = self.api_client.patch("/api/v1/plugins", data=updates)
        if response is None or response.status_code not in (200, 204):
            status = response.status_code if response is not None else "no response"
            msg = f"PATCH /api/v1/plugins failed — status {status}"
            self.logger.error(msg)
            return {"error": msg}
        return {"success": True}

    def _set_single_plugin_state(self, plugin: str, enabled: bool) -> dict[str, Any]:
        """Look up a plugin and set its enabled state, skipping if already set."""
        all_plugins = self.get_all_plugins()
        if all_plugins and "error" in all_plugins[0]:
            return all_plugins[0]

        match = self._find_plugin(all_plugins, plugin)
        if match is None:
            msg = f"Plugin '{plugin}' not found"
            self.logger.error(msg)
            return {"error": msg}

        folder = match["folderName"]
        if match.get("isEnabled") == enabled:
            state = "enabled" if enabled else "disabled"
            self.logger.info(f"Plugin '{folder}' already {state} — no change")
            return {"folderName": folder, "isEnabled": enabled, "changed": False}

        result = self._patch_plugins([{"folderName": folder, "isEnabled": enabled}])
        if "error" in result:
            return result

        action = "Enabled" if enabled else "Disabled"
        self.logger.info(f"{action} plugin '{folder}'")
        return {"folderName": folder, "isEnabled": enabled, "changed": True}

    def _set_plugins_state(self, plugins: str | list[str], enabled: bool, bulk: bool) -> dict[str, Any]:
        """Shared implementation for enable_plugins / disable_plugins."""
        if isinstance(plugins, str):
            plugins = [plugins]

        all_plugins = self.get_all_plugins()
        if all_plugins and "error" in all_plugins[0]:
            return all_plugins[0]

        already_key = "already_enabled" if enabled else "already_disabled"
        result: dict[str, Any] = {"changed": [], already_key: [], "not_found": [], "errors": []}

        pending_updates: list[dict[str, Any]] = []

        for name in plugins:
            match = self._find_plugin(all_plugins, name)
            if match is None:
                self.logger.error(f"Plugin '{name}' not found")
                result["not_found"].append(name)
                continue

            folder = match["folderName"]
            if match.get("isEnabled") == enabled:
                self.logger.debug(f"Plugin '{folder}' already in target state — skipped")
                result[already_key].append(folder)
            else:
                pending_updates.append({"folderName": folder, "isEnabled": enabled})

        if not pending_updates:
            self.logger.info(f"No plugins needed state change (target enabled={enabled})")
            return result

        action = "Enabling" if enabled else "Disabling"

        if bulk:
            self.logger.debug(f"{action} {len(pending_updates)} plugin(s) in a single bulk PATCH")
            patch_result = self._patch_plugins(pending_updates)
            if "error" in patch_result:
                return patch_result
            result["changed"] = [u["folderName"] for u in pending_updates]
        else:
            for update in pending_updates:
                folder = update["folderName"]
                self.logger.debug(f"{action} plugin '{folder}'")
                patch_result = self._patch_plugins([update])
                if "error" in patch_result:
                    self.logger.error(f"Failed to update plugin '{folder}': {patch_result['error']}")
                    result["errors"].append(folder)
                else:
                    result["changed"].append(folder)

        done_action = "Enabled" if enabled else "Disabled"
        self.logger.info(
            f"{done_action} {len(result['changed'])} plugin(s). "
            f"Skipped: {len(result[already_key])}. "
            f"Not found: {len(result['not_found'])}. "
            f"Errors: {len(result['errors'])}."
        )
        return result
