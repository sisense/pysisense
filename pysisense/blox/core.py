from __future__ import annotations

from typing import Any


class BloxCoreMixin:
    def get_blox_actions(self) -> list[dict[str, Any]]:
        """Retrieve all custom Blox actions from the Sisense instance.

        The endpoint used varies by the ``operating_system`` set on the
        ``SisenseClient``:

        - ``"linux"``: ``GET /api/v1/blox/getCustomActions``
        - ``"windows"``: ``GET /api/v1/getCustomActions/actions``

        Returns
        -------
        list[dict[str, Any]]
            A list of Blox action objects, or ``[{"error": "..."}]`` on failure.
        """
        os = self.api_client.operating_system
        endpoint = "/api/v1/getCustomActions/actions" if os == "windows" else "/api/v1/blox/getCustomActions"
        self.logger.debug(f"Fetching all custom Blox actions (os={os})")
        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            status = response.status_code if response is not None else "no response"
            msg = f"Failed to fetch Blox actions — status {status}"
            self.logger.error(msg)
            return [{"error": msg}]

        try:
            actions = response.json()
        except Exception:
            msg = "Failed to parse Blox actions response as JSON"
            self.logger.error(msg)
            return [{"error": msg}]

        if not isinstance(actions, list):
            msg = f"Unexpected response shape from Blox actions endpoint — got {type(actions).__name__}"
            self.logger.error(msg)
            return [{"error": msg}]

        self.logger.info(f"Retrieved {len(actions)} Blox action(s)")
        return actions

    def save_blox_action(self, action: dict[str, Any]) -> dict[str, Any]:
        """Save a custom Blox action on the Sisense instance.

        Sends the action payload to ``POST /api/v1/blox/saveCustomAction``.
        This endpoint is supported on Linux deployments only.

        Parameters
        ----------
        action : dict[str, Any]
            A Blox action object.

        Returns
        -------
        dict[str, Any]
            The API response body on success, or ``{"error": "..."}`` on failure.
        """
        if self.api_client.operating_system == "windows":
            msg = "save_blox_action is not supported on Windows deployments."
            self.logger.error(msg)
            return {"error": msg}

        endpoint = "/api/v1/blox/saveCustomAction"
        action_type = action.get("type", "<unnamed>")
        self.logger.debug(f"Saving Blox action '{action_type}'")
        response = self.api_client.post(endpoint, data=action)

        if response is None or response.status_code not in (200, 201):
            status = response.status_code if response is not None else "no response"
            msg = f"Failed to save Blox action '{action_type}' — status {status}"
            self.logger.error(msg)
            return {"error": msg}

        self.logger.info(f"Saved Blox action '{action_type}'")
        return response.json() if response.content else {"success": True}

    def delete_blox_action(self, action_type: str) -> dict[str, Any]:
        """Delete a custom Blox action from the Sisense instance.

        Sends the action type to ``POST /api/v1/blox/deleteCustomAction``.
        This endpoint is supported on Linux deployments only.

        Parameters
        ----------
        action_type : str
            The ``type`` identifier of the Blox action to delete.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success, or ``{"error": "..."}`` on failure.
        """
        if self.api_client.operating_system == "windows":
            msg = "delete_blox_action is not supported on Windows deployments."
            self.logger.error(msg)
            return {"error": msg}

        endpoint = "/api/v1/blox/deleteCustomAction"
        self.logger.debug(f"Deleting Blox action '{action_type}'")
        response = self.api_client.post(endpoint, data={"type": action_type})

        if response is None or response.status_code not in (200, 201):
            status = response.status_code if response is not None else "no response"
            msg = f"Failed to delete Blox action '{action_type}' — status {status}"
            self.logger.error(msg)
            return {"error": msg}

        self.logger.info(f"Deleted Blox action '{action_type}'")
        return response.json() if response.content else {"success": True}
