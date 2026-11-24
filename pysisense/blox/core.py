from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class BloxCoreMixin:
    def get_blox_actions(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all custom Blox actions from the Sisense instance.

        The endpoint used varies by the ``operating_system`` set on the
        ``SisenseClient``:

        - ``"linux"``: ``GET /api/v1/blox/getCustomActions``
        - ``"windows"``: ``GET /api/v1/getCustomActions/actions``

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            The Blox action objects (an empty list when the instance has none), or the
            standard ``{"ok": False, "error": "...", ...}`` dict on failure.
        """
        os = self.api_client.operating_system
        endpoint = "/api/v1/getCustomActions/actions" if os == "windows" else "/api/v1/blox/getCustomActions"
        self.logger.debug(f"Fetching all custom Blox actions (os={os})")
        response = self.api_client.get(endpoint)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to fetch Blox actions", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            actions = response.json()
        except Exception:
            msg = "Failed to parse Blox actions response as JSON"
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        if not isinstance(actions, list):
            msg = f"Unexpected response shape from Blox actions endpoint — got {type(actions).__name__}"
            self.logger.error(msg)
            return {"ok": False, "error": msg}

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
            The API response body on success (``{"success": True}`` when it is empty), or the
            standard ``{"ok": False, "error": "...", ...}`` dict on failure.
        """
        if self.api_client.operating_system == "windows":
            msg = "save_blox_action is not supported on Windows deployments."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        endpoint = "/api/v1/blox/saveCustomAction"
        action_type = action.get("type", "<unnamed>")
        self.logger.debug(f"Saving Blox action '{action_type}'")
        response = self.api_client.post(endpoint, data=action)

        if response is None or response.status_code not in (200, 201):
            failure = _extract_error_message(response, f"Failed to save Blox action '{action_type}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Saved Blox action '{action_type}'")
        return _json_or_success(response)

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
            The API response body on success (``{"success": True}`` when it is empty), or the
            standard ``{"ok": False, "error": "...", ...}`` dict on failure.
        """
        if self.api_client.operating_system == "windows":
            msg = "delete_blox_action is not supported on Windows deployments."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        endpoint = "/api/v1/blox/deleteCustomAction"
        self.logger.debug(f"Deleting Blox action '{action_type}'")
        response = self.api_client.post(endpoint, data={"type": action_type})

        if response is None or response.status_code not in (200, 201):
            failure = _extract_error_message(response, f"Failed to delete Blox action '{action_type}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        self.logger.info(f"Deleted Blox action '{action_type}'")
        return _json_or_success(response)


def _json_or_success(response: Any) -> dict[str, Any]:
    """The JSON body of a successful write, or ``{"success": True}`` when it is empty or not JSON."""
    if not response.content:
        return {"success": True}
    try:
        body = response.json()
    except ValueError:
        return {"success": True}
    return body if isinstance(body, dict) else {"success": True, "response": body}
