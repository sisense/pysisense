from __future__ import annotations

from typing import Any

# Fields that Sisense manages server-side and must be stripped before a PUT write.
_SERVER_MANAGED_FIELDS = frozenset({"oid", "_id", "owner", "userId", "created", "lastUpdated", "instanceType", "dashboardid"})
_BLOX_WIDGET_TYPE = "BloX"


class BloxWidgetsMixin:
    def get_blox_widget_style(
        self,
        dashboard_id: str,
        widget_id: str,
        *,
        admin_access: bool = True,
    ) -> dict[str, Any]:
        """Retrieve the style objects of a BloX widget.

        Fetches the widget and returns its ``style.currentCard`` and
        ``style.currentConfig`` objects. ``currentCard`` holds the BloX card
        definition (body, actions, the ``style`` CSS string, and so on) and
        ``currentConfig`` holds the widget configuration (``fontFamily``,
        ``fontSizes``, and so on). Returns ``{"error": "..."}`` if the widget
        is not a BloX type.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard that contains the widget.
        widget_id : str
            The ``oid`` of the BloX widget.
        admin_access : bool, optional
            When ``True`` (default), appends ``?adminAccess=true`` to the request,
            allowing access to dashboards the API token user does not own.

        Returns
        -------
        dict[str, Any]
            A dictionary with:

            - ``currentCard`` (dict): The widget's ``style.currentCard`` object.
            - ``currentConfig`` (dict): The widget's ``style.currentConfig`` object.

            Returns ``{"error": "..."}`` on failure or if the widget type is not ``"BloX"``.
        """
        widget = self._get_blox_widget(dashboard_id, widget_id, admin_access=admin_access)
        if "error" in widget:
            return widget

        style_block = widget.get("style", {})

        self.logger.info(f"Retrieved BloX style for widget {widget_id} on dashboard {dashboard_id}.")
        return {"currentCard": style_block.get("currentCard", {}), "currentConfig": style_block.get("currentConfig", {})}

    def update_blox_widget_style(
        self,
        dashboard_id: str,
        widget_id: str,
        *,
        current_card: dict[str, Any] | None = None,
        current_config: dict[str, Any] | None = None,
        executing_user_id: str | None = None,
    ) -> dict[str, Any]:
        """Update the style objects of a BloX widget.

        Reads the current widget, replaces its ``style.currentCard`` and/or
        ``style.currentConfig`` objects with the provided values, and writes
        the result back via ``PUT /api/dashboards/{dashboard_id}/widgets/{widget_id}``.
        Server-managed fields are stripped before the write.

        The typical flow is read-modify-write: fetch the objects with
        :meth:`get_blox_widget_style`, change the fields you need (for example
        the ``style`` CSS string on the card, or ``fontFamily`` on the config),
        and pass the modified objects back here. Each provided object replaces
        the existing one wholesale; omitted objects are left unchanged.

        When ``executing_user_id`` is provided, ownership of the dashboard is
        temporarily transferred to that user before the write, then restored
        in a ``finally`` block regardless of whether the write succeeds. Pass
        the Sisense user ID (not email); resolve it with
        ``AccessManagement.get_my_user()`` for the API token user, or
        ``AccessManagement.get_user(email)`` for any other user.

        When neither ``current_card`` nor ``current_config`` is provided the
        method returns immediately with the current style objects and makes
        no write.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard that contains the widget.
        widget_id : str
            The ``oid`` of the BloX widget to update.
        current_card : dict[str, Any] | None, optional
            Replacement for the ``style.currentCard`` object. Omit to leave
            the current value unchanged.
        current_config : dict[str, Any] | None, optional
            Replacement for the ``style.currentConfig`` object. Omit to leave
            the current value unchanged.
        executing_user_id : str | None, optional
            Sisense user ID of the user who should own the dashboard during the
            write. Required when the API token user is not the dashboard owner.

        Returns
        -------
        dict[str, Any]
            The style objects after the update:

            - ``currentCard`` (dict): The value of ``style.currentCard`` after the write.
            - ``currentConfig`` (dict): The value of ``style.currentConfig`` after the write.

            Returns ``{"error": "..."}`` on failure or if the widget is not a BloX type.
        """
        widget = self._get_blox_widget(dashboard_id, widget_id, admin_access=True)
        if "error" in widget:
            return widget

        style_block = widget.setdefault("style", {})

        if current_card is None and current_config is None:
            self.logger.info(f"No style objects provided for widget {widget_id} — nothing to update.")
            return {"currentCard": style_block.get("currentCard", {}), "currentConfig": style_block.get("currentConfig", {})}

        if current_card is not None:
            style_block["currentCard"] = current_card
        if current_config is not None:
            style_block["currentConfig"] = current_config

        payload = {k: v for k, v in widget.items() if k not in _SERVER_MANAGED_FIELDS}

        took_ownership = False
        original_owner_id: str | None = None
        original_shares: list[dict[str, Any]] = []

        if executing_user_id:
            take_result = self._blox_take_ownership(dashboard_id, executing_user_id)
            if isinstance(take_result, str):
                return {"error": take_result}
            original_owner_id, original_shares = take_result
            took_ownership = True

        try:
            response = self.api_client.put(f"/api/dashboards/{dashboard_id}/widgets/{widget_id}", data=payload)

            if response is None:
                self.logger.error(f"No response received when updating BloX widget {widget_id}.")
                return {"error": f"No response received when updating widget '{widget_id}'."}

            if response.status_code != 200:
                try:
                    error_detail = response.json()
                except Exception:
                    error_detail = response.text
                self.logger.error(f"Failed to update BloX widget {widget_id} (HTTP {response.status_code}): {error_detail}")
                return {"error": f"Failed to update widget '{widget_id}': {error_detail}"}

            self.logger.info(f"BloX widget {widget_id} style updated on dashboard {dashboard_id}.")
            return {"currentCard": style_block.get("currentCard", {}), "currentConfig": style_block.get("currentConfig", {})}

        finally:
            if took_ownership:
                self._blox_release_ownership(dashboard_id, original_owner_id, original_shares)

    def _get_blox_widget(self, dashboard_id: str, widget_id: str, *, admin_access: bool = True) -> dict[str, Any]:
        """Fetch a widget and verify it is a BloX widget.

        Parameters
        ----------
        dashboard_id : str
            The ``oid`` of the dashboard that contains the widget.
        widget_id : str
            The ``oid`` of the widget to fetch.
        admin_access : bool, optional
            When ``True`` (default), appends ``?adminAccess=true`` to the request.

        Returns
        -------
        dict[str, Any]
            The full widget object, or ``{"error": "..."}`` on failure or if
            the widget type is not ``"BloX"``.
        """
        endpoint = f"/api/v1/dashboards/{dashboard_id}/widgets/{widget_id}"
        if admin_access:
            endpoint += "?adminAccess=true"

        self.logger.debug(f"Fetching BloX widget {widget_id} on dashboard {dashboard_id}.")
        response = self.api_client.get(endpoint)

        if response is None:
            self.logger.error(f"No response received for widget {widget_id}.")
            return {"error": f"No response received for widget '{widget_id}'."}

        if response.status_code != 200:
            try:
                error_detail = response.json()
            except Exception:
                error_detail = response.text
            self.logger.error(f"Failed to fetch widget {widget_id} (HTTP {response.status_code}): {error_detail}")
            return {"error": f"Failed to fetch widget '{widget_id}': {error_detail}"}

        widget = response.json()

        if widget.get("type") != _BLOX_WIDGET_TYPE:
            msg = f"Widget '{widget_id}' is of type '{widget.get('type')}', not '{_BLOX_WIDGET_TYPE}'."
            self.logger.error(msg)
            return {"error": msg}

        return widget

    def _blox_take_ownership(self, dashboard_id: str, executing_user_id: str) -> tuple[str, list[dict[str, Any]]] | str:
        """Take temporary ownership of a dashboard for the specified user.

        Fetches the current owner and shares, then transfers ownership to
        ``executing_user_id``. The caller must call :meth:`_blox_release_ownership`
        in a ``finally`` block to restore the original state.

        Parameters
        ----------
        dashboard_id : str
            The dashboard to take ownership of.
        executing_user_id : str
            Sisense user ID of the user to transfer ownership to.

        Returns
        -------
        tuple[str, list[dict[str, Any]]] | str
            On success: ``(original_owner_id, original_shares_list)``.
            On failure: an error message string.
        """
        self.logger.debug(f"Taking temporary ownership of dashboard {dashboard_id} for user {executing_user_id}.")

        owner_response = self.api_client.get(f"/api/v1/dashboards/admin?dashboardType=owner&id={dashboard_id}&asObject=false")
        if owner_response is None or owner_response.status_code != 200:
            self.logger.error(f"Failed to retrieve original owner for dashboard {dashboard_id}.")
            return f"Failed to retrieve original owner for dashboard '{dashboard_id}'."

        owner_data = owner_response.json()
        if not owner_data:
            self.logger.error(f"Dashboard {dashboard_id} not found in the admin dashboard list.")
            return f"Dashboard '{dashboard_id}' not found."
        original_owner_id = owner_data[0].get("owner")

        shares_response = self.api_client.get(f"/api/shares/dashboard/{dashboard_id}?adminAccess=true")
        if shares_response is None or shares_response.status_code != 200:
            error_message = shares_response.json() if shares_response else "No response received."
            self.logger.error(f"Failed to retrieve shares for dashboard {dashboard_id}. Error: {error_message}")
            return f"Failed to retrieve shares for dashboard '{dashboard_id}'."

        original_shares = shares_response.json().get("sharesTo", [])

        change_response = self.api_client.post(
            f"/api/v1/dashboards/{dashboard_id}/change_owner?adminAccess=true",
            data={"ownerId": executing_user_id, "originalOwnerRule": "edit"},
        )
        if change_response is None or change_response.status_code != 200:
            error_message = change_response.json() if change_response else "No response received."
            self.logger.error(f"Failed to change ownership of dashboard {dashboard_id}. Error: {error_message}")
            return f"Failed to change ownership of dashboard '{dashboard_id}'."

        self.logger.info(f"Dashboard {dashboard_id} ownership temporarily transferred to {executing_user_id}.")
        return (original_owner_id, original_shares)

    def _blox_release_ownership(self, dashboard_id: str, original_owner_id: str, shares: list[dict[str, Any]]) -> None:
        """Restore original ownership and shares for a dashboard.

        Intended to be called in a ``finally`` block after :meth:`_blox_take_ownership`.
        Restoration errors are logged but not raised so the caller's return value
        is not overridden.

        Parameters
        ----------
        dashboard_id : str
            The dashboard to restore ownership for.
        original_owner_id : str
            The user ID of the original owner.
        shares : list[dict[str, Any]]
            The original shares list as returned by :meth:`_blox_take_ownership`.
        """
        self.logger.info(f"Restoring ownership of dashboard {dashboard_id} to '{original_owner_id}'.")

        shares_payload = [{"shareId": s["shareId"], "type": s["type"], "rule": s.get("rule", "edit"), "subscribe": s.get("subscribe", False)} for s in shares]

        restore_shares_response = self.api_client.post(f"/api/shares/dashboard/{dashboard_id}", data={"sharesTo": shares_payload})
        if restore_shares_response is None or restore_shares_response.status_code != 200:
            error_message = restore_shares_response.json() if restore_shares_response else "No response received."
            self.logger.error(f"Failed to restore shares for dashboard {dashboard_id}. Error: {error_message}")

        restore_ownership_response = self.api_client.post(
            f"/api/v1/dashboards/{dashboard_id}/change_owner",
            data={"ownerId": original_owner_id, "originalOwnerRule": "edit"},
        )
        if restore_ownership_response is None or restore_ownership_response.status_code != 200:
            error_message = restore_ownership_response.json() if restore_ownership_response else "No response received."
            self.logger.error(f"Failed to restore ownership of dashboard {dashboard_id}. Error: {error_message}")
        else:
            self.logger.info(f"Ownership of dashboard {dashboard_id} restored to '{original_owner_id}'.")
