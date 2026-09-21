from __future__ import annotations

from typing import Any

from ..utils import _dashboard_write_access, _extract_error_message, _finish_ownership, _with_query, _write_dashboard_copies

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
        act_as_owner: bool = False,
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

        With Dashboard Co-Authoring on, a published dashboard's widget lives on its
        shared copy (what viewers see) and on the owner's private copy; both are
        written, shared first with ``sharedMode=true``, and the dashboard is
        republished. Only the owner may write; a non-owner is refused with the owner
        named, unless ``act_as_owner`` is true and the token belongs to an
        administrator, in which case ownership is borrowed for the change and
        returned afterwards together with the exact share list. ``executing_user_id``
        is the older form of the same thing: when provided, ownership is borrowed for
        that user (a Sisense user ID) instead of the token's user.

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
            Deprecated in favour of ``act_as_owner``: the Sisense user ID to transfer
            ownership to for the change. Default ``None``.
        act_as_owner : bool, optional
            Take ownership temporarily when the token's user is an administrator but not
            the owner. Default ``False``: refuse instead.

        Returns
        -------
        dict[str, Any]
            The style objects after the update:

            - ``currentCard`` (dict): The value of ``style.currentCard`` after the write.
            - ``currentConfig`` (dict): The value of ``style.currentConfig`` after the write.

            ``published`` (and ``publish_error``) is added when a shared copy was written,
            ``ownership_transferred_temporarily`` / ``original_owner`` when ownership was
            borrowed. On failure, or if the widget is not a BloX type, the standard
            ``{"ok": False, "error": "...", ...}`` dict, with ``owner`` and ``co_owners``
            when the token's user is not the owner.
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

        access = _dashboard_write_access(self.api_client, self.logger, dashboard_id, act_as_owner or bool(executing_user_id), borrower=executing_user_id or None, what="modify its widgets")
        if access.get("ok") is False:
            return access
        title, co_authoring, borrowed = access["title"], access["co_authoring"], access["borrowed"]
        result: dict[str, Any] = {"ok": False, "error": f"Updating BloX widget '{widget_id}' failed unexpectedly."}
        try:
            outcome = _write_dashboard_copies(
                self.api_client,
                self.logger,
                dashboard_id,
                title,
                co_authoring,
                lambda query: self.api_client.put(_with_query(f"/api/dashboards/{dashboard_id}/widgets/{widget_id}", query), data=payload),
                label=f"update BloX widget '{widget_id}'",
            )
            if outcome.get("ok") is False:
                result = outcome
            else:
                self.logger.info(f"BloX widget {widget_id} style updated on dashboard {dashboard_id}.")
                result = {"currentCard": style_block.get("currentCard", {}), "currentConfig": style_block.get("currentConfig", {})}
                if outcome.get("published") is not None:
                    result["published"] = outcome["published"]
                    if outcome.get("publish_error"):
                        result["publish_error"] = outcome["publish_error"]
        finally:
            result = _finish_ownership(self.api_client, self.logger, dashboard_id, title, borrowed, result)
        return result

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

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to fetch widget '{widget_id}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        widget = response.json()

        if widget.get("type") != _BLOX_WIDGET_TYPE:
            msg = f"Widget '{widget_id}' is of type '{widget.get('type')}', not '{_BLOX_WIDGET_TYPE}'."
            self.logger.error(msg)
            return {"ok": False, "error": msg}

        return widget
