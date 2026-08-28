from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class TenantsMixin:
    def get_tenants(self) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all tenants.

        Fetches the full list of tenants defined on the Sisense server. Only
        meaningful on multi-tenant deployments.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A list of raw tenant objects, or ``{"error": "..."}`` if
            retrieval fails (for example, on a single-tenant deployment
            where the tenants endpoint is unavailable).
        """
        self.logger.debug("Starting 'get_tenants' method.")

        response = self.api_client.get("/api/v1/tenants")

        if response is None or not response.ok:
            failure = _extract_error_message(response, "Failed to retrieve tenants", self.api_client)
            self.logger.error(failure["error"])
            return failure

        try:
            tenants = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse tenants response JSON.")
            return {"error": f"Failed to parse tenants response JSON: {str(e)}"}

        self.logger.debug(f"Retrieved {len(tenants or [])} tenant(s).")
        return tenants or []
