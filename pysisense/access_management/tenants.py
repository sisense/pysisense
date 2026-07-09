from __future__ import annotations

from typing import Any


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

        if not response or not response.ok:
            status_code = response.status_code if response else "No response"
            self.logger.error(f"Failed to retrieve tenants. Status Code: {status_code}")
            return {"error": "Failed to retrieve tenants."}

        try:
            tenants = response.json()
        except Exception as e:
            self.logger.exception("Failed to parse tenants response JSON.")
            return {"error": f"Failed to parse tenants response JSON: {str(e)}"}

        self.logger.debug(f"Retrieved {len(tenants or [])} tenant(s).")
        return tenants or []
