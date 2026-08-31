from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


class AdminMixin:
    def _get_user_email_and_group_name_maps(self) -> dict[str, Any]:
        """Fetch all users and groups and build ID-to-name lookup maps.

        Internal helper: resolves share entries (dashboard or data model),
        which reference users and groups only by ID, into readable emails and
        group names. Shared by ``get_all_dashboard_shares`` here and by
        ``Dashboard.get_dashboard_share`` (via ``self.access_mgmt``). Use
        ``get_users_all`` and ``get_groups`` for the public equivalents.

        Returns
        -------
        dict[str, Any]
            ``{"users_by_id": {user_id: email, ...}, "groups_by_id": {group_id: name, ...}}``
            on success, or ``{"error": "..."}`` if either API call fails.
        """
        users_response = self.api_client.get("/api/v1/users")
        if users_response is None or users_response.status_code != 200:
            failure = _extract_error_message(users_response, "Failed to fetch users", self.api_client)
            self.logger.error(failure["error"])
            return failure

        users_data = users_response.json()
        users_by_id = {user["_id"]: user.get("email", "Unknown Email") for user in users_data}

        groups_response = self.api_client.get("/api/v1/groups")
        if groups_response is None or groups_response.status_code != 200:
            failure = _extract_error_message(groups_response, "Failed to fetch groups", self.api_client)
            self.logger.error(failure["error"])
            return failure

        groups_data = groups_response.json()
        groups_by_id = {group["_id"]: group.get("name", "Unknown Group") for group in groups_data}

        return {"users_by_id": users_by_id, "groups_by_id": groups_by_id}

    def _fetch_all_dashboards_paginated(self) -> list[dict[str, Any]]:
        """Fetch every dashboard via the paginated dashboard-search endpoint.

        Shared by ``get_all_dashboard_shares`` and
        ``change_folder_and_dashboard_ownership``'s access-grant fallback.

        Returns
        -------
        list[dict[str, Any]]
            All dashboard objects across every page. If a page request fails
            or returns no response, whatever was retrieved so far is
            returned instead of raising.
        """
        limit = 50
        skip = 0
        dashboards: list[dict[str, Any]] = []

        while True:
            self.logger.debug(f"Fetching dashboards with limit={limit}, skip={skip}")
            dashboard_response = self.api_client.post(
                "/api/v1/dashboards/searches",
                data={"queryParams": {"ownershipType": "allRoot", "search": "", "ownerInfo": True, "asObject": True}, "queryOptions": {"sort": {"title": 1}, "limit": limit, "skip": skip}},
            )

            if not dashboard_response or dashboard_response.status_code != 200:
                self.logger.error("Failed to fetch dashboards.")
                break

            response_data = dashboard_response.json()
            items = response_data.get("items", [])
            if not items:
                self.logger.info("No more dashboards found.")
                break

            dashboards.extend(items)
            skip += limit
            self.logger.debug(f"Retrieved {len(items)} dashboards, total so far: {len(dashboards)}")

        return dashboards

    def get_all_dashboard_shares(self) -> list[dict[str, Any]]:
        """Retrieve all dashboard shares, including user and group details for each shared dashboard.

        Uses pagination to retrieve all dashboards and their share information,
        and collects the corresponding user and group details for each share.

        Returns
        -------
        list[dict[str, Any]]
            A list of dictionaries containing the dashboard title, share type
            (``user`` or ``group``), and share name (email or group name) — one
            row per share, so the row count equals the number of shares.
            Dashboards with no shares contribute no rows. An empty list is
            returned if users or groups cannot be fetched.
        """
        self.logger.info("Starting to retrieve dashboard shares...")

        # Step 1: Fetch all dashboards with pagination
        dashboards = self._fetch_all_dashboards_paginated()

        # Step 2: Fetch user/group ID-to-name lookup maps
        self.logger.info("Fetching users and groups.")
        maps = self._get_user_email_and_group_name_maps()
        if "error" in maps:
            return []

        users_by_id = maps["users_by_id"]
        groups_by_id = maps["groups_by_id"]

        shared_list = []

        # Step 3: Parse the dashboards to find shared users and groups
        self.logger.debug(f"Parsing {len(dashboards)} dashboards for shared users and groups.")
        for dashboard in dashboards:
            if dashboard.get("shares"):
                for share in dashboard["shares"]:
                    share_info = {"dashboard": dashboard["title"], "type": None, "name": None}

                    if share["type"] == "user" and share["shareId"] in users_by_id:
                        share_info["type"] = "user"
                        share_info["name"] = users_by_id[share["shareId"]]
                    elif share["type"] == "group" and share["shareId"] in groups_by_id:
                        share_info["type"] = "group"
                        share_info["name"] = groups_by_id[share["shareId"]]

                    shared_list.append(share_info)
            # Dashboards with no shares contribute no rows — a placeholder row
            # would read as one share to any consumer that counts results.

        self.logger.info(f"Parsed {len(shared_list)} shared dashboards.")

        # Return the result as a list of dictionaries
        return shared_list

    def create_schedule_build(
        self,
        datamodel_name: str,
        build_type: str = "ACCUMULATE",
        *,
        days: list[str] | None = None,
        hour: int | None = None,
        minute: int | None = None,
        interval_days: int | None = None,
        interval_hours: int | None = None,
        interval_minutes: int | None = None,
    ) -> dict[str, Any]:
        """Create a schedule build for a DataModel.

        Supports both cron-based schedules (e.g. every Monday at 9:00 UTC) and
        interval-based schedules (e.g. every 2 days, 1 hour, 30 minutes). An
        interval-based schedule is created when any of the ``interval_*``
        parameters are provided; otherwise a cron-based schedule is created from
        ``days``, ``hour``, and ``minute``.

        Parameters
        ----------
        datamodel_name : str
            The name of the DataModel.
        build_type : str, optional
            Type of the build, one of ``"ACCUMULATE"``, ``"FULL"``, or
            ``"SCHEMA_CHANGES"``. Defaults to ``"ACCUMULATE"``.
        days : list[str] | None, optional
            Keyword-only. List of days for a cron schedule, e.g.
            ``["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]`` or ``["*"]``
            for all days.
        hour : int | None, optional
            Keyword-only. Hour in 24-hour format (UTC) for a cron schedule.
        minute : int | None, optional
            Keyword-only. Minute of the hour (UTC) for a cron schedule.
        interval_days : int | None, optional
            Keyword-only. Interval in days for an interval-based schedule.
        interval_hours : int | None, optional
            Keyword-only. Interval in hours for an interval-based schedule.
        interval_minutes : int | None, optional
            Keyword-only. Interval in minutes for an interval-based schedule.

        Returns
        -------
        dict[str, Any]
            The API response on success, or ``{"error": "..."}`` on failure or
            invalid schedule configuration.
        """
        self.logger.debug(f"Fetching DataModel ID for '{datamodel_name}'")
        schema_url = f"/api/v2/datamodels/schema?title={datamodel_name}"
        response = self.api_client.get(schema_url)

        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, f"Failed to fetch DataModel schema for '{datamodel_name}'", self.api_client)
            self.logger.error(failure["error"])
            return failure

        response_data = response.json()
        if not response_data:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"ok": False, "error": f"DataModel '{datamodel_name}' not found"}

        # Extract DataModel ID
        datamodel_id = response_data.get("oid")
        self.logger.info(f"DataModel ID for '{datamodel_name}' is {datamodel_id}")

        # Determine schedule type
        if interval_days is not None or interval_hours is not None or interval_minutes is not None:
            self.logger.info("Creating interval-based schedule...")
            days = interval_days or 0
            hours = interval_hours or 0
            minutes = interval_minutes or 0
            interval_seconds = (days * 86400) + (hours * 3600) + (minutes * 60)

            if interval_seconds <= 0:
                self.logger.error("Interval must be greater than 0 seconds.")
                return {"ok": False, "error": "Interval must be greater than 0 seconds."}

            schedule_payload = {"scheduleType": "Interval", "buildType": build_type, "intervalSeconds": interval_seconds}
        elif days and hour is not None and minute is not None:
            self.logger.info("Creating cron-based schedule...")
            if days == ["*"]:
                days_string = "0,1,2,3,4,5,6"
            else:
                day_mapping = {"SUN": "0", "MON": "1", "TUE": "2", "WED": "3", "THU": "4", "FRI": "5", "SAT": "6"}
                days_string = ",".join([day_mapping[day] for day in days])

            cron_string = f"{minute} {hour} * * {days_string}"
            self.logger.debug(f"Generated cron string: {cron_string}")

            schedule_payload = {"cronString": cron_string, "buildType": build_type, "daysOfWeek": days, "hour": hour, "minute": minute}
        else:
            self.logger.error("Invalid schedule configuration: Provide either interval or full cron config.")
            return {"ok": False, "error": "Invalid schedule configuration: Provide either interval or full cron config."}

        self.logger.info("Creating schedule build with the following details:")
        self.logger.debug(schedule_payload)

        api_url = f"/api/v2/datamodels/{datamodel_id}/schedule"
        response = self.api_client.post(api_url, data=schedule_payload)

        if not response or response.status_code not in [200, 201]:
            self.logger.error("Failed to create schedule build. Response: %s", getattr(response, "text", "No response text"))
            return {"ok": False, "error": "Failed to create schedule build."}

        try:
            response_data = response.json()
            self.logger.info(f"Schedule build created successfully. Response: {response_data}")
            return response_data
        except (AttributeError, ValueError):
            self.logger.warning("Response does not contain valid JSON. Returning raw response.")
            return {"message": "Schedule build created successfully", "raw_response": getattr(response, "text", "No response text")}
