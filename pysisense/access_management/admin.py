from __future__ import annotations


class AdminMixin:
    def get_all_dashboard_shares(self):
        """
        Method to retrieve all dashboard shares, including user and group details for each shared dashboard.

        This method uses pagination to retrieve all dashboards and their share information, and it collects
        corresponding user and group details for each share.

        Returns:
            list: A list of dictionaries containing the dashboard title, share type (user or group),
            and share name (email or group name).
        """
        limit = 50
        skip = 0
        dashboards = []

        self.logger.info("Starting to retrieve dashboard shares...")

        # Step 1: Fetch all dashboards with pagination
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

        # Step 2: Fetch all users
        self.logger.info("Fetching all users.")
        users_response = self.api_client.get("/api/v1/users")
        if not users_response or users_response.status_code != 200:
            self.logger.error("Failed to fetch users.")
            return []

        users_data = users_response.json()
        users_detail = [{"id": user["_id"], "email": user.get("email", "Unknown Email")} for user in users_data]

        # Step 3: Fetch all groups
        self.logger.info("Fetching all groups.")
        groups_response = self.api_client.get("/api/v1/groups")
        if not groups_response or groups_response.status_code != 200:
            self.logger.error("Failed to fetch groups.")
            return []

        groups_data = groups_response.json()
        groups_detail = [{"id": group["_id"], "name": group.get("name", "Unknown Group")} for group in groups_data]

        shared_list = []

        # Step 4: Parse the dashboards to find shared users and groups
        self.logger.debug(f"Parsing {len(dashboards)} dashboards for shared users and groups.")
        for dashboard in dashboards:
            if dashboard.get("shares"):
                for share in dashboard["shares"]:
                    share_info = {"dashboard": dashboard["title"], "type": None, "name": None}

                    if share["type"] == "user":
                        user = next((user for user in users_detail if user["id"] == share["shareId"]), None)
                        if user:
                            share_info["type"] = "user"
                            share_info["name"] = user["email"]
                    elif share["type"] == "group":
                        group = next((group for group in groups_detail if group["id"] == share["shareId"]), None)
                        if group:
                            share_info["type"] = "group"
                            share_info["name"] = group["name"]

                    shared_list.append(share_info)
            else:
                # Add placeholder if there are no shares for the dashboard
                shared_list.append({"dashboard": dashboard["title"], "type": None, "name": None})

        self.logger.info(f"Parsed {len(shared_list)} shared dashboards.")

        # Return the result as a list of dictionaries
        return shared_list

    def create_schedule_build(self, datamodel_name, build_type="ACCUMULATE", *, days=None, hour=None, minute=None, interval_days=None, interval_hours=None, interval_minutes=None):
        """
        Method to create a schedule build for a DataModel.

        Supports both cron-based schedules (e.g., every Monday at 9:00 UTC)
        and interval-based schedules (e.g., every 2 days, 1 hour, 30 minutes).

        Parameters:
            datamodel_name (str): The name of the DataModel.
            build_type (str): Optional. Type of the build (e.g., "ACCUMULATE", "FULL",
            "SCHEMA_CHANGES"). Defaults to "ACCUMULATE".
            days (list, optional): List of days for cron schedule. Eg.: ["SUN", "MON", "TUE", "WED", "THU", "FRI",
            "SAT"] or ["*"] for all days.
            hour (int, optional): Hour in 24-hour format (UTC).
            minute (int, optional): Minute of the hour (UTC).
            interval_days (int, optional): Interval in days.
            interval_hours (int, optional): Interval in hours.
            interval_minutes (int, optional): Interval in minutes.

        Returns:
            dict: API response or error.
        """
        self.logger.debug(f"Fetching DataModel ID for '{datamodel_name}'")
        schema_url = f"/api/v2/datamodels/schema?title={datamodel_name}"
        response = self.api_client.get(schema_url)

        if not response or response.status_code != 200:
            self.logger.error(f"Failed to fetch DataModel schema for '{datamodel_name}'")
            return {"error": f"Failed to fetch DataModel schema for '{datamodel_name}'"}

        response_data = response.json()
        if not response_data:
            self.logger.error(f"DataModel '{datamodel_name}' not found.")
            return {"error": f"DataModel '{datamodel_name}' not found"}

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
                return {"error": "Interval must be greater than 0 seconds."}

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
            return {"error": "Invalid schedule configuration: Provide either interval or full cron config."}

        self.logger.info("Creating schedule build with the following details:")
        self.logger.debug(schedule_payload)

        api_url = f"/api/v2/datamodels/{datamodel_id}/schedule"
        response = self.api_client.post(api_url, data=schedule_payload)

        if not response or response.status_code not in [200, 201]:
            self.logger.error("Failed to create schedule build. Response: %s", getattr(response, "text", "No response text"))
            return {"error": "Failed to create schedule build."}

        try:
            response_data = response.json()
            self.logger.info(f"Schedule build created successfully. Response: {response_data}")
            return response_data
        except (AttributeError, ValueError):
            self.logger.warning("Response does not contain valid JSON. Returning raw response.")
            return {"message": "Schedule build created successfully", "raw_response": getattr(response, "text", "No response text")}
