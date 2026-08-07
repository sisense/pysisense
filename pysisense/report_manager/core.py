from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError

_DEFAULT_PAGE_LIMIT = 100


class ReportTypeFlags(BaseModel):
    """Output formats for a Report Manager report.

    The Report Manager API's own schema validation requires ``PDF``,
    ``CSV``, and ``URL`` to all be present (as booleans) — omitting any of
    them fails with ``OBJECT_MISSING_REQUIRED_PROPERTY``, even though only
    one format may actually be enabled. Additional format keys (for example
    ``XLS`` or a nested ``pdf`` settings object) are accepted but not
    required.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    pdf: bool = Field(..., alias="PDF")
    csv: bool = Field(..., alias="CSV")
    url: bool = Field(..., alias="URL")


class FileShareSettings(BaseModel):
    """Archive-to-file-share destination for a Report Manager report.

    Report Manager's schema validation requires ``overwriteExisting`` to be
    present even when the report has no file-share destination configured.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    overwrite_existing: bool = Field(..., alias="overwriteExisting")
    url: str | None = None
    sftp_server_name: str | None = Field(None, alias="SFTPServerName")


class RunOnFinishSettings(BaseModel):
    """Post-run actions for a Report Manager report."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    file_share: FileShareSettings = Field(..., alias="fileShare")


class CreateReportPayload(BaseModel):
    """Payload for creating a Report Manager report."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    name: str
    report_type: ReportTypeFlags = Field(default_factory=lambda: ReportTypeFlags(PDF=False, CSV=False, URL=False), alias="reportType")
    run_on_finish: RunOnFinishSettings = Field(
        default_factory=lambda: RunOnFinishSettings(fileShare=FileShareSettings(overwriteExisting=False)),
        alias="runOnFinish",
    )
    events: list[str] = Field(default_factory=list)
    enabled: bool | None = None
    schedule: dict[str, Any] | None = None
    dashboards: list[dict[str, Any]] | None = None
    recipients: list[dict[str, Any]] | None = None
    template_id: str | None = Field(None, alias="templateId")
    priority: str | None = None
    error_emails: list[str] | None = Field(None, alias="errorEmails")


class UpdateReportPayload(BaseModel):
    """Payload for updating a Report Manager report."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    name: str | None = None
    enabled: bool | None = None
    schedule: dict[str, Any] | None = None
    events: list[str] | None = None
    dashboards: list[dict[str, Any]] | None = None
    recipients: list[dict[str, Any]] | None = None
    template_id: str | None = Field(None, alias="templateId")
    priority: str | None = None
    run_on_finish: RunOnFinishSettings | None = Field(None, alias="runOnFinish")
    error_emails: list[str] | None = Field(None, alias="errorEmails")
    report_type: ReportTypeFlags | None = Field(None, alias="reportType")


class ReportManagerCoreMixin:
    def get_reports(
        self,
        *,
        name: str | None = None,
        ids: list[str] | str | None = None,
        enabled: bool | None = None,
        statuses: list[str] | str | None = None,
        priority: str | None = None,
        owner_ids: list[str] | str | None = None,
        fields: str | None = None,
        sort: str | None = None,
        limit: int = _DEFAULT_PAGE_LIMIT,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve all reports configured in Report Manager.

        Sends paginated requests to ``GET /api/v1/report_manager/reports`` and
        collects every page into a single flat list. Filters are applied
        server-side.

        Parameters
        ----------
        name : str, optional
            Search reports by name.
        ids : list[str] or str, optional
            One or more report ids to filter by. A bare string is treated as
            a single id.
        enabled : bool, optional
            When provided, filter to only enabled (``True``) or disabled
            (``False``) reports.
        statuses : list[str] or str, optional
            One or more running statuses to filter by. A bare string is
            treated as a single status.
        priority : str, optional
            Filter reports by priority. One of ``"high"``, ``"normal"``.
        owner_ids : list[str] or str, optional
            One or more owner user ids to filter by. A bare string is
            treated as a single id.
        fields : str, optional
            Whitelist of fields to return for each report. Fields can also
            be excluded by prefixing their name with ``-``.
        sort : str, optional
            Field to sort results by. Ascending by default, descending if
            prefixed with ``-``.
        limit : int, optional
            Page size used for the underlying paginated requests. Default is
            ``100``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            A flat list of report objects, or ``{"error": "..."}`` on
            failure.
        """
        if isinstance(ids, str):
            ids = [ids]
        if isinstance(statuses, str):
            statuses = [statuses]
        if isinstance(owner_ids, str):
            owner_ids = [owner_ids]

        params: dict[str, Any] = {"limit": limit}
        if name is not None:
            params["name"] = name
        if ids is not None:
            params["ids"] = ",".join(ids)
        if enabled is not None:
            params["enabled"] = enabled
        if statuses is not None:
            params["statuses"] = ",".join(statuses)
        if priority is not None:
            params["priority"] = priority
        if owner_ids is not None:
            params["ownerIds"] = ",".join(owner_ids)
        if fields is not None:
            params["fields"] = fields
        if sort is not None:
            params["sort"] = sort

        reports: list[dict[str, Any]] = []
        page = 1

        while True:
            params["page"] = page
            self.logger.debug(f"Fetching Report Manager reports, page={page} limit={limit}")
            response = self.api_client.get("/api/v1/report_manager/reports", params=params)

            error = self._report_manager_error(response, "fetch reports")
            if error is not None:
                return error

            body = response.json()
            page_data = body.get("data", [])
            reports.extend(page_data)

            pagination = body.get("pagination", {})
            if pagination.get("isLastPage", True) or not page_data:
                break
            page += 1

        self.logger.info(f"Retrieved {len(reports)} Report Manager report(s)")
        return reports

    def get_report(
        self,
        report_id: str,
        *,
        owner_info: bool = False,
        recipients_info: bool = False,
        dashboards_info: bool = False,
        fields: str | None = None,
    ) -> dict[str, Any]:
        """Retrieve a single Report Manager report by id.

        Sends ``GET /api/v1/report_manager/reports/{id}``.

        Parameters
        ----------
        report_id : str
            The report id.
        owner_info : bool, optional
            When ``True``, include the report owner's information. Default
            is ``False``.
        recipients_info : bool, optional
            When ``True``, include recipient data. Default is ``False``.
        dashboards_info : bool, optional
            When ``True``, include dashboard names. Default is ``False``.
        fields : str, optional
            Whitelist of fields to return. Fields can also be excluded by
            prefixing their name with ``-``.

        Returns
        -------
        dict[str, Any]
            The report object, or ``{"error": "..."}`` on failure or when the
            report is not found.
        """
        params: dict[str, Any] = {}
        if owner_info:
            params["ownerInfo"] = True
        if recipients_info:
            params["recipientsInfo"] = True
        if dashboards_info:
            params["dashboardsInfo"] = True
        if fields is not None:
            params["fields"] = fields

        self.logger.debug(f"Fetching Report Manager report '{report_id}'")
        response = self.api_client.get(f"/api/v1/report_manager/reports/{report_id}", params=params)

        error = self._report_manager_error(response, f"fetch report '{report_id}'")
        if error is not None:
            return error

        report = response.json()
        self.logger.info(f"Retrieved Report Manager report '{report_id}'")
        return report

    def create_report(self, report: dict[str, Any]) -> dict[str, Any]:
        """Create a new Report Manager report.

        Sends ``POST /api/v1/report_manager/reports``. Supported fields (canonical
        Sisense payload names): ``name`` (required), ``reportType``
        (optional — defaults to all formats disabled when omitted; if
        provided, must include the ``PDF``, ``CSV``, and ``URL`` boolean
        flags), ``runOnFinish`` (optional — defaults to
        ``{"fileShare": {"overwriteExisting": False}}`` when omitted; if
        provided, must include ``fileShare.overwriteExisting``), ``events``
        (defaults to ``[]`` when omitted), ``enabled``, ``schedule``,
        ``dashboards``, ``recipients``, ``templateId``, ``priority``,
        ``errorEmails``.

        Parameters
        ----------
        report : dict[str, Any]
            The report definition to create.

        Returns
        -------
        dict[str, Any]
            The API response body on success, or ``{"error": "..."}`` on
            failure.
        """
        try:
            payload = CreateReportPayload(**report)
        except ValidationError as e:
            msg = f"Invalid report payload: {e}"
            self.logger.error(msg)
            return {"error": msg}

        body = [payload.model_dump(by_alias=True, exclude_none=True)]

        self.logger.debug(f"Creating Report Manager report '{payload.name}'")
        response = self.api_client.post("/api/v1/report_manager/reports", data=body)

        error = self._report_manager_error(response, f"create report '{payload.name}'", success_codes=(200, 201))
        if error is not None:
            return error

        result = response.json() if response.content else {"success": True}
        self.logger.info(f"Created Report Manager report '{payload.name}'")
        return result if isinstance(result, dict) else {"result": result}

    def update_report(self, report_id: str, report: dict[str, Any]) -> dict[str, Any]:
        """Update an existing Report Manager report.

        Updates must be provided inside the ``report`` payload. Only fields
        provided are updated; omitted fields are not modified. Sends
        ``PATCH /api/v1/report_manager/reports/{id}``. Supported fields (canonical
        Sisense payload names): ``name``, ``enabled``, ``schedule``,
        ``events``, ``dashboards``, ``recipients``, ``templateId``,
        ``priority``, ``errorEmails``, ``reportType`` (if provided, must
        include the ``PDF``, ``CSV``, and ``URL`` boolean flags),
        ``runOnFinish`` (if provided, must include
        ``fileShare.overwriteExisting``).

        Parameters
        ----------
        report_id : str
            The report id to update.
        report : dict[str, Any]
            The fields to update.

        Returns
        -------
        dict[str, Any]
            The API response body on success, or ``{"error": "..."}`` on
            failure.
        """
        try:
            payload = UpdateReportPayload(**report)
        except ValidationError as e:
            msg = f"Invalid report payload: {e}"
            self.logger.error(msg)
            return {"error": msg}

        body = payload.model_dump(by_alias=True, exclude_unset=True, exclude_none=True)
        if not body:
            self.logger.info(f"No fields provided to update for report '{report_id}' — nothing to do.")
            return {"success": True, "changed": False}

        self.logger.debug(f"Updating Report Manager report '{report_id}' — fields: {list(body.keys())}")
        response = self.api_client.patch(f"/api/v1/report_manager/reports/{report_id}", data=body)

        error = self._report_manager_error(response, f"update report '{report_id}'")
        if error is not None:
            return error

        result = response.json() if response.content else {"success": True}
        self.logger.info(f"Updated Report Manager report '{report_id}'")
        return result if isinstance(result, dict) else {"result": result}

    def delete_report(self, report_id: str) -> dict[str, Any]:
        """Delete a Report Manager report by id.

        Sends ``DELETE /api/v1/report_manager/reports/{id}``.

        Parameters
        ----------
        report_id : str
            The report id to delete.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success, or ``{"error": "..."}`` on
            failure.
        """
        self.logger.debug(f"Deleting Report Manager report '{report_id}'")
        response = self.api_client.delete(f"/api/v1/report_manager/reports/{report_id}")

        error = self._report_manager_error(response, f"delete report '{report_id}'", success_codes=(200, 204))
        if error is not None:
            return error

        self.logger.info(f"Deleted Report Manager report '{report_id}'")
        return {"success": True}

    def run_report(self, report_id: str) -> dict[str, Any]:
        """Trigger an immediate run of a Report Manager report.

        Sends ``POST /api/v1/report_manager/reports/{id}/run``. If the maximum
        number of concurrently running reports has been reached, the report
        is queued by the server instead of running immediately.

        Parameters
        ----------
        report_id : str
            The report id to run.

        Returns
        -------
        dict[str, Any]
            ``{"success": True}`` on success, or ``{"error": "..."}`` on
            failure.
        """
        self.logger.debug(f"Running Report Manager report '{report_id}' now")
        response = self.api_client.post(f"/api/v1/report_manager/reports/{report_id}/run")

        error = self._report_manager_error(response, f"run report '{report_id}'")
        if error is not None:
            return error

        self.logger.info(f"Triggered run for Report Manager report '{report_id}'")
        return response.json() if response.content else {"success": True}

    def _report_manager_error(
        self,
        response: Any,
        action: str,
        *,
        success_codes: tuple[int, ...] = (200,),
    ) -> dict[str, Any] | None:
        """Build an error dict for a failed Report Manager response, or return ``None`` on success.

        Report Manager is an on-demand plugin, so a missing or unexpected
        response is reported as a normal ``{"error": "..."}`` result rather
        than raising. A 504 is flagged as the Report Manager service being
        down or unavailable. A 404 with an empty body is flagged as the
        plugin possibly not being installed or enabled, since Report
        Manager's own "not found" errors always carry a JSON error body.
        """
        if response is None:
            msg = f"Failed to {action} — no response received."
            self.logger.error(msg)
            return {"error": msg}

        if response.status_code not in success_codes:
            try:
                detail = response.json()
            except Exception:
                detail = response.text

            msg = f"Failed to {action} — status {response.status_code}: {detail}"
            if response.status_code == 504:
                msg = f"{msg} A 504 means the request timed out waiting for a response, the Report Manager service may be down or otherwise unavailable."
            elif response.status_code == 404 and not response.content:
                msg = f"{msg} A 404 with an empty body usually means the Report Manager plugin is not installed or enabled on this Sisense instance."
            self.logger.error(msg)
            return {"error": msg}

        return None
