from __future__ import annotations

import uuid
from typing import Any

from ..payloads import PerspectiveTableSpec
from ..utils import _build_schema_index, _column_name_variants, _compute_dependency_closure, _discover_dashboards_on_datasource, _extract_dashboard_references, _extract_error_message


def _is_default_perspective(perspective: dict[str, Any]) -> bool:
    """Return True for the auto-generated ``Default`` perspective every model carries.

    Live-observed: the default entry has ``isDefault: true`` and ``parentOid: null``;
    real perspectives omit the ``isDefault`` key entirely and carry a ``parentOid``.
    """
    return bool(perspective.get("isDefault")) or perspective.get("parentOid") is None


class PerspectivesMixin:
    def _attach_datamodel_titles(self, perspectives: list[dict[str, Any]]) -> None:
        """Add ``datamodelTitle`` to each perspective from one lookup of the data model list.

        Sisense's perspective objects carry only ``datamodelOid``. This resolves every
        oid to its title with a single ``GET /api/v2/datamodels/schema`` call. A failed
        lookup leaves ``datamodelTitle`` as ``None`` rather than failing the caller.
        """
        if not perspectives:
            return
        titles: dict[str, str] = {}
        response = self.api_client.get("/api/v2/datamodels/schema")
        if response is not None and response.status_code == 200:
            try:
                for model in response.json() or []:
                    if isinstance(model, dict) and isinstance(model.get("oid"), str):
                        titles[model["oid"]] = model.get("title")
            except Exception:
                self.logger.debug("Could not parse the data model list while resolving perspective model titles.")
        else:
            self.logger.debug("Could not fetch the data model list while resolving perspective model titles.")
        for perspective in perspectives:
            perspective["datamodelTitle"] = titles.get(perspective.get("datamodelOid"))

    def get_perspectives(
        self,
        perspectives: str | list[str] | None = None,
        datamodel: str | None = None,
        include_default: bool = False,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Retrieve perspectives — all of them, those of one data model, or specific ones by name or ID.

        A perspective is a metadata-only view over a root data model that keeps a
        subset of its tables and columns. This single method covers listing and
        lookup: with no arguments it returns every real perspective on the
        instance; ``datamodel`` narrows the list to one root model; and
        ``perspectives`` picks specific ones by name or ``oid``. Sisense creates
        a hidden ``Default`` perspective for every model — those are left out
        unless ``include_default`` is true or one is requested explicitly.

        Parameters
        ----------
        perspectives : str | list[str] | None, optional
            One perspective reference or a list of them, each a name
            (case-insensitive) or an ``oid``. ``None`` returns all.
        datamodel : str | None, optional
            Root data model to restrict to, as an ID or title.
        include_default : bool, optional
            Include the auto-generated ``Default`` perspectives when listing. Default ``False``.

        Returns
        -------
        list[dict[str, Any]] | dict[str, Any]
            Perspective objects as Sisense returns them, plus ``datamodelTitle`` (the root
            model's title, ``None`` if it could not be looked up). Key fields: ``oid``, ``name``,
            ``description``, ``datamodelOid`` (the root model), ``parentOid``, and
            ``tables`` — a list of ``{"oid", "diffType", "columnsDiff": [{"oid", "enabled"}]}``
            keyed by table and column oids. An empty list means nothing matched the
            filters. When one or more requested references do not exist, returns the
            standard ``{"ok": False, "error": "...", ...}`` dict, which additionally carries
            ``missing`` (the unresolved references) and ``results`` (the ones that were
            found). On an API failure or an unresolvable ``datamodel``, returns the standard
            failure dict.
        """
        if isinstance(perspectives, str):
            perspectives = [perspectives]
        requested = [ref.strip() for ref in perspectives or [] if isinstance(ref, str) and ref.strip()]
        if perspectives is not None and not requested:
            failure = {"ok": False, "error": "perspectives must be a non-empty name or ID, or a list of them."}
            self.logger.error(failure["error"])
            return failure

        datamodel_id = None
        if datamodel is not None:
            resolved = self.resolve_datamodel_reference(datamodel)
            if not resolved.get("success"):
                reason = resolved.get("error") or "not found"
                failure = {"ok": False, "error": f"Data model '{datamodel}' could not be resolved: {reason}", "status_code": resolved.get("status_code")}
                self.logger.error(failure["error"])
                return failure
            datamodel_id = resolved["datamodel_id"]

        self.logger.debug(f"Fetching perspectives (requested={requested or 'all'}, datamodel_id={datamodel_id}, include_default={include_default})")
        response = self.api_client.get("/api/v2/perspectives")
        if response is None or response.status_code != 200:
            failure = _extract_error_message(response, "Failed to fetch perspectives", self.api_client)
            self.logger.error(failure["error"])
            return failure
        try:
            payload = response.json()
        except Exception:
            failure = {"ok": False, "error": "Failed to parse the perspectives response."}
            self.logger.exception(failure["error"])
            return failure
        if not isinstance(payload, list):
            failure = {"ok": False, "error": "Unexpected perspectives response structure."}
            self.logger.error(failure["error"])
            return failure

        candidates = [p for p in payload if isinstance(p, dict)]
        if datamodel_id is not None:
            candidates = [p for p in candidates if p.get("datamodelOid") == datamodel_id]
        self._attach_datamodel_titles(candidates)

        if not requested:
            results = candidates if include_default else [p for p in candidates if not _is_default_perspective(p)]
            self.logger.info(f"Retrieved {len(results)} perspectives" + (f" for data model '{datamodel}'" if datamodel else ""))
            return results

        results: list[dict[str, Any]] = []
        seen: set[str] = set()
        missing: list[str] = []
        for ref in requested:
            wanted = ref.lower()
            matches = [p for p in candidates if p.get("oid") == ref or (isinstance(p.get("name"), str) and p["name"].strip().lower() == wanted)]
            if not matches:
                missing.append(ref)
                continue
            for match in matches:
                key = match.get("oid") or id(match)
                if key not in seen:
                    seen.add(key)
                    results.append(match)
        if missing:
            failure = {"ok": False, "error": f"Perspective(s) not found: {', '.join(missing)}", "missing": missing, "results": results}
            self.logger.error(failure["error"])
            return failure
        self.logger.info(f"Retrieved {len(results)} perspective(s) for {len(requested)} reference(s)")
        return results

    def delete_perspective(self, perspective: str, datamodel: str | None = None) -> dict[str, Any]:
        """Delete a perspective by name or ID.

        Resolves the name or ID against ``GET /api/v2/perspectives`` and sends
        ``DELETE /api/v2/perspectives/{oid}``. The root data model and its data
        are untouched; only the perspective (a metadata-only view) is removed.
        A model's hidden ``Default`` perspective is never deleted. When the same
        name exists on more than one model, ``datamodel`` must say which one.

        Parameters
        ----------
        perspective : str
            The perspective's name (case-insensitive) or ``oid``.
        datamodel : str | None, optional
            Root data model (ID or title) to disambiguate a name that exists on several models.

        Returns
        -------
        dict[str, Any]
            ``{"success": True, "message": "...", "oid", "name", "datamodelOid", "datamodelTitle"}``
            on success. On failure (not found, ambiguous, a default perspective, or an API
            error), the standard ``{"ok": False, "error": "...", ...}`` dict.
        """
        found = self.get_perspectives(perspective, datamodel=datamodel)
        if isinstance(found, dict):
            return found
        if len(found) > 1:
            owners = ", ".join(f"'{p.get('datamodelTitle') or p.get('datamodelOid')}'" for p in found)
            failure = {"ok": False, "error": f"Perspective '{perspective}' exists on several data models ({owners}); pass datamodel= to choose one."}
            self.logger.error(failure["error"])
            return failure
        target = found[0]
        if _is_default_perspective(target):
            failure = {"ok": False, "error": f"Perspective '{perspective}' is the model's built-in Default perspective and cannot be deleted."}
            self.logger.error(failure["error"])
            return failure

        oid = target.get("oid")
        self.logger.debug(f"Deleting perspective '{target.get('name')}' (oid={oid}) on data model '{target.get('datamodelTitle')}'")
        response = self.api_client.delete(f"/api/v2/perspectives/{oid}")
        if response is None or response.status_code not in (200, 204):
            failure = _extract_error_message(response, f"Failed to delete perspective '{perspective}'", self.api_client)
            self.logger.error(failure["error"])
            return failure
        self.logger.info(f"Deleted perspective '{target.get('name')}' (oid={oid}) from data model '{target.get('datamodelTitle')}'")
        return {
            "success": True,
            "message": f"Perspective '{target.get('name')}' deleted.",
            "oid": oid,
            "name": target.get("name"),
            "datamodelOid": target.get("datamodelOid"),
            "datamodelTitle": target.get("datamodelTitle"),
        }

    def create_perspective(
        self,
        datamodel: str,
        name: str,
        tables: list[PerspectiveTableSpec | str],
        description: str = "",
        ai_context: str | None = None,
    ) -> dict[str, Any]:
        """Create a perspective over a data model, keeping only the named tables and columns.

        A perspective is a metadata-only view: the root model and its data are
        untouched, and everything not listed here is left out of the view. Table
        and column names are resolved to their ids against the model's schema
        before anything is sent, so a typo fails fast and nothing half-built is
        created. Sends ``POST /api/v2/perspectives`` with the kept tables as
        ``include`` entries whose ``columnsDiff`` lists the kept columns; tables
        and columns not kept are absent from the request. After creation the
        perspective is read back and compared with the request.

        Parameters
        ----------
        datamodel : str
            The root data model, as an ID or title.
        name : str
            Name for the new perspective. Must not already exist on that model.
        tables : list[PerspectiveTableSpec | str]
            Tables to keep. Each entry is ``{"table": name, "columns": [names] | "all"}``, or a
            bare table name meaning all of its columns. Tables not listed are excluded.
        description : str, optional
            Description shown in Sisense. Default empty.
        ai_context : str | None, optional
            Free-text context for the AI assistant, stored on the perspective as ``aiContext``.

        Returns
        -------
        dict[str, Any]
            ``{"success": True, "oid", "name", "datamodelOid", "datamodelTitle", "description",
            "tables": [{"table", "table_oid", "columns_kept", "columns_total"}],
            "excluded_tables": [names], "warnings": [...]}`` on success — ``warnings`` is non-empty
            only when the read-back differs from the request. On failure (unknown model, table or
            column, a name already in use, or an API error), the standard
            ``{"ok": False, "error": "...", ...}`` dict.
        """
        if not isinstance(name, str) or not name.strip():
            return self._fail("name is required.")
        name = name.strip()
        if isinstance(tables, (str, dict)):
            tables = [tables]
        if not isinstance(tables, list) or not tables:
            return self._fail("tables must be a non-empty list of table names or {'table', 'columns'} specs.")

        resolved = self.resolve_datamodel_reference(datamodel)
        if not resolved.get("success"):
            return self._fail(f"Data model '{datamodel}' could not be resolved: {resolved.get('error') or 'not found'}", status_code=resolved.get("status_code"))
        datamodel_id, datamodel_title = resolved["datamodel_id"], resolved.get("datamodel_title")

        schema_response = self.api_client.get(f"/api/v2/datamodels/{datamodel_id}/schema")
        if schema_response is None or schema_response.status_code != 200:
            failure = _extract_error_message(schema_response, f"Failed to read the schema of data model '{datamodel_title}'", self.api_client)
            self.logger.error(failure["error"])
            return failure
        try:
            schema = schema_response.json()
        except Exception:
            return self._fail(f"Failed to parse the schema of data model '{datamodel_title}'.")
        index = _build_schema_index(schema)
        if not index["tables"]:
            return self._fail(f"Data model '{datamodel_title}' has no tables.")

        existing = self.get_perspectives(datamodel=datamodel_id, include_default=True)
        if isinstance(existing, dict):
            return existing
        if any(isinstance(p.get("name"), str) and p["name"].strip().lower() == name.lower() for p in existing):
            return self._fail(f"A perspective named '{name}' already exists on data model '{datamodel_title}'.")

        # Resolve the requested tables and columns to oids; collect every problem before failing.
        problems: list[str] = []
        kept: list[dict[str, Any]] = []
        seen_tables: set[str] = set()
        for spec in tables:
            if isinstance(spec, str):
                spec = {"table": spec}
            if not isinstance(spec, dict) or not isinstance(spec.get("table"), str) or not spec["table"].strip():
                problems.append(f"invalid table spec {spec!r}")
                continue
            table_name = spec["table"].strip()
            table_oids = index["tables_by_name"].get(table_name.lower(), [])
            if not table_oids:
                problems.append(f"table '{table_name}' not found")
                continue
            table_oid = table_oids[0]
            if table_oid in seen_tables:
                problems.append(f"table '{table_name}' listed more than once")
                continue
            seen_tables.add(table_oid)
            table = index["tables"][table_oid]
            wanted = spec.get("columns", "all")
            if isinstance(wanted, str) and wanted.strip().lower() == "all":
                column_oids = list(table["columns"])
            elif isinstance(wanted, list) and wanted:
                column_oids = []
                for column_name in wanted:
                    column_oid = table["columns_by_name"].get(column_name.strip().lower()) if isinstance(column_name, str) else None
                    if column_oid is None:
                        problems.append(f"column '{column_name}' not found in table '{table_name}'")
                    elif column_oid not in column_oids:
                        column_oids.append(column_oid)
            else:
                problems.append(f"table '{table_name}': columns must be a non-empty list of names or 'all'")
                continue
            kept.append({"table": table["name"], "table_oid": table_oid, "column_oids": column_oids, "columns_total": len(table["columns"])})
        if problems:
            return self._fail(f"Cannot create perspective '{name}' on '{datamodel_title}': " + "; ".join(problems))

        tenant = schema.get("tenant") if isinstance(schema, dict) else None
        body: dict[str, Any] = {
            "oid": str(uuid.uuid4()),
            "name": name,
            "datamodelOid": datamodel_id,
            "parentOid": datamodel_id,
            "tables": [{"oid": k["table_oid"], "diffType": "include", "columnsDiff": [{"oid": c, "enabled": True} for c in k["column_oids"]]} for k in kept],
            "relations": [],
            "fiscalYear": "system",
            "shares": [],
            "description": description or "",
            "tags": [],
        }
        if isinstance(tenant, dict) and isinstance(tenant.get("_id"), str):
            body["tenantId"] = tenant["_id"]
        if ai_context is not None:
            body["aiContext"] = ai_context

        excluded = sorted(t["name"] for oid, t in index["tables"].items() if oid not in seen_tables and isinstance(t.get("name"), str))
        self.logger.debug(f"Creating perspective '{name}' on '{datamodel_title}': keeping {len(kept)} tables, excluding {len(excluded)}")
        response = self.api_client.post("/api/v2/perspectives", data=body)
        if response is None or response.status_code not in (200, 201):
            failure = _extract_error_message(response, f"Failed to create perspective '{name}'", self.api_client)
            self.logger.error(failure["error"])
            return failure
        created_oid = body["oid"]
        try:
            created = response.json()
            if isinstance(created, dict) and isinstance(created.get("oid"), str):
                created_oid = created["oid"]
        except Exception:
            self.logger.debug("Create response carried no JSON body; using the requested oid.")

        # Read back and compare with what was asked for.
        warnings: list[str] = []
        readback = self.api_client.get(f"/api/v2/perspectives/{created_oid}")
        if readback is None or readback.status_code != 200:
            warnings.append("created, but the perspective could not be read back for verification")
        else:
            try:
                stored = readback.json()
                stored_tables = {
                    t.get("oid"): {c.get("oid") for c in (t.get("columnsDiff") or []) if isinstance(c, dict) and c.get("enabled", True)} for t in (stored.get("tables") or []) if isinstance(t, dict)
                }
                for k in kept:
                    stored_cols = stored_tables.get(k["table_oid"])
                    if stored_cols is None:
                        warnings.append(f"table '{k['table']}' is missing from the created perspective")
                    elif stored_cols != set(k["column_oids"]):
                        warnings.append(f"table '{k['table']}': {len(stored_cols)} columns stored, {len(k['column_oids'])} requested")
                for extra in set(stored_tables) - seen_tables:
                    warnings.append(f"the created perspective carries an unrequested table (oid {extra})")
            except Exception:
                warnings.append("created, but the read-back response could not be parsed")
        for w in warnings:
            self.logger.warning(f"Perspective '{name}': {w}")
        self.logger.info(f"Created perspective '{name}' (oid={created_oid}) on data model '{datamodel_title}' with {len(kept)} tables")
        return {
            "success": True,
            "oid": created_oid,
            "name": name,
            "datamodelOid": datamodel_id,
            "datamodelTitle": datamodel_title,
            "description": body["description"],
            "tables": [{"table": k["table"], "table_oid": k["table_oid"], "columns_kept": len(k["column_oids"]), "columns_total": k["columns_total"]} for k in kept],
            "excluded_tables": excluded,
            "warnings": warnings,
        }

    def _fail(self, message: str, status_code: int | None = None) -> dict[str, Any]:
        """Log and return a standard failure dict for the perspective methods."""
        failure: dict[str, Any] = {"ok": False, "error": message}
        if status_code is not None:
            failure["status_code"] = status_code
        self.logger.error(message)
        return failure

    def analyze_perspective_requirements(self, datamodel: str, detailed: bool = False) -> dict[str, Any]:
        """Work out which tables and columns of a data model its dashboards need, ready to build a perspective from.

        Read-only. Finds every dashboard that uses the model — directly, through a
        single widget, or through a perspective already built over it — reads each one's fields
        (filters, hierarchies, widget panels, nested formulas, drill history), keeping
        only references that belong to this model, resolves them against the model's
        schema, then adds what those columns depend on to keep working: join columns
        and intermediate tables on the relation paths between used tables, the columns
        custom columns read, and the tables custom tables select from. Anything that
        could not be resolved or verified is reported as an issue rather than dropped.

        Parameters
        ----------
        datamodel : str
            The data model, as an ID or title.
        detailed : bool, optional
            Include the per-dashboard, per-column, per-dependency and per-issue detail.
            Default ``False`` returns the summary view only.

        Returns
        -------
        dict[str, Any]
            Always: ``datamodel`` (``oid``, ``title``, ``type``, counts of ``tables``, ``columns``,
            ``relations``, ``custom_columns`` and ``custom_tables``, and the names of its existing
            ``perspectives``); ``summary`` (``model_tables``, ``model_columns``, ``dashboards_analyzed``,
            ``dashboards_failed``, ``tables_used_by_dashboards``, ``columns_used_by_dashboards``,
            ``columns_required_for_dependencies``, ``tables_required_in_perspective``,
            ``columns_required_in_perspective``, ``tables_not_required``, ``columns_not_required``, and
            ``issues`` by severity); ``perspective_tables`` — the ``{"table", "columns"}`` entries a
            perspective must keep, every used column plus every dependency, in the form
            ``create_perspective`` accepts; ``errors`` — the distinct error messages; and ``warnings`` —
            warning counts by kind.

            With ``detailed=True`` also: ``required`` (``tables``: ``table``, ``columns_used``,
            ``columns_total``, ``used_by_dashboards``; ``columns``: ``table``, ``column``, ``used_in`` —
            ``"filter"``, ``"hierarchy"`` and/or ``"widget"`` — ``used_by``); ``dependencies`` (``columns``
            with ``table``, ``column``, ``reason`` — ``join_column``, ``custom_column_expression``,
            ``custom_table_source`` — ``required_by``, ``detail``; ``tables`` required only as join
            paths; ``join_paths``); ``not_required`` (``tables``, ``columns``); ``dashboards``
            (``analyzed``: ``dashboard_id``, ``title``, ``match``, ``datasource`` — the model or the
            perspective the dashboard sits on — ``owner``, ``owner_email``, ``tables_used``,
            ``columns_used``, ``columns`` as ``"Table.Column"``, ``widgets_on_other_datasources``;
            ``failed``); and ``issues`` (``severity``, ``kind``, ``dashboard``, ``widget_id``, ``detail``).
            On failure to resolve the model, read its schema or list dashboards, the standard
            ``{"ok": False, "error": "...", ...}`` dict.
        """
        resolved = self.resolve_datamodel_reference(datamodel)
        if not resolved.get("success"):
            return self._fail(f"Data model '{datamodel}' could not be resolved: {resolved.get('error') or 'not found'}", status_code=resolved.get("status_code"))
        model_id, model_title = resolved["datamodel_id"], resolved.get("datamodel_title") or datamodel

        schema_response = self.api_client.get(f"/api/v2/datamodels/{model_id}/schema")
        if schema_response is None or schema_response.status_code != 200:
            failure = _extract_error_message(schema_response, f"Failed to read the schema of data model '{model_title}'", self.api_client)
            self.logger.error(failure["error"])
            return failure
        try:
            schema = schema_response.json()
        except Exception:
            return self._fail(f"Failed to parse the schema of data model '{model_title}'.")
        index = _build_schema_index(schema)
        if not index["tables"]:
            return self._fail(f"Data model '{model_title}' has no tables.")
        model_type = schema.get("type") if isinstance(schema, dict) else None
        known_columns = {
            (table["name"], column["name"]) for table in index["tables"].values() for column in table["columns"].values() if isinstance(table.get("name"), str) and isinstance(column.get("name"), str)
        }

        # Dashboards on the model itself, and on any perspective already built over it: both
        # consume the model's columns, and their references resolve against the same schema.
        existing = self.get_perspectives(datamodel=model_id)
        perspective_titles = [p["name"] for p in existing if isinstance(p.get("name"), str)] if isinstance(existing, list) else []
        matches: dict[str, str] = {}
        sources: dict[str, str] = {}
        listing: dict[str, dict[str, Any]] = {}
        for source_title in [model_title] + perspective_titles:
            discovered = _discover_dashboards_on_datasource(self.api_client, self.logger, source_title)
            if discovered.get("ok") is False:
                self.logger.error(discovered["error"])
                return discovered
            listing.update(discovered["dashboards"])
            for oid, match in discovered["matches"].items():
                if oid not in matches or (matches[oid] == "widget" and match == "dashboard"):
                    matches[oid] = match
                    sources[oid] = source_title

        issues: list[dict[str, Any]] = []

        def issue(severity: str, kind: str, dashboard: str | None, widget_id: str | None, detail: str) -> None:
            if not any(i["kind"] == kind and i["detail"] == detail and i["dashboard"] == dashboard for i in issues):
                issues.append({"severity": severity, "kind": kind, "dashboard": dashboard, "widget_id": widget_id, "detail": detail})

        # Export the dashboards in batches and collect every reference to this model.
        exports: dict[str, dict[str, Any]] = {}
        failed: list[dict[str, Any]] = []
        ids = sorted(matches)
        for start in range(0, len(ids), 20):
            batch = ids[start : start + 20]
            response = self.api_client.get("/api/v1/dashboards/export", params={"dashboardIds": ",".join(batch), "adminAccess": "true"})
            body = None
            if response is not None and response.status_code == 200:
                try:
                    body = response.json()
                except Exception:
                    body = None
            if not isinstance(body, list):
                reason = _extract_error_message(response, "export failed", self.api_client)["error"] if response is None or response.status_code != 200 else "export returned no dashboards"
                for oid in batch:
                    failed.append({"dashboard_id": oid, "title": (listing.get(oid) or {}).get("title"), "error": reason})
                    issue("error", "dashboard_export_failed", oid, None, f"dashboard '{(listing.get(oid) or {}).get('title')}' could not be exported: {reason}")
                continue
            for dashboard in body:
                if isinstance(dashboard, dict) and isinstance(dashboard.get("oid"), str):
                    exports[dashboard["oid"]] = dashboard
            for oid in batch:
                if oid not in exports:
                    failed.append({"dashboard_id": oid, "title": (listing.get(oid) or {}).get("title"), "error": "not present in the export response"})
                    issue("error", "dashboard_export_failed", oid, None, f"dashboard '{(listing.get(oid) or {}).get('title')}' was not present in the export response")

        owner_emails: dict[str, str] = {}
        users = self.api_client.get("/api/v1/users")
        if users is not None and users.status_code == 200:
            try:
                owner_emails = {u["_id"]: u.get("email") for u in users.json() if isinstance(u, dict) and u.get("_id")}
            except Exception:
                self.logger.debug("Could not parse the user list while resolving dashboard owners.")

        used: dict[tuple[str, str], set[str]] = {}  # (table_oid, column_oid) -> dashboard oids
        used_where: dict[tuple[str, str], set[str]] = {}  # (table_oid, column_oid) -> {"filter", "hierarchy", "widget"}
        other_datasources: dict[str, list[dict[str, Any]]] = {}  # dashboard oid -> widgets left on other datasources
        lowered_tables = {name.lower(): oids for name, oids in ((t["name"], [oid]) for oid, t in index["tables"].items() if isinstance(t.get("name"), str))}
        severity_of = {"unreadable_dim": "error", "ambiguous_dim": "warning", "blox_widget": "warning", "script_present": "warning", "unclassified_location": "warning"}
        for oid, dashboard in exports.items():
            title = dashboard.get("title")
            report = _extract_dashboard_references(dashboard, title, known_columns=known_columns, logger=self.logger, datasource=sources.get(oid, model_title))
            other_datasources[oid] = [{"widget_id": w.get("widget_id"), "title": w.get("title"), "type": w.get("type"), "datasource": w.get("datasource")} for w in report["skipped_widgets"]]
            for found in report["issues"]:
                severity = severity_of.get(found["kind"])
                if severity:  # informational kinds (a widget on another datasource) are not issues for the perspective
                    issue(severity, found["kind"], oid, found.get("widget_id"), f"{title}: {found['detail']}")
            for row in report["rows"]:
                table_oids = lowered_tables.get(str(row["table"]).strip().lower(), [])
                column_oid = None
                for table_oid in table_oids:
                    table_index = index["tables"][table_oid]
                    for variant in _column_name_variants(str(row["column"])):
                        key = variant.strip().lower()
                        column_oid = table_index["columns_by_name"].get(key)
                        if column_oid:
                            break
                        # Renamed after the dashboard was built: the dashboard still uses the original
                        # (or display) name. Keep the column — dropping it would break the dashboard — and warn.
                        column_oid = table_index["columns_by_alias"].get(key)
                        if column_oid:
                            current = table_index["columns"][column_oid].get("name")
                            issue(
                                "warning",
                                "renamed_reference",
                                oid,
                                row.get("widget_id"),
                                f"{title}: '{row['table']}'.'{row['column']}' is referenced by a former name; the model now calls it '{current}' (kept)",
                            )
                            break
                    if column_oid:
                        used.setdefault((table_oid, column_oid), set()).add(oid)
                        used_where.setdefault((table_oid, column_oid), set()).add(str(row.get("source")))
                        break
                if not column_oid:
                    issue("error", "unresolved_reference", oid, row.get("widget_id"), f"{title}: '{row['table']}'.'{row['column']}' is used but does not exist in data model '{model_title}'")

        closure = _compute_dependency_closure(index, set(used))
        for found in closure["issues"]:
            issue(found["severity"], found["kind"], None, None, found["detail"])

        # Assemble the report.
        def name_of(table_oid: str, column_oid: str | None = None) -> tuple[str, str | None]:
            table = index["tables"].get(table_oid) or {}
            column = (table.get("columns") or {}).get(column_oid) if column_oid else None
            return table.get("name"), (column or {}).get("name") if column else None

        kept: dict[str, set[str]] = {}
        for table_oid, column_oid in list(used) + list(closure["retained"]):
            kept.setdefault(table_oid, set()).add(column_oid)
        for table_oid in closure["tables"]:
            kept.setdefault(table_oid, set())

        titles = {oid: (exports.get(oid) or listing.get(oid) or {}).get("title") for oid in matches}
        required_columns = []
        for (table_oid, column_oid), dashboards_using in sorted(used.items(), key=lambda kv: (name_of(*kv[0])[0] or "", name_of(*kv[0])[1] or "")):
            table_name, column_name = name_of(table_oid, column_oid)
            required_columns.append(
                {"table": table_name, "column": column_name, "used_in": sorted(used_where.get((table_oid, column_oid), set())), "used_by": sorted(titles.get(d) or d for d in dashboards_using)}
            )
        required_tables = []
        for table_oid in sorted({t for t, _ in used}, key=lambda t: name_of(t)[0] or ""):
            table = index["tables"][table_oid]
            using = {d for (t, _), ds in used.items() if t == table_oid for d in ds}
            required_tables.append({"table": table["name"], "columns_used": sum(1 for (t, _) in used if t == table_oid), "columns_total": len(table["columns"]), "used_by_dashboards": len(using)})
        dependencies = []
        for (table_oid, column_oid), reasons in sorted(closure["retained"].items(), key=lambda kv: (name_of(*kv[0])[0] or "", name_of(*kv[0])[1] or "")):
            table_name, column_name = name_of(table_oid, column_oid)
            for reason in reasons:
                required_by = reason.get("required_by")
                if isinstance(required_by, tuple) and len(required_by) == 2 and required_by[1] in (index["tables"].get(required_by[0]) or {}).get("columns", {}):
                    required_by_label = "{}.{}".format(*name_of(*required_by))
                elif isinstance(required_by, tuple):
                    required_by_label = " .. ".join(name_of(t)[0] or t for t in required_by)
                else:
                    required_by_label = name_of(required_by)[0] if isinstance(required_by, str) else str(required_by)
                dependencies.append({"table": table_name, "column": column_name, "reason": reason["reason"], "required_by": required_by_label, "detail": reason.get("detail")})
        dependency_tables = sorted(name_of(t)[0] for t, reasons in closure["tables"].items() if t not in {k[0] for k in used} and t not in {k[0] for k in closure["retained"]})

        tables_spec: list[dict[str, Any]] = []
        for table_oid in sorted(kept, key=lambda t: name_of(t)[0] or ""):
            table = index["tables"][table_oid]
            columns = sorted(table["columns"][c]["name"] for c in kept[table_oid] if c in table["columns"] and isinstance(table["columns"][c].get("name"), str))
            tables_spec.append({"table": table["name"], "columns": columns if columns else "all"})

        excluded_tables = sorted(t["name"] for oid, t in index["tables"].items() if oid not in kept and isinstance(t.get("name"), str))
        excluded_columns = []
        for table_oid, column_oids in kept.items():
            table = index["tables"][table_oid]
            for column_oid, column in table["columns"].items():
                if column_oid not in column_oids and isinstance(column.get("name"), str):
                    excluded_columns.append({"table": table["name"], "column": column["name"]})
        excluded_columns.sort(key=lambda c: (c["table"], c["column"]))
        excluded_column_count = len(excluded_columns) + sum(len(t["columns"]) for oid, t in index["tables"].items() if oid not in kept)

        analyzed = []
        for oid in sorted(exports, key=lambda o: (titles.get(o) or "").lower()):
            owner_id = (listing.get(oid) or {}).get("owner")
            analyzed.append(
                {
                    "dashboard_id": oid,
                    "title": titles.get(oid),
                    "match": matches[oid],
                    "datasource": sources.get(oid, model_title),
                    "owner": owner_id,
                    "owner_email": owner_emails.get(owner_id),
                    "tables_used": len({key[0] for key, ds in used.items() if oid in ds}),
                    "columns_used": sum(1 for ds in used.values() if oid in ds),
                    "columns": sorted(f"{name_of(*key)[0]}.{name_of(*key)[1]}" for key, ds in used.items() if oid in ds),
                    "widgets_on_other_datasources": other_datasources.get(oid, []),
                }
            )
        by_severity = {s: sum(1 for i in issues if i["severity"] == s) for s in ("error", "warning")}
        summary = {
            "model_tables": len(index["tables"]),
            "model_columns": sum(len(t["columns"]) for t in index["tables"].values()),
            "dashboards_analyzed": len(analyzed),
            "dashboards_failed": len(failed),
            "tables_used_by_dashboards": len(required_tables),
            "columns_used_by_dashboards": len(required_columns),
            "columns_required_for_dependencies": len(closure["retained"]),
            "tables_required_in_perspective": len(tables_spec),
            "columns_required_in_perspective": sum(len(cols) if cols else len(index["tables"][oid]["columns"]) for oid, cols in kept.items()),
            "tables_not_required": len(excluded_tables),
            "columns_not_required": excluded_column_count,
            "issues": by_severity,
        }
        self.logger.info(f"Perspective analysis for '{model_title}': {summary}")
        model_facts = {
            "oid": model_id,
            "title": model_title,
            "type": model_type,
            "tables": len(index["tables"]),
            "columns": sum(len(t["columns"]) for t in index["tables"].values()),
            "relations": len(index["relations"]),
            "custom_columns": sum(1 for t in index["tables"].values() for c in t["columns"].values() if c.get("is_custom") or c.get("expression")),
            "custom_tables": sum(1 for t in index["tables"].values() if t.get("sql")),
            "perspectives": perspective_titles,
        }
        errors = sorted({i["detail"] for i in issues if i["severity"] == "error"})
        warnings_by_kind: dict[str, int] = {}
        for i in issues:
            if i["severity"] == "warning":
                warnings_by_kind[i["kind"]] = warnings_by_kind.get(i["kind"], 0) + 1
        result: dict[str, Any] = {
            "datamodel": model_facts,
            "summary": summary,
            "perspective_tables": tables_spec,
            "errors": errors,
            "warnings": warnings_by_kind,
        }
        if detailed:
            result.update(
                {
                    "required": {"tables": required_tables, "columns": required_columns},
                    "dependencies": {"columns": dependencies, "tables": dependency_tables, "join_paths": [[name_of(t)[0] for t in path["tables"]] for path in closure["join_paths"]]},
                    "not_required": {"tables": excluded_tables, "columns": excluded_columns},
                    "dashboards": {"analyzed": analyzed, "failed": failed},
                    "issues": issues,
                }
            )
        return result
