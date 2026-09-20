from __future__ import annotations

import uuid
from typing import Any

from ..payloads import PerspectiveTableSpec
from ..utils import (
    _build_schema_index,
    _co_authoring_enabled,
    _column_name_variants,
    _compute_dependency_closure,
    _dashboard_for_reading,
    _discover_dashboards_on_datasource,
    _extract_dashboard_references,
    _extract_error_message,
    _group_relation_column_pairs,
    _many_to_many_check,
    _sql_names_table,
    _widget_query_metadata,
)


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
        (filters, hierarchies, widget panels, nested formulas, drill history) from the
        dashboard export — under Dashboard Co-Authoring, from the shared copy viewers see — its own filters,
        hierarchies and widgets, read as administrator or as owner; a shared copy neither can read fails that dashboard with a
        ``shared_copy_unreadable`` error rather than analysing the owner's private copy in its
        place — keeping only references that belong to this model, and resolves
        them against the model's schema. It then adds what the joins need: for every pair
        of tables that meet in one query — a widget's own tables together with the tables
        of the dashboard's filters and hierarchies, which apply to every widget, including
        widgets that have switched a filter off — it follows the shortest relation paths
        between them in the model and keeps both key columns of every relation on the way
        and any intermediate table. Tables used only by separate widgets, or by separate
        dashboards, need no join and get none. A perspective inherits a relation only when
        both of its columns are kept, and it evaluates custom columns and custom tables
        through the root model, so nothing else is added.

        When two tables are joined by more than one equally short path, and at least one of
        those paths runs through a table nothing else needs, the pair is a choice. For each
        widget behind such a pair the method sends the widget's query, with the dashboard
        filters applied, to ``POST /api/datasources/{model}/jaql/sql`` — translation only,
        nothing is executed — and reads from the returned SQL which of the candidate tables
        the query engine actually joins through. ``perspective_tables`` keeps only those
        paths; ``perspective_tables_all_paths`` keeps every path; ``join_path_choices`` lists
        the pair, every path and which ones are in use. When the translation cannot be
        obtained, or names none of the candidates, every path is kept in both lists and the
        pair is reported as ``ambiguous_join_path``. Every relation between two kept tables is
        then tested for a many-to-many join with one aggregate SQL query per side
        (``GET /api/datasources/{model}/sql``): a perspective inherits the root model's
        relations, so a query spanning two kept tables joined many-to-many can fan out and
        double count. Such a pair is a warning with its evidence, never an error and never a
        reason to drop a table. Anything that could not be resolved or verified is reported as
        an issue rather than dropped.

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
            ``columns_required_in_perspective``, ``tables_required_all_paths``,
            ``columns_required_all_paths``, ``tables_not_required``, ``columns_not_required``, and
            ``issues`` by severity); ``perspective_tables`` — the ``{"table", "columns"}`` entries a
            perspective must keep, ``columns`` always the explicit list of names: every used column
            plus the join columns and intermediate tables of the paths the query engine uses (every
            path where that could not be determined); ``perspective_tables_all_paths`` — the same
            with every equally short path kept; ``join_path_choices`` — one entry per pair of
            tables joined by more than one equally short path where some path runs through a
            table nothing else needs, with ``from``, ``to``, ``needed_by`` (which dashboards, filters
            and how many widgets put the two tables in one query), ``resolved`` (whether the
            engine's path is known) and ``paths`` (each ``{"via": [...], "in_use": ...}`` — the
            intermediate tables of one path and whether the engine uses it; ``None`` when not
            resolved); ``errors`` — the distinct error messages; and ``warnings`` — warning counts
            by kind, ``many_to_many_in_perspective`` always present (``0`` when none), plus
            ``many_to_many_unchecked`` when a pair's SQL check failed.

            With ``detailed=True`` also: ``required`` (``tables``: ``table``, ``columns_used``,
            ``columns_total``, ``used_by_dashboards``; ``columns``: ``table``, ``column``, ``used_in`` —
            ``"filter"``, ``"hierarchy"`` and/or ``"widget"`` — ``used_by``); ``dependencies`` (``columns``
            with ``table``, ``column``, ``reason`` — always ``join_column`` — ``required_by``, ``detail``,
            ``in_use`` — whether the column is in ``perspective_tables``; ``tables`` kept only as join
            paths in ``perspective_tables``; ``tables_all_paths`` the same for every path; ``join_paths``);
            ``not_required`` (``tables``, ``columns`` — relative to ``perspective_tables``); ``many_to_many``
            (one entry per kept table pair joined many-to-many or not checkable: ``table_a``, ``columns_a``,
            ``table_b``, ``columns_b``, ``duplicate_keys_a``, ``duplicate_keys_b``, ``is_m2m``, ``status``,
            ``error``, and ``scope`` — ``"perspective"``, or ``"all_paths"`` for a pair kept only by the
            all-paths variant); ``dashboards``
            (``analyzed``: ``dashboard_id``, ``title``, ``match``, ``datasource`` — the model or the
            perspective the dashboard sits on — ``copy`` — ``"shared"`` under Dashboard Co-Authoring
            for a published dashboard, else ``"private"`` (the single copy) — ``owner``, ``owner_email``, ``tables_used``,
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

        # Under Dashboard Co-Authoring the export returns the owner's private copy; viewers see the shared copy,
        # which is read as owner or as administrator. A shared copy neither can read fails that dashboard: the
        # private copy may differ from what viewers see and is never analysed in its place.
        copies_read: dict[str, str] = {}
        co_authoring = _co_authoring_enabled(self.api_client, next(iter(exports), None))
        for oid, dashboard in list(exports.items()):
            document, copy_read, shared_status = _dashboard_for_reading(self.api_client, self.logger, dashboard, co_authoring)
            if document is None:
                del exports[oid]
                failed.append({"dashboard_id": oid, "title": dashboard.get("title"), "error": f"the shared copy could not be read (HTTP {shared_status})"})
                issue(
                    "error",
                    "shared_copy_unreadable",
                    oid,
                    None,
                    f"dashboard '{dashboard.get('title')}': the shared copy viewers see could not be read (HTTP {shared_status}); an owner or administrator token is required",
                )
                continue
            exports[oid] = document
            copies_read[oid] = copy_read

        owner_emails: dict[str, str] = {}
        users = self.api_client.get("/api/v1/users")
        if users is not None and users.status_code == 200:
            try:
                owner_emails = {u["_id"]: u.get("email") for u in users.json() if isinstance(u, dict) and u.get("_id")}
            except Exception:
                self.logger.debug("Could not parse the user list while resolving dashboard owners.")

        used: dict[tuple[str, str], set[str]] = {}  # (table_oid, column_oid) -> dashboard oids
        used_where: dict[tuple[str, str], set[str]] = {}  # (table_oid, column_oid) -> {"filter", "hierarchy", "widget"}
        # Which tables meet in one query: per dashboard, each widget's own tables plus the tables of the
        # dashboard-level filters and hierarchies (widget_id "N/A"), which apply to every widget.
        scopes: dict[str, dict[str, dict[str, set[str]]]] = {}  # dashboard oid -> widget_id -> table_oid -> {source, ...}
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
                        scopes.setdefault(oid, {}).setdefault(str(row.get("widget_id")), {}).setdefault(table_oid, set()).add(str(row.get("source")))
                        break
                if not column_oid:
                    issue("error", "unresolved_reference", oid, row.get("widget_id"), f"{title}: '{row['table']}'.'{row['column']}' is used but does not exist in data model '{model_title}'")

        def name_of(table_oid: str, column_oid: str | None = None) -> tuple[str, str | None]:
            table = index["tables"].get(table_oid) or {}
            column = (table.get("columns") or {}).get(column_oid) if column_oid else None
            return table.get("name"), (column or {}).get("name") if column else None

        join_pairs: set[tuple[str, str]] = set()
        # sorted table pair -> (dashboard oid, dashboard title, kind, table a, table b) -> widget ids; kind is "both"
        # (the widget itself uses both tables) or the dashboard-level source ("filter"/"hierarchy") that reaches the widget.
        needed_by: dict[tuple[str, str], dict[tuple[str, str, str, str, str], set[str]]] = {}
        for oid, by_widget in scopes.items():
            title = (exports.get(oid) or {}).get("title") or oid
            shared = by_widget.get("N/A", {})
            for widget_id, own in by_widget.items():
                if widget_id == "N/A":
                    continue
                in_query = {t: set(srcs) for t, srcs in own.items()}
                for t, srcs in shared.items():
                    in_query.setdefault(t, set()).update(srcs)
                for a in in_query:
                    for b in in_query:
                        if a >= b:
                            continue
                        pair = (a, b)
                        join_pairs.add(pair)
                        if a in own and b in own:
                            why = (oid, title, "both", a, b)
                        else:
                            shared_t, own_t = (a, b) if a not in own else (b, a)
                            kind = "filter" if "filter" in shared.get(shared_t, set()) else "hierarchy"
                            why = (oid, title, kind, shared_t, own_t)
                        needed_by.setdefault(pair, {}).setdefault(why, set()).add(widget_id)

        closure = _compute_dependency_closure(index, set(used), custom_columns=False, custom_tables=False, join_pairs=join_pairs)
        for found in closure["issues"]:
            issue(found["severity"], found["kind"], None, None, found["detail"])

        # A pair is a choice only when picking one path would leave some table out: tables dashboards use
        # directly, or that lie on the single path of another pair, are in the perspective regardless.
        anchored = {t for t, _ in used}
        for path in closure["join_paths"]:
            if len(path.get("paths") or []) == 1:
                anchored.update(path["paths"][0])

        def edge_columns(u: str, v: str) -> set[tuple[str, str]]:
            columns: set[tuple[str, str]] = set()
            for group in index.get("relations") or []:
                ours = [k for k in group if k[0] == u]
                theirs = [k for k in group if k[0] == v]
                if ours and theirs:
                    columns.update(ours)
                    columns.update(theirs)
            return columns

        def path_columns(path: list[str]) -> set[tuple[str, str]]:
            columns: set[tuple[str, str]] = set()
            for u, v in zip(path, path[1:], strict=False):
                columns |= edge_columns(u, v)
            return columns

        # Ask the query translator which tables a widget's query actually joins through. Translation only;
        # nothing is executed. Every dashboard filter is applied, even one the widget has switched off.
        sql_cache: dict[tuple[str, str], str | None] = {}

        def widget_sql(dashboard_oid: str, widget_id: str) -> str | None:
            key = (dashboard_oid, widget_id)
            if key in sql_cache:
                return sql_cache[key]
            dashboard = exports.get(dashboard_oid) or {}
            widget = next((w for w in dashboard.get("widgets") or [] if isinstance(w, dict) and w.get("oid") == widget_id), None)
            sql: str | None = None
            if widget is not None:
                widget_ds = widget.get("datasource") if isinstance(widget.get("datasource"), dict) else dashboard.get("datasource")
                metadata = _widget_query_metadata(widget, dashboard, widget_ds, sources.get(dashboard_oid, model_title), honour_ignore=False)
                if any(m["panel"] != "scope" for m in metadata):
                    response = self.api_client.post(f"/api/datasources/{model_title}/jaql/sql", data={"datasource": model_title, "metadata": metadata, "count": 1})
                    if response is not None and response.status_code == 200 and isinstance(response.text, str) and response.text.strip():
                        sql = response.text
                    else:
                        self.logger.debug(f"Query translation unavailable for widget {widget_id} of dashboard {dashboard_oid} (status={getattr(response, 'status_code', None)})")
            sql_cache[key] = sql
            return sql

        def widgets(n: int) -> str:
            return "1 widget" if n == 1 else f"{n} widgets"

        join_path_choices = []
        paths_in_use: dict[tuple[str, str], list[list[str]]] = {}  # reported pair -> the paths the engine uses (resolved pairs only)
        for path in closure["join_paths"]:
            paths = path.get("paths") or []
            if len(paths) < 2 or not any(t not in anchored for p in paths for t in p[1:-1]):
                continue
            pair = tuple(sorted((path["from"], path["to"])))
            from_name, to_name = name_of(path["from"])[0], name_of(path["to"])[0]
            reasons = []
            translated = 0
            used_paths: set[int] = set()
            for (dashboard_oid, title, kind, a, b), widget_ids in sorted(needed_by.get(pair, {}).items(), key=lambda kv: (kv[0][1], kv[0][2], name_of(kv[0][3])[0] or "", name_of(kv[0][4])[0] or "")):
                if kind == "both":
                    verb = "uses" if len(widget_ids) == 1 else "use"
                    reasons.append(f"{title}: {widgets(len(widget_ids))} {verb} both '{name_of(a)[0]}' and '{name_of(b)[0]}'")
                else:
                    reasons.append(f"{title}: dashboard {kind} on '{name_of(a)[0]}' applies to {widgets(len(widget_ids))} on '{name_of(b)[0]}'")
                for widget_id in sorted(widget_ids):
                    sql = widget_sql(dashboard_oid, widget_id)
                    if sql is None:
                        continue
                    translated += 1
                    for i, candidate in enumerate(paths):
                        if all(_sql_names_table(sql, index["tables"][t]) for t in candidate[1:-1]):
                            used_paths.add(i)
            resolved = bool(used_paths)
            candidates = "; ".join(" -> ".join(name_of(t)[0] or t for t in p[1:-1]) for p in paths)
            if resolved:
                paths_in_use[pair] = [paths[i] for i in sorted(used_paths)]
            else:
                why_not = "the translated query could not be obtained" if translated == 0 else "the translated query names none of the candidate tables"
                issue("warning", "ambiguous_join_path", None, None, f"'{from_name}' and '{to_name}' are joined by {len(paths)} equally short paths, all kept ({why_not}): {candidates}")
            join_path_choices.append(
                {
                    "from": from_name,
                    "to": to_name,
                    "needed_by": reasons,
                    "resolved": resolved,
                    "paths": [{"via": [name_of(t)[0] for t in p[1:-1]], "in_use": (i in used_paths) if resolved else None} for i, p in enumerate(paths)],
                }
            )

        # Assemble the report. Two table lists: what the engine's own join paths need (all paths where they
        # could not be determined), and the superset with every equally short path kept.
        kept_all: dict[str, set[str]] = {}
        for table_oid, column_oid in list(used) + list(closure["retained"]):
            kept_all.setdefault(table_oid, set()).add(column_oid)
        for table_oid in closure["tables"]:
            kept_all.setdefault(table_oid, set())

        kept: dict[str, set[str]] = {}
        for table_oid, column_oid in used:
            kept.setdefault(table_oid, set()).add(column_oid)
        for path in closure["join_paths"]:
            pair = tuple(sorted((path["from"], path["to"])))
            if pair in paths_in_use:
                for p in paths_in_use[pair]:
                    for t in p:
                        kept.setdefault(t, set())
                    for t, c in path_columns(p):
                        kept.setdefault(t, set()).add(c)
            else:
                for (t, c), reasons in closure["retained"].items():
                    if any(r.get("required_by") == pair for r in reasons):
                        kept.setdefault(t, set()).add(c)
                for t, reasons in closure["tables"].items():
                    if any(r.get("required_by") == pair for r in reasons):
                        kept.setdefault(t, set())

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
                dependencies.append(
                    {
                        "table": table_name,
                        "column": column_name,
                        "reason": reason["reason"],
                        "required_by": required_by_label,
                        "detail": reason.get("detail"),
                        "in_use": column_oid in kept.get(table_oid, set()),
                    }
                )
        used_tables = {t for t, _ in used}
        dependency_tables = sorted(name_of(t)[0] for t in kept if t not in used_tables)
        dependency_tables_all = sorted(name_of(t)[0] for t in closure["tables"] if t not in used_tables)

        def spec(kept_map: dict[str, set[str]]) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for table_oid in sorted(kept_map, key=lambda t: name_of(t)[0] or ""):
                table = index["tables"][table_oid]
                out.append(
                    {"table": table["name"], "columns": sorted(table["columns"][c]["name"] for c in kept_map[table_oid] if c in table["columns"] and isinstance(table["columns"][c].get("name"), str))}
                )
            return out

        tables_spec = spec(kept)
        tables_spec_all = spec(kept_all)

        # Many-to-many joins between tables the perspective keeps. A perspective inherits the root model's
        # relations, so a query spanning two kept tables joined many-to-many can fan out and double count.
        # Reported as a warning with the evidence; nothing is dropped or blocked, since a many-to-many is a
        # modelling decision. Pairs that exist only in the all-paths variant are reported with that scope.
        def relation_groups_among(kept_map: dict[str, set[str]]) -> list[dict[str, Any]]:
            pairs: list[dict[str, str]] = []
            for group in index.get("relations") or []:
                entries = [(t, c) for t, c in group if t in kept_map]
                for i, (ta, ca) in enumerate(entries):
                    for tb, cb in entries[i + 1 :]:
                        if ta == tb:
                            continue
                        na, nca = name_of(ta, ca)
                        nb, ncb = name_of(tb, cb)
                        if na and nca and nb and ncb:
                            pairs.append({"left_table": na, "left_column": nca, "right_table": nb, "right_column": ncb})
            return _group_relation_column_pairs(pairs)

        live_model = str(model_type or "").lower() == "live"
        many_to_many: list[dict[str, Any]] = []
        checked_pairs: set[tuple[Any, ...]] = set()
        for scope, kept_map in (("perspective", kept), ("all_paths", kept_all)):
            for group in relation_groups_among(kept_map):
                pair_key = (group["left_table"], tuple(group["left_columns"]), group["right_table"], tuple(group["right_columns"]))
                if pair_key in checked_pairs:
                    continue
                checked_pairs.add(pair_key)
                outcome = _many_to_many_check(self.api_client, model_title, group, live_model)
                a, b = group["left_table"], group["right_table"]
                key_a, key_b = ", ".join(group["left_columns"]), ", ".join(group["right_columns"])
                if outcome["status"] == "error":
                    issue("warning", "many_to_many_unchecked", None, None, f"the join between '{a}' ({key_a}) and '{b}' ({key_b}) could not be checked for many-to-many: {outcome['error']}")
                elif outcome["is_m2m"]:
                    where = "the perspective" if scope == "perspective" else "the all-paths variant of the perspective"
                    issue(
                        "warning",
                        "many_to_many_in_perspective",
                        None,
                        None,
                        f"'{a}' ({key_a}) and '{b}' ({key_b}) are joined many-to-many — {outcome['left_duplicate_keys']} duplicated keys in '{a}', "
                        f"{outcome['right_duplicate_keys']} in '{b}'; both tables are kept in {where}, so queries spanning them may double count",
                    )
                else:
                    continue
                many_to_many.append(
                    {
                        "table_a": a,
                        "columns_a": list(group["left_columns"]),
                        "table_b": b,
                        "columns_b": list(group["right_columns"]),
                        "duplicate_keys_a": outcome["left_duplicate_keys"],
                        "duplicate_keys_b": outcome["right_duplicate_keys"],
                        "is_m2m": outcome["is_m2m"],
                        "status": outcome["status"],
                        "error": outcome["error"],
                        "scope": scope,
                    }
                )

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
                    "copy": copies_read.get(oid, "private"),
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
            "columns_required_for_dependencies": sum(len(cols) for cols in kept.values()) - len(used),
            "tables_required_in_perspective": len(tables_spec),
            "columns_required_in_perspective": sum(len(t["columns"]) for t in tables_spec),
            "tables_required_all_paths": len(tables_spec_all),
            "columns_required_all_paths": sum(len(t["columns"]) for t in tables_spec_all),
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
        warnings_by_kind.setdefault("many_to_many_in_perspective", 0)
        result: dict[str, Any] = {
            "datamodel": model_facts,
            "summary": summary,
            "perspective_tables": tables_spec,
            "perspective_tables_all_paths": tables_spec_all,
            "join_path_choices": join_path_choices,
            "errors": errors,
            "warnings": warnings_by_kind,
        }
        if detailed:
            result.update(
                {
                    "required": {"tables": required_tables, "columns": required_columns},
                    "dependencies": {
                        "columns": dependencies,
                        "tables": dependency_tables,
                        "tables_all_paths": dependency_tables_all,
                        "join_paths": [[name_of(t)[0] for t in path["tables"]] for path in closure["join_paths"]],
                    },
                    "not_required": {"tables": excluded_tables, "columns": excluded_columns},
                    "many_to_many": many_to_many,
                    "dashboards": {"analyzed": analyzed, "failed": failed},
                    "issues": issues,
                }
            )
        return result
