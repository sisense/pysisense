from __future__ import annotations

import uuid
from typing import Any

from ..payloads import PerspectiveTableSpec
from ..utils import _build_schema_index, _extract_error_message


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

        Resolves the reference with ``get_perspectives`` and sends
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
        and column names are resolved against the model's schema before anything
        is sent, so a typo fails fast and nothing half-built is created. The
        request is the one the Sisense UI sends (``POST /api/v2/perspectives``):
        kept tables as ``include`` entries whose ``columnsDiff`` lists the kept
        columns; tables and columns not kept are simply absent. After creation
        the perspective is read back and compared with the request.

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
