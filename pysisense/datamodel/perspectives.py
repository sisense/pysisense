from __future__ import annotations

from typing import Any

from ..utils import _extract_error_message


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
