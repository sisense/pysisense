import json
import logging
import os
import re
from collections import deque
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Literal

import pandas as pd
import yaml
from pandas import json_normalize

# Key names (case-insensitive) whose values are replaced by redact_secrets().
_SENSITIVE_KEYS = {
    "password",
    "token",
    "secret",
    "value",
    "apisecret",
    "accesstoken",
    "refreshtoken",
    "clientsecret",
    "privatekey",
    "authorization",
}


def redact_secrets(data):
    """
    Recursively replaces values for credential-shaped keys with a placeholder so a dict/list is safe to write
    to a debug log. Returns a new structure; the input is left untouched.

    Parameters:
        data: dict, list, or any other value

    Returns:
        The same structure with sensitive values replaced by "***REDACTED***".
    """
    if isinstance(data, dict):
        return {key: ("***REDACTED***" if str(key).lower() in _SENSITIVE_KEYS else redact_secrets(value)) for key, value in data.items()}
    if isinstance(data, list):
        return [redact_secrets(item) for item in data]
    return data


# File extensions parsed as JSON. Everything else is parsed as YAML, which is
# a superset of JSON, so a JSON document under another extension still loads.
_JSON_SUFFIXES = frozenset({".json"})


def load_config(source):
    """Load a client configuration from a file path or an in-memory mapping.

    Accepts the same settings from any of three sources: a YAML file
    (``.yaml``/``.yml``), a JSON file (``.json``), or a plain Python mapping
    such as a ``dict``. Files with any other extension are parsed as YAML,
    which also accepts JSON syntax.

    Parameters
    ----------
    source : str | os.PathLike | Mapping[str, Any]
        Path to a YAML or JSON config file, or an already-built mapping with
        the same keys (``domain``, ``token``, ``is_ssl``, ...).

    Returns
    -------
    dict[str, Any]
        A new dict holding the configuration. A mapping is shallow-copied, so
        later changes by the client never leak back into the caller's object.

    Raises
    ------
    TypeError
        If ``source`` is neither a path nor a mapping.
    ValueError
        If the file does not contain a top-level mapping of settings (for
        example an empty file, or a bare list).
    """
    if isinstance(source, Mapping):
        return dict(source)
    if not isinstance(source, (str, os.PathLike)):
        raise TypeError(f"Config source must be a file path or a mapping, not {type(source).__name__}.")

    path = os.fspath(source)
    with open(path, encoding="utf-8") as stream:
        data = json.load(stream) if os.path.splitext(path)[1].lower() in _JSON_SUFFIXES else yaml.safe_load(stream)

    if not isinstance(data, dict):
        found = "nothing" if data is None else f"a {type(data).__name__}"
        raise ValueError(f"Config file '{path}' must contain a top-level mapping of settings, but it holds {found}.")
    return data


# Maximum characters of a raw error body carried into a failure message.
_MAX_ERROR_REASON_CHARS = 300


def _extract_error_message(response, context, api_client=None):
    """
    Builds a standardized failure dict from a Sisense API response.

    Distinguishes three failure kinds:
    - HTTP error with a body: relays the Sisense reason and the HTTP status.
      The reason is taken best-effort from the JSON body ("detail", then
      "message", then "title", then "error"), looking one level inside a
      nested ``{"error": {...}}`` object as well, falling back to the raw
      body text truncated to a safe length.
    - HTTP error with an empty body: says so, with the HTTP status.
    - No response at all (connection failure): names the target domain and
      carries no invented status code.

    The body is passed through redact_secrets() before use so credential-shaped
    values never reach the returned message.

    Parameters:
        response: requests.Response or None (as returned by SisenseClient requests)
        context (str): What the caller was doing, e.g. "Failed to retrieve dashboards"
        api_client: optional SisenseClient, used to name the target domain when
            there is no response

    Returns:
        dict: {"ok": False, "error": str} always; plus {"status_code": int}
        when an HTTP status is available, and {"raw_body": str} when the body
        could not be recognized (unknown JSON shape or non-JSON text — the
        redacted, truncated dump travels there while "error" stays a clean
        sentence). The explicit "ok": False marker is the forward-compatible
        failure signal — consumers detect failures via payload.get("ok") is
        False (or the presence of "error"), never by matching an exact key
        set, since additive keys may arrive in any release.
    """
    if response is None:
        domain = getattr(api_client, "domain", None) or "the Sisense server"
        return {"ok": False, "error": f"{context}: no response from {domain} — connection failed"}

    status = response.status_code
    reason = None
    raw_body = None
    try:
        body = redact_secrets(response.json())
    except ValueError:
        raw_text = (response.text or "").strip()
        if raw_text:
            raw_body = raw_text
    else:
        if isinstance(body, dict):
            # Sisense uses both flat bodies ({"message": ...}) and nested ones
            # ({"error": {"code": 5002, "message": "Invalid token.", ...}}); read one level in.
            candidates = [body] + [body["error"]] if isinstance(body.get("error"), dict) else [body]
            for candidate in candidates:
                for key in ("detail", "message", "title", "error"):
                    value = candidate.get(key)
                    if isinstance(value, str) and value.strip():
                        reason = value.strip()
                        break
                if reason is not None:
                    break
            # Validation failures carry the specific complaint in error.subErrors[].message.
            nested = body.get("error") if isinstance(body.get("error"), dict) else None
            if reason is not None and nested is not None and isinstance(nested.get("subErrors"), list):
                details = [s["message"].strip() for s in nested["subErrors"] if isinstance(s, dict) and isinstance(s.get("message"), str) and s["message"].strip()]
                if details:
                    reason = f"{reason}: {details[0]}" + (f" (+{len(details) - 1} more)" if len(details) > 1 else "")
            if reason is None and body:
                raw_body = str(body)
        elif body not in (None, "", [], {}):
            raw_body = str(body)

    # The "error" sentence is always clean, human-authored text: either the
    # recognised Sisense reason or an honest label. Unrecognised bodies travel
    # separately in "raw_body" (redacted, truncated) so consumers with
    # different trust boundaries can relay or drop them independently.
    if reason is None:
        reason = "unrecognized error body" if raw_body else "the server returned an empty error body"
    if len(reason) > _MAX_ERROR_REASON_CHARS:
        reason = reason[:_MAX_ERROR_REASON_CHARS] + "…"

    failure: dict = {"ok": False, "error": f"{context}: {reason} (HTTP {status})", "status_code": status}
    if raw_body is not None:
        if len(raw_body) > _MAX_ERROR_REASON_CHARS:
            raw_body = raw_body[:_MAX_ERROR_REASON_CHARS] + "…"
        failure["raw_body"] = raw_body
    return failure


def convert_to_dataframe(data, logger=None):
    """
    Converts a list of dictionaries, a single dictionary, or a simple list to a pandas DataFrame.
    Automatically handles flat and nested structures.

    Parameters:
        data: dict, list of dicts, or a simple list
        logger: logging.Logger, optional logger for capturing debug/error output

    Returns:
        DataFrame: A pandas DataFrame with the data flattened as much as possible,
                   or None if conversion fails.
    """
    try:
        if isinstance(data, dict):
            df = json_normalize(data)
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                df = json_normalize(data) if any(any(isinstance(value, dict) for value in item.values()) for item in data) else pd.DataFrame(data)
            elif all(not isinstance(item, dict) for item in data):
                df = pd.DataFrame(data, columns=["Column_A"])
            else:
                raise ValueError("Data contains mixed types. Expected either a list of dictionaries or a simple list.")
        else:
            raise ValueError("Data must be a dictionary, list of dictionaries, or a plain list.")

        return df

    except ValueError as e:
        message = f"Data conversion failed: {e}"
        if logger:
            logger.error(message)
        return None


def export_to_csv(data, file_name="export.csv", logger=None):
    """
    Converts data to a DataFrame and exports it to a CSV file.

    Parameters:
        data: dict, list of dicts, or a simple list
        file_name (str): Name of the CSV file to export
        logger: logging.Logger, optional logger for capturing debug/error output
    """
    try:
        df = convert_to_dataframe(data, logger=logger)

        if df is not None:
            df.to_csv(file_name, index=False)
            message = f"Data successfully exported to {file_name}"
            if logger:
                logger.info(message)
        else:
            message = "Failed to export data due to invalid input format."
            if logger:
                logger.warning(message)

    except ValueError as e:
        message = f"Data export to CSV failed: {e}"
        if logger:
            logger.error(message)


def convert_utc_to_local(utc_str):
    """
    Converts a UTC timestamp string to the system's local timezone.
    Assumes the input is in ISO 8601 format with 'Z' suffix.

    Parameters:
        utc_str (str): A UTC timestamp string, e.g., '2025-05-14T16:24:33.537Z'

    Returns:
        str: Formatted local timestamp, e.g., '2025-05-14 12:24:33 EDT',
             or None if input is invalid.
    """
    if not utc_str:
        return None
    try:
        utc_time = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        local_time = utc_time.astimezone()
        return local_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception as e:
        return f"Invalid timestamp: {utc_str} - {str(e)}"


# ---------------------------------------------------------------------------
# JAQL column-reference parsing (shared by the dashboard and access_management
# facades; lives here because `dashboard` imports `access_management`, so a
# helper hosted in either package and imported by the other is a cycle).
# ---------------------------------------------------------------------------

# A date column used at a calendar level arrives as "[Orders.Date (Calendar)]".
_CALENDAR_SUFFIX = " (Calendar)"

# Separator of the two-bracket dim form "[Table].[Column]".
_TWO_BRACKET_SEPARATOR = "]."


def _parse_dim_candidates(dim):
    """
    Returns every plausible (table, column) reading of a JAQL ``dim`` string.

    Sisense writes column references two ways — ``[Table.Column]`` and
    ``[Table].[Column]`` — and table or column names may themselves contain
    dots, leading dots, single or double quotes, and even square brackets
    (``[Sales [EU].Amount]``, ``[trips].[."tpep_pickup_datetime]``). No grammar
    can split such names reliably, so this parser is deliberately permissive:
    it strips exactly one outer bracket pair, then offers a candidate for every
    possible split point, and leaves the real decision to resolution against the
    data model's actual table and column names. It never refuses a bracketed
    dim for containing odd characters, and it never mangles a name the way the
    old ``strip("[]").split(".", 1)`` did (which turned ``[Orders].[Amount]``
    into ``("Orders]", "[Amount")``).

    Candidate order is deterministic: the two-bracket reading first (split at
    each ``].[``), then single-bracket readings at each dot, left to right.
    Column names are returned as written; use ``_column_name_variants`` to also
    try the ``" (Calendar)"``-stripped form.

    Parameters:
        dim: the ``dim`` value from a JAQL node (any type; non-strings yield [])

    Returns:
        list[tuple[str, str]]: candidates, or [] when the value is not a
        bracketed reference with a separator at all (``"Orders.Amount"``,
        ``"[Orders]"``, ``""``, ``None``).
    """
    if not isinstance(dim, str):
        return []
    text = dim.strip()
    if len(text) < 3 or not (text.startswith("[") and text.endswith("]")):
        return []

    inner = text[1:-1]
    candidates = []

    # Two-bracket form. Names may contain "].[" only pathologically, so every
    # occurrence is offered as a split rather than assuming the first. The dot
    # inside each separator is remembered so the dot pass below does not offer
    # it again as a bogus single-bracket split ("Orders]" / "[Amount").
    separator = _TWO_BRACKET_SEPARATOR + "["
    separator_dots = set()
    start = 0
    while True:
        index = inner.find(separator, start)
        if index == -1:
            break
        separator_dots.add(index + 1)
        table, column = inner[:index], inner[index + len(separator) :]
        if table and column:
            candidates.append((table, column))
        start = index + 1

    # "].[" is Sisense's explicit separator: when present, every other dot is
    # part of a name (a leading-dot column like "[trips].[.tpep]" must not be
    # re-split at that dot), so the single-bracket pass is skipped entirely.
    if separator_dots:
        return candidates

    # Single-bracket form: a candidate per dot position. Consecutive dots yield
    # a leading-dot name on one side ("[trips..tpep]" -> ("trips", ".tpep")).
    for index, char in enumerate(inner):
        if char != ".":
            continue
        table, column = inner[:index], inner[index + 1 :]
        if table and column and (table, column) not in candidates:
            candidates.append((table, column))

    return candidates


def _column_name_variants(column):
    """
    Returns the schema column names a dashboard column reference may denote.

    A date column used at a calendar level is written ``"Date (Calendar)"`` in
    the dashboard while the schema column is just ``"Date"``. Both are returned,
    raw first, so a column literally named ``"X (Calendar)"`` still resolves.

    Parameters:
        column (str): the column part of a parsed dim

    Returns:
        list[str]: one or two names to try, raw form first.
    """
    variants = [column]
    if column.endswith(_CALENDAR_SUFFIX):
        stripped = column[: -len(_CALENDAR_SUFFIX)]
        if stripped and stripped != column:
            variants.append(stripped)
    return variants


def _parse_dim(dim):
    """
    Returns the single (table, column) reading of ``dim``, or None.

    None means unparseable or ambiguous — never a best guess. Callers that need
    a definite answer for an ambiguous dim must resolve
    ``_parse_dim_candidates`` against the real schema.

    Parameters:
        dim: the ``dim`` value from a JAQL node

    Returns:
        tuple[str, str] | None
    """
    candidates = _parse_dim_candidates(dim)
    return candidates[0] if len(candidates) == 1 else None


_UNKNOWN_DIM = "Unknown.Table"


def _split_dim(dim: str, known_columns: set[tuple[str, str]] | None = None) -> tuple[str, str]:
    """Split a dashboard ``dim`` string into ``(table, column)`` for the column walkers.

    Bracketed references go through ``_parse_dim_candidates``. A single
    candidate is returned as is. When a name itself contains dots there are
    several candidates: the first whose table and column exist in
    ``known_columns`` wins (the ``" (Calendar)"`` suffix is tolerated), and
    without a schema the first candidate is used, which is the reading the
    old first-dot split produced for ordinary names. Strings that are not a
    bracketed reference keep the old behaviour: split at the first dot, or
    ``"Unknown Column"`` when there is none.

    Parameters:
        dim (str): The raw ``dim`` value from a dashboard export.
        known_columns (set[tuple[str, str]] | None): ``(table, column)`` pairs of the data model, used to choose between readings.

    Returns:
        tuple[str, str]: The ``(table, column)`` reading of the dim.
    """
    candidates = _parse_dim_candidates(dim)
    if len(candidates) == 1:
        return candidates[0]
    if candidates:
        if known_columns:
            for table, column in candidates:
                if any((table, variant) in known_columns for variant in _column_name_variants(column)):
                    return table, column
        return candidates[0]
    text = dim.strip()
    if len(text) >= 2 and text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    if "." in text:
        table, column = text.split(".", 1)
        return table, column
    return text, "Unknown Column"


def _match_known_column(candidates: list[tuple[str, str]], known_columns: set[tuple[str, str]]) -> tuple[str, str] | None:
    """Return the schema's own ``(table, column)`` for the first candidate found in ``known_columns``.

    Tries an exact match first and a case-insensitive match second, tolerating
    the ``" (Calendar)"`` suffix on the candidate column. The tuple returned is
    the schema's spelling, so a caller comparing against the schema matches.

    Parameters:
        candidates (list[tuple[str, str]]): ``(table, column)`` readings in order of preference.
        known_columns (set[tuple[str, str]]): ``(table, column)`` pairs of the data model.

    Returns:
        tuple[str, str] | None: The matching schema pair, or ``None`` when nothing matches.
    """
    for table, column in candidates:
        for variant in _column_name_variants(column):
            if (table, variant) in known_columns:
                return table, variant
    lowered = {(table.lower(), column.lower()): (table, column) for table, column in known_columns if isinstance(table, str) and isinstance(column, str)}
    for table, column in candidates:
        for variant in _column_name_variants(column):
            hit = lowered.get((table.lower(), variant.lower()))
            if hit:
                return hit
    return None


def _reference_from_jaql(node: dict[str, Any], known_columns: set[tuple[str, str]] | None = None) -> tuple[str, str] | None:
    """Read the ``(table, column)`` a JAQL node refers to.

    Sisense writes the table and column beside ``dim`` on nearly every node,
    and those keys are authoritative: a table named ``T1.csv`` cannot be
    recovered from ``[T1.csv.C1]`` by splitting at a dot. When present they
    are used; ``dim`` is parsed only as a fallback. With ``known_columns`` the
    explicit pair and every parser candidate are checked against the schema
    first, so a case difference between the dashboard and the model resolves
    to the model's spelling.

    Parameters:
        node (dict[str, Any]): A JAQL dict, possibly carrying ``dim``, ``table`` and ``column``.
        known_columns (set[tuple[str, str]] | None): ``(table, column)`` pairs of the data model.

    Returns:
        tuple[str, str] | None: The reference, or ``None`` when the node carries no usable ``dim`` (a missing key still yields the legacy ``Unknown.Table`` placeholder).
    """
    table, column, dim = node.get("table"), node.get("column"), node.get("dim", _UNKNOWN_DIM)
    explicit = (table, column) if isinstance(table, str) and table and isinstance(column, str) and column else None
    if explicit is None and (not isinstance(dim, str) or not dim):
        return None
    if known_columns:
        candidates = ([explicit] if explicit else []) + [c for c in (_parse_dim_candidates(dim) if isinstance(dim, str) else []) if c != explicit]
        hit = _match_known_column(candidates, known_columns)
        if hit:
            return hit
    if explicit:
        return explicit
    return _split_dim(dim, known_columns)


def _datasource_title(datasource: Any) -> str | None:
    """Return the comparable title of a datasource reference, or ``None``.

    Accepts the ``datasource`` dict Sisense writes on dashboards, widgets and
    filters (``title``, ``fullname`` such as ``live:name`` or ``LocalHost/name``),
    or a bare title string.

    Parameters:
        datasource (Any): A datasource dict, a title string, or anything else.

    Returns:
        str | None: The title, lower-cased and stripped, or ``None`` when there is none.
    """
    if isinstance(datasource, str):
        title = datasource
    elif isinstance(datasource, dict):
        title = datasource.get("title")
        if not isinstance(title, str) or not title:
            fullname = datasource.get("fullname")
            title = fullname.replace("/", ":").rsplit(":", 1)[-1] if isinstance(fullname, str) else None
    else:
        return None
    title = title.strip().lower() if isinstance(title, str) else None
    return title or None


def _iter_dim_nodes(node: Any, datasource: Any, path: str):
    """Yield ``(node, datasource, path)`` for every dict under ``node`` that carries a field reference.

    A node counts when it has a non-empty string ``dim`` or explicit string
    ``table`` and ``column`` keys. Descent is generic (every dict and list
    child), so formulas nested inside formulas, ``filter.by`` measures,
    ``dimension`` wrappers and drill chains are all reached. The nearest
    enclosing ``datasource`` dict is carried down so each reference knows
    which model it belongs to.

    Parameters:
        node (Any): The subtree to scan.
        datasource (Any): The datasource inherited from the enclosing scope.
        path (str): JSON-path-like label of ``node`` for diagnostics.
    """
    if isinstance(node, dict):
        own = node.get("datasource")
        if isinstance(own, dict):
            datasource = own
        dim = node.get("dim")
        table, column = node.get("table"), node.get("column")
        if (isinstance(dim, str) and dim) or (isinstance(table, str) and table and isinstance(column, str) and column):
            yield node, datasource, path
        for key, value in node.items():
            if isinstance(value, (dict, list)):
                yield from _iter_dim_nodes(value, datasource, f"{path}.{key}")
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from _iter_dim_nodes(value, datasource, f"{path}[{index}]")


def _extract_dashboard_references(
    dashboard: dict[str, Any],
    dashboard_name: str | None = None,
    known_columns: set[tuple[str, str]] | None = None,
    logger: logging.Logger | None = None,
    datasource: Any = None,
) -> dict[str, Any]:
    """Walk an exported dashboard and collect every field reference, with diagnostics.

    The one traversal behind ``Dashboard.get_dashboard_columns``,
    ``AccessManagement.get_unused_columns_bulk`` and the perspective
    analysis. It reads dashboard filters and default filters (plain ``jaql``,
    dependent ``levels`` and ``filter.by`` measures), drill hierarchies, and
    for each widget its panel items (with nested formula ``context``,
    ``originalJaql``, conditional-format expressions and drill chains), its
    drill history, its ``query.metadata`` block and its table-state headers.
    A safety net then scans the whole document for references at locations
    not listed above and keeps them, flagged, rather than dropping them.

    Each reference's table and column come from the node's explicit keys when
    present and from parsing ``dim`` otherwise (see ``_reference_from_jaql``).
    When ``datasource`` is given, references that belong to a different
    datasource (a widget or filter pointing at another model) are skipped and
    reported instead of counted; a node without any datasource information
    inherits its parent's and is kept.

    Parameters:
        dashboard (dict[str, Any]): One dashboard as returned by the export endpoint.
        dashboard_name (str | None): Title recorded on each row; defaults to the dashboard's own title.
        known_columns (set[tuple[str, str]] | None): ``(table, column)`` pairs of the data model, used to resolve dims whose names contain dots and to adopt the model's spelling.
        logger (logging.Logger | None): Optional logger for step-by-step debug output.
        datasource (Any): Title string or datasource dict to keep references for; ``None`` keeps every reference.

    Returns:
        dict[str, Any]: ``{"rows", "issues", "skipped_widgets", "stats"}``.
            ``rows`` are ``{"dashboard_name", "source", "widget_id", "table", "column"}`` with
            ``source`` one of ``"filter"``, ``"hierarchy"``, ``"widget"``, in document order and not
            deduplicated. ``issues`` are ``{"severity", "kind", "widget_id", "path", "detail"}``.
            ``skipped_widgets`` lists widgets left out because of their datasource. ``stats`` carries counts.
    """
    result: dict[str, Any] = {"rows": [], "issues": [], "skipped_widgets": [], "stats": {}}
    if not isinstance(dashboard, dict):
        return result
    name = dashboard_name if dashboard_name is not None else dashboard.get("title", "Unknown Dashboard")
    rows: list[dict[str, Any]] = result["rows"]
    issues: list[dict[str, Any]] = result["issues"]
    wanted = _datasource_title(datasource)
    dashboard_ds = dashboard.get("datasource") if isinstance(dashboard.get("datasource"), dict) else None
    visited: set[int] = set()

    def issue(severity: str, kind: str, widget_id: str, path: str, detail: str) -> None:
        issues.append({"severity": severity, "kind": kind, "widget_id": widget_id, "path": path, "detail": detail})
        if logger:
            logger.debug(f"{kind} at {path}: {detail}")

    def belongs(node_ds: Any) -> bool:
        if wanted is None:
            return True
        title = _datasource_title(node_ds)
        return title is None or title == wanted

    def add(source: str, widget_id: str, node: dict[str, Any], node_ds: Any, path: str) -> None:
        visited.add(id(node))
        if not belongs(node_ds):
            issue("info", "reference_other_datasource", widget_id, path, f"belongs to datasource {_datasource_title(node_ds)!r}, not counted")
            return
        reference = _reference_from_jaql(node, known_columns)
        if reference is None:
            issue("warning", "unreadable_dim", widget_id, path, f"dim {node.get('dim')!r} carries no usable field reference")
            return
        dim = node.get("dim")
        explicit = isinstance(node.get("table"), str) and isinstance(node.get("column"), str)
        if not explicit and isinstance(dim, str):
            candidates = _parse_dim_candidates(dim)
            if not candidates:
                issue("error", "unreadable_dim", widget_id, path, f"dim {dim!r} is not a bracketed field reference; read as {reference!r}")
            elif len(candidates) > 1 and not (known_columns and _match_known_column(candidates, known_columns)):
                issue("warning", "ambiguous_dim", widget_id, path, f"dim {dim!r} has {len(candidates)} readings; took {reference!r}")
        table, column = reference
        rows.append({"dashboard_name": name, "source": source, "widget_id": widget_id, "table": table, "column": column})

    def scan(source: str, widget_id: str, root: Any, inherited_ds: Any, path: str) -> None:
        for node, node_ds, node_path in _iter_dim_nodes(root, inherited_ds, path):
            add(source, widget_id, node, node_ds, node_path)

    # Dashboard-level filters, default filters, drill hierarchies
    for key in ("filters", "defaultFilters"):
        entries = dashboard.get(key) or []
        for index, entry in enumerate(entries):
            if isinstance(entry, dict):
                scan("filter", "N/A", entry, dashboard_ds, f"$.{key}[{index}]")
    hierarchies = dashboard.get("hierarchies") or []
    for index, hierarchy in enumerate(hierarchies):
        if isinstance(hierarchy, dict):
            # A drill hierarchy names its model in elasticubeTitle rather than in a datasource object.
            cube = hierarchy.get("elasticubeTitle")
            hierarchy_ds = {"title": cube} if isinstance(cube, str) and cube.strip() else dashboard_ds
            scan("hierarchy", "N/A", hierarchy, hierarchy_ds, f"$.hierarchies[{index}]")

    # Widgets
    widgets = dashboard.get("widgets") or []
    widgets_scanned = 0
    for index, widget in enumerate(widgets):
        if not isinstance(widget, dict):
            continue
        widget_id = widget.get("oid", "Unknown Widget")
        widget_path = f"$.widgets[{index}]"
        widget_ds = widget.get("datasource") if isinstance(widget.get("datasource"), dict) else dashboard_ds
        if not belongs(widget_ds):
            result["skipped_widgets"].append({"widget_id": widget_id, "title": widget.get("title"), "type": widget.get("type"), "datasource": _datasource_title(widget_ds)})
            issue("info", "widget_other_datasource", widget_id, widget_path, f"widget datasource {_datasource_title(widget_ds)!r} is not {wanted!r}; skipped")
            for node, _ds, _path in _iter_dim_nodes(widget, widget_ds, widget_path):
                visited.add(id(node))
            continue
        widgets_scanned += 1
        widget_type = widget.get("type")
        if isinstance(widget_type, str) and widget_type.lower() == "blox":
            issue("warning", "blox_widget", widget_id, widget_path, "BloX widgets can reference fields inside actions and templates that cannot be verified")
        script = widget.get("script")
        if isinstance(script, str) and script.strip():
            issue("warning", "script_present", widget_id, widget_path + ".script", "widget script may reference fields; not analysed")
        metadata = widget.get("metadata") if isinstance(widget.get("metadata"), dict) else {}
        for panel_index, panel in enumerate(metadata.get("panels") or []):
            if isinstance(panel, dict):
                scan("widget", widget_id, panel.get("items"), widget_ds, f"{widget_path}.metadata.panels[{panel_index}].items")
        scan("widget", widget_id, metadata.get("drillHistory"), widget_ds, f"{widget_path}.metadata.drillHistory")
        query = widget.get("query")
        if isinstance(query, dict):
            scan("widget", widget_id, query.get("metadata"), widget_ds, f"{widget_path}.query.metadata")
        style = widget.get("style")
        if isinstance(style, dict) and isinstance(style.get("tableState"), dict):
            scan("widget", widget_id, style["tableState"].get("headers"), widget_ds, f"{widget_path}.style.tableState.headers")

    script = dashboard.get("script")
    if isinstance(script, str) and script.strip():
        issue("warning", "script_present", "N/A", "$.script", "dashboard script may reference fields; not analysed")

    # Safety net: any reference at a location not listed above is kept and flagged.
    unclassified = 0
    for node, node_ds, path in _iter_dim_nodes(dashboard, dashboard_ds, "$"):
        if id(node) in visited:
            continue
        unclassified += 1
        widget_id = "N/A"
        if path.startswith("$.widgets["):
            try:
                widget_id = widgets[int(path[len("$.widgets[") : path.index("]")])].get("oid", "Unknown Widget")
            except (ValueError, IndexError, AttributeError):
                widget_id = "Unknown Widget"
        issue("warning", "unclassified_location", widget_id, path, "field reference at an unexpected location; kept")
        add("widget" if path.startswith("$.widgets[") else "filter", widget_id, node, node_ds, path)

    result["stats"] = {
        "filters": len(dashboard.get("filters") or []),
        "default_filters": len(dashboard.get("defaultFilters") or []),
        "hierarchies": len(hierarchies),
        "widgets": len(widgets),
        "widgets_scanned": widgets_scanned,
        "widgets_skipped": len(result["skipped_widgets"]),
        "unclassified": unclassified,
        "rows": len(rows),
    }
    if logger:
        logger.debug(f"Dashboard '{name}': {result['stats']}")
    return result


def _extract_dashboard_columns(
    dashboard: dict[str, Any],
    dashboard_name: str | None = None,
    known_columns: set[tuple[str, str]] | None = None,
    logger: logging.Logger | None = None,
    datasource: Any = None,
) -> list[dict[str, Any]]:
    """Walk an exported dashboard and list every column reference it contains.

    Thin wrapper over ``_extract_dashboard_references`` returning only the
    rows, for callers that do not need the diagnostics. See that function
    for what is read and how datasource filtering behaves.

    Parameters:
        dashboard (dict[str, Any]): One dashboard as returned by the export endpoint.
        dashboard_name (str | None): Title recorded on each row; defaults to the dashboard's own title.
        known_columns (set[tuple[str, str]] | None): ``(table, column)`` pairs of the data model.
        logger (logging.Logger | None): Optional logger for step-by-step debug output.
        datasource (Any): Title string or datasource dict to keep references for; ``None`` keeps every reference.

    Returns:
        list[dict[str, Any]]: Rows with ``dashboard_name``, ``source`` (``"filter"``, ``"hierarchy"`` or ``"widget"``), ``widget_id``, ``table`` and ``column``.
    """
    return _extract_dashboard_references(dashboard, dashboard_name, known_columns, logger, datasource)["rows"]


def _discover_dashboards_on_datasource(api_client: Any, logger: logging.Logger | None, datasource_title: str, deep: bool = False) -> dict[str, Any]:
    """Find every dashboard that uses a datasource, from the admin listing (plus an optional deep scan).

    Shared by ``Dashboard.get_dashboards_by_datasource`` and the unused-columns
    check. Reads ``GET /api/v1/dashboards/admin`` once, collapses repeated oids,
    and matches each dashboard on its own ``datasource`` (``"dashboard"``) or on
    any entry of its ``widgetsDatasources`` summary (``"widget"``), comparing
    titles case-insensitively. With ``deep`` true, dashboards whose summary is
    empty are exported in batches of 20 and their widgets inspected directly.

    Parameters:
        api_client (Any): The shared ``SisenseClient``.
        logger (logging.Logger | None): Logger for debug output.
        datasource_title (str): Title of the datasource (data model) to look for.
        deep (bool): Export dashboards with an empty widget-datasource summary and inspect their widgets.

    Returns:
        dict[str, Any]: ``{"matches": {oid: "dashboard" | "widget"}, "dashboards": {oid: listing_entry}}``
            on success, or the standard ``{"ok": False, "error": ...}`` dict on failure.
    """
    wanted = _datasource_title(datasource_title)
    response = api_client.get("/api/v1/dashboards/admin", params={"dashboardType": "owner"})
    if response is None or response.status_code != 200:
        return _extract_error_message(response, "Failed to list dashboards", api_client)
    try:
        listing = response.json()
    except Exception:
        return {"ok": False, "error": "Failed to parse the dashboard listing."}
    if not isinstance(listing, list):
        return {"ok": False, "error": "Unexpected dashboard listing structure."}

    dashboards: dict[str, dict[str, Any]] = {}
    for entry in listing:
        if isinstance(entry, dict) and isinstance(entry.get("oid"), str):
            dashboards.setdefault(entry["oid"], entry)  # the listing can repeat an oid

    matches: dict[str, str] = {}
    unsummarized: list[str] = []
    for oid, entry in dashboards.items():
        if _datasource_title(entry.get("datasource")) == wanted:
            matches[oid] = "dashboard"
            continue
        summary = entry.get("widgetsDatasources")
        if isinstance(summary, list) and summary:
            if any(_datasource_title(ds) == wanted for ds in summary if isinstance(ds, dict)):
                matches[oid] = "widget"
        else:
            unsummarized.append(oid)

    if deep and unsummarized:
        if logger:
            logger.debug(f"Deep scan: exporting {len(unsummarized)} dashboards with no widget-datasource summary")
        for start in range(0, len(unsummarized), 20):
            batch = unsummarized[start : start + 20]
            export = api_client.get("/api/v1/dashboards/export", params={"dashboardIds": ",".join(batch), "adminAccess": "true"})
            if export is None or export.status_code != 200:
                return _extract_error_message(export, "Failed to export dashboards for the deep scan", api_client)
            try:
                exported = export.json()
            except Exception:
                return {"ok": False, "error": "Failed to parse a dashboard export during the deep scan."}
            for dashboard in exported if isinstance(exported, list) else []:
                if not isinstance(dashboard, dict):
                    continue
                for widget in dashboard.get("widgets") or []:
                    if isinstance(widget, dict) and _datasource_title(widget.get("datasource")) == wanted:
                        matches[dashboard.get("oid")] = "widget"
                        break
    if logger:
        logger.debug(f"Discovered {len(matches)} dashboards on datasource '{datasource_title}' ({sum(m == 'widget' for m in matches.values())} via widgets only)")
    return {"matches": matches, "dashboards": dashboards}


# ---------------------------------------------------------------------------
# Column dependencies inside a data model: what a set of columns needs to work.
# Pure functions over a /datamodels/{oid}/schema payload. Used by the perspective
# analysis; every ambiguity resolves toward keeping more, and anything that
# cannot be resolved is reported as an issue rather than dropped.
# ---------------------------------------------------------------------------

_ColumnKey = tuple[str, str]  # (table_oid, column_oid)

_TWO_PART_TOKEN = re.compile(r"\[([^\[\]]+)\]\s*\.\s*\[([^\[\]]+)\]")
_ONE_PART_TOKEN = re.compile(r"\[([^\[\]]+)\]")
_WORD_TOKEN = re.compile(r"(?<![\w\[\]'\"])([A-Za-z_][A-Za-z0-9_]*)(?![\w\]'\"])")
_SQL_FROM_JOIN = re.compile(
    r"\b(?:from|join)\s+(?:\[([^\]]+)\]|([A-Za-z_][\w.]*))(?:\s+(?:as\s+)?(?!on\b|where\b|join\b|left\b|right\b|inner\b|outer\b|full\b|cross\b|group\b|order\b|limit\b|union\b|select\b)([A-Za-z_]\w*))?",
    re.IGNORECASE,
)
_SQL_QUALIFIED = re.compile(r"(?:\[([^\]]+)\]|([A-Za-z_]\w*))\s*\.\s*(?:\[([^\]]+)\]|([A-Za-z_]\w*))")
_SQL_SELECT_STAR = re.compile(r"\bselect\s+(?:distinct\s+)?(?:\w+\s*\.\s*)?\*", re.IGNORECASE)
_SQL_COMPLEX = re.compile(r"\bwith\b|\(\s*select\b", re.IGNORECASE)
_FORMULA_WORDS_TO_IGNORE = {"case", "when", "then", "else", "end", "and", "or", "not", "in", "is", "null", "true", "false", "like", "between"}


def _build_schema_index(schema: dict[str, Any]) -> dict[str, Any]:
    """Index a data model schema by table and column oid for fast, name-tolerant lookups.

    Parameters
    ----------
    schema : dict[str, Any]
        The payload of ``GET /api/v2/datamodels/{oid}/schema``.

    Returns
    -------
    dict[str, Any]
        ``{"tables": {table_oid: {...}}, "tables_by_name": {lower_name: [table_oid]}, "relations": [...]}``.
        Each table entry carries ``oid``, ``name``, ``type``, ``dataset``, ``sql`` (custom-table SQL or
        ``None``), ``columns`` (``{column_oid: {"oid", "name", "id", "display_name", "expression", "is_custom"}}``),
        ``columns_by_name`` (``{lower identity name: column_oid}``) and ``columns_by_alias`` (``{lower display
        or original name: column_oid}``, for columns renamed after dashboards were built). ``relations`` is a list of
        ``(table_oid, column_oid)`` groups, one per relation. Entries that are not dicts are skipped.
    """
    tables: dict[str, dict[str, Any]] = {}
    tables_by_name: dict[str, list[str]] = {}
    if not isinstance(schema, dict):
        return {"tables": tables, "tables_by_name": tables_by_name, "relations": []}
    for dataset in schema.get("datasets") or []:
        if not isinstance(dataset, dict):
            continue
        dataset_schema = dataset.get("schema") if isinstance(dataset.get("schema"), dict) else {}
        for table in dataset_schema.get("tables") or []:
            if not isinstance(table, dict) or not isinstance(table.get("oid"), str):
                continue
            expression = table.get("expression")
            sql = expression.get("expression") if isinstance(expression, dict) else expression if isinstance(expression, str) else None
            columns: dict[str, dict[str, Any]] = {}
            columns_by_name: dict[str, str] = {}
            columns_by_alias: dict[str, str] = {}  # display name and original (physical) name -> oid
            for column in table.get("columns") or []:
                if not isinstance(column, dict) or not isinstance(column.get("oid"), str):
                    continue
                entry = {
                    "oid": column["oid"],
                    "name": column.get("name"),
                    "id": column.get("id") if isinstance(column.get("id"), str) else None,
                    "display_name": column.get("displayName") if isinstance(column.get("displayName"), str) else None,
                    "expression": column.get("expression") if isinstance(column.get("expression"), str) else None,
                    "is_custom": bool(column.get("isCustom")),
                }
                columns[column["oid"]] = entry
                if isinstance(entry["name"], str):
                    columns_by_name.setdefault(entry["name"].strip().lower(), column["oid"])
                for alias in (entry["display_name"], entry["id"]):
                    if isinstance(alias, str) and alias.strip():
                        columns_by_alias.setdefault(alias.strip().lower(), column["oid"])
            name = table.get("name")
            tables[table["oid"]] = {
                "oid": table["oid"],
                "name": name,
                "type": table.get("type"),
                "dataset": dataset.get("oid"),
                "sql": sql if isinstance(sql, str) and sql.strip() else None,
                "columns": columns,
                "columns_by_name": columns_by_name,
                "columns_by_alias": columns_by_alias,
            }
            if isinstance(name, str):
                tables_by_name.setdefault(name.strip().lower(), []).append(table["oid"])
    relations: list[list[_ColumnKey]] = []
    for relation in schema.get("relations") or []:
        if not isinstance(relation, dict):
            continue
        group: list[_ColumnKey] = []
        for end in relation.get("columns") or []:
            if isinstance(end, dict) and isinstance(end.get("table"), str) and isinstance(end.get("column"), str):
                group.append((end["table"], end["column"]))
        if len(group) >= 2:
            relations.append(group)
    return {"tables": tables, "tables_by_name": tables_by_name, "relations": relations}


def _compute_dependency_closure(
    index: dict[str, Any],
    used: set[_ColumnKey],
    *,
    join_paths: bool = True,
    custom_columns: bool = True,
    custom_tables: bool = True,
    custom_table_columns: Literal["all", "parsed"] = "all",
) -> dict[str, Any]:
    """Compute everything a set of columns depends on beyond the columns themselves.

    Runs three closures to a fixpoint over the retained set: join paths between
    every pair of retained tables (both join columns of every edge on every
    shortest path, and the intermediate tables), custom-column formulas
    (the columns they read), and custom-table SQL (the tables and columns it
    selects from). Each retained entry carries the reasons it was kept.

    Parameters
    ----------
    index : dict[str, Any]
        The result of ``_build_schema_index``.
    used : set[tuple[str, str]]
        ``(table_oid, column_oid)`` pairs dashboards use directly.
    join_paths : bool, optional
        Retain join columns and intermediate tables between used tables. Default ``True``.
    custom_columns : bool, optional
        Retain the columns a retained custom column's formula reads. Default ``True``.
    custom_tables : bool, optional
        Retain what a retained custom table's SQL selects from. Default ``True``.
    custom_table_columns : {"all", "parsed"}, optional
        ``"all"`` keeps every column of every table a custom table's SQL references;
        ``"parsed"`` keeps only the columns the SQL names (``select *`` still keeps all).

    Returns
    -------
    dict[str, Any]
        ``{"retained": {(table_oid, column_oid): [reason, ...]}, "tables": {table_oid: [reason, ...]},
        "join_paths": [{"from", "to", "tables"}], "issues": [{"severity", "kind", "detail"}], "options": {...}}``.
        ``retained`` holds dependency columns only (never the ``used`` input); ``tables`` lists tables
        kept for a table-level reason — an intermediate table on a join path or the source table of a
        custom table — with their reasons, whether or not they also have retained columns.
        A reason is ``{"reason", "required_by", "detail"}``.
    """
    tables: dict[str, dict[str, Any]] = index.get("tables") or {}
    retained: dict[_ColumnKey, list[dict[str, Any]]] = {}
    extra_tables: dict[str, list[dict[str, Any]]] = {}
    issues: list[dict[str, Any]] = []
    join_path_report: list[dict[str, Any]] = []
    used = {key for key in used if key[0] in tables}

    def keep(key: _ColumnKey, reason: str, required_by: Any, detail: str) -> bool:
        if key in used or key[0] not in tables or key[1] not in tables[key[0]]["columns"]:
            return False
        reasons = retained.setdefault(key, [])
        if any(r["reason"] == reason and r["required_by"] == required_by for r in reasons):
            return False
        reasons.append({"reason": reason, "required_by": required_by, "detail": detail})
        return True

    def keep_table(table_oid: str, reason: str, required_by: Any, detail: str) -> None:
        reasons = extra_tables.setdefault(table_oid, [])
        if not any(r["reason"] == reason and r["required_by"] == required_by for r in reasons):
            reasons.append({"reason": reason, "required_by": required_by, "detail": detail})

    def issue(severity: str, kind: str, detail: str) -> None:
        if not any(i["kind"] == kind and i["detail"] == detail for i in issues):
            issues.append({"severity": severity, "kind": kind, "detail": detail})

    def retained_tables() -> set[str]:
        return {t for t, _ in used} | {t for t, _ in retained} | set(extra_tables)

    def endpoint_tables() -> set[str]:
        # Tables needed in their own right: used, or retained for a reason other
        # than lying on a join path. Only these are joined to each other.
        needed = {t for t, _ in used}
        needed |= {t for (t, _), reasons in retained.items() if any(r["reason"] != "join_column" for r in reasons)}
        needed |= {t for t, reasons in extra_tables.items() if any(r["reason"] != "join_path_table" for r in reasons)}
        return needed

    seen_paths: set[tuple[str, str]] = set()
    processed_columns: set[_ColumnKey] = set()
    processed_tables: set[str] = set()

    for _ in range(50):  # fixpoint; each pass only adds
        changed = False
        if join_paths:
            changed |= _close_join_paths(index, endpoint_tables(), keep, keep_table, issue, join_path_report, seen_paths)
        if custom_columns:
            for key in sorted((set(used) | set(retained)) - processed_columns):
                processed_columns.add(key)
                changed |= _close_custom_column(index, key, keep, issue)
        if custom_tables:
            for table_oid in sorted(retained_tables() - processed_tables):
                processed_tables.add(table_oid)
                changed |= _close_custom_table(index, table_oid, custom_table_columns, keep, keep_table, issue)
        if not changed:
            break
    else:
        issue("warning", "closure_not_converged", "dependency closure stopped after 50 passes")

    return {
        "retained": retained,
        "tables": extra_tables,
        "join_paths": join_path_report,
        "issues": issues,
        "options": {"join_paths": join_paths, "custom_columns": custom_columns, "custom_tables": custom_tables, "custom_table_columns": custom_table_columns},
    }


def _close_join_paths(index, tables_needed, keep, keep_table, issue, report, seen_paths) -> bool:
    """Retain the join columns on every shortest path between each pair of needed tables.

    ``tables_needed`` are the endpoint tables only; a table that lies on a path
    is kept as an intermediate but never becomes an endpoint itself.
    """
    tables = index["tables"]
    adjacency: dict[str, dict[str, list[tuple[_ColumnKey, _ColumnKey]]]] = {}
    for group in index.get("relations") or []:
        for a in group:
            for b in group:
                if a[0] != b[0] and a[0] in tables and b[0] in tables:
                    adjacency.setdefault(a[0], {}).setdefault(b[0], []).append((a, b))
    changed = False
    needed = sorted(t for t in tables_needed if t in tables)
    for i, source in enumerate(needed):
        distances = _bfs(adjacency, source)
        for target in needed[i + 1 :]:
            if (source, target) in seen_paths:
                continue
            seen_paths.add((source, target))
            if target not in distances:
                issue("info", "tables_not_joined", f"no relation path between '{tables[source]['name']}' and '{tables[target]['name']}'")
                continue
            back = _bfs(adjacency, target)
            total = distances[target]
            on_path = {t for t in distances if t in back and distances[t] + back[t] == total}
            for u in on_path:
                for v, pairs in adjacency.get(u, {}).items():
                    if v in on_path and distances.get(v) == distances[u] + 1:
                        for a, b in pairs:
                            label = f"join {tables[a[0]]['name']} -> {tables[b[0]]['name']} on the path {tables[source]['name']} .. {tables[target]['name']}"
                            changed |= keep(a, "join_column", (source, target), label)
                            changed |= keep(b, "join_column", (source, target), label)
            for t in on_path - set(needed):
                keep_table(t, "join_path_table", (source, target), f"intermediate table between '{tables[source]['name']}' and '{tables[target]['name']}'")
                changed = True
            report.append({"from": source, "to": target, "tables": sorted(on_path, key=lambda t: distances[t])})
    return changed


def _bfs(adjacency, start) -> dict[str, int]:
    distances = {start: 0}
    queue = deque([start])
    while queue:
        node = queue.popleft()
        for neighbour in adjacency.get(node, {}):
            if neighbour not in distances:
                distances[neighbour] = distances[node] + 1
                queue.append(neighbour)
    return distances


def _find_table(index, name: str) -> list[str]:
    return index["tables_by_name"].get(name.strip().lower(), [])


def _close_custom_column(index, key: _ColumnKey, keep, issue) -> bool:
    """Retain the columns a custom column's formula reads (``[Col]``, ``[Table].[Col]`` or bare names)."""
    tables = index["tables"]
    table = tables[key[0]]
    column = table["columns"][key[1]]
    expression = column.get("expression")
    if not expression:
        return False
    changed = False
    label = f"read by custom column '{table['name']}'.'{column['name']}'"
    rest = expression
    for match in _TWO_PART_TOKEN.finditer(expression):
        table_name, column_name = match.group(1), match.group(2)
        targets = _find_table(index, table_name)
        if not targets:
            issue("warning", "custom_column_token_unresolved", f"{label}: table '{table_name}' not found")
            continue
        for target in targets:
            column_oid = tables[target]["columns_by_name"].get(column_name.strip().lower())
            if column_oid:
                changed |= keep((target, column_oid), "custom_column_expression", key, label)
            else:
                issue("warning", "custom_column_token_unresolved", f"{label}: column '{table_name}'.'{column_name}' not found")
    rest = _TWO_PART_TOKEN.sub(" ", rest)
    for match in _ONE_PART_TOKEN.finditer(rest):
        name = match.group(1)
        column_oid = table["columns_by_name"].get(name.strip().lower())
        if column_oid:
            changed |= keep((key[0], column_oid), "custom_column_expression", key, label)
            continue
        for target in _find_table(index, name):
            for other_oid in tables[target]["columns"]:
                changed |= keep((target, other_oid), "custom_column_expression", key, f"{label}: whole table '{name}' referenced")
        if not _find_table(index, name):
            issue("warning", "custom_column_token_unresolved", f"{label}: '[{name}]' matches no column of the table and no table")
    rest = _ONE_PART_TOKEN.sub(" ", rest)
    rest = re.sub(r"'[^']*'|\"[^\"]*\"", " ", rest)
    for match in _WORD_TOKEN.finditer(rest):
        word = match.group(1)
        if word.lower() in _FORMULA_WORDS_TO_IGNORE:
            continue
        column_oid = table["columns_by_name"].get(word.lower())
        if column_oid:
            changed |= keep((key[0], column_oid), "custom_column_expression", key, f"{label}: bare name '{word}'")
    return changed


def _close_custom_table(index, table_oid: str, mode: str, keep, keep_table, issue) -> bool:
    """Retain what a custom table's SQL selects from."""
    tables = index["tables"]
    table = tables[table_oid]
    sql = table.get("sql")
    if not sql:
        return False
    changed = False
    label = f"selected by custom table '{table['name']}'"
    if _SQL_COMPLEX.search(sql):
        issue("warning", "custom_table_sql_complex", f"{label}: SQL uses WITH or a nested SELECT; parsed heuristically")
    aliases: dict[str, str] = {}
    referenced: list[str] = []
    for match in _SQL_FROM_JOIN.finditer(sql):
        name = match.group(1) or match.group(2)
        alias = match.group(3)
        targets = _find_table(index, name)
        if not targets:
            issue("error", "custom_table_sql_unresolved", f"{label}: table '{name}' not found in the model")
            continue
        for target in targets:
            referenced.append(target)
            aliases[name.strip().lower()] = target
            if alias:
                aliases[alias.lower()] = target
    if not referenced:
        if not _SQL_FROM_JOIN.search(sql):
            issue("warning", "custom_table_sql_no_source", f"{label}: no FROM/JOIN source found")
        return changed
    keep_all = mode == "all" or bool(_SQL_SELECT_STAR.search(sql))
    for target in referenced:
        keep_table(target, "custom_table_source", table_oid, label)
        changed = True
        if keep_all:
            for column_oid in tables[target]["columns"]:
                changed |= keep((target, column_oid), "custom_table_source", table_oid, label)
    if not keep_all:
        for match in _SQL_QUALIFIED.finditer(sql):
            qualifier = (match.group(1) or match.group(2) or "").strip().lower()
            column_name = (match.group(3) or match.group(4) or "").strip().lower()
            target = aliases.get(qualifier)
            if target is None:
                continue
            column_oid = tables[target]["columns_by_name"].get(column_name)
            if column_oid:
                changed |= keep((target, column_oid), "custom_table_source", table_oid, label)
            else:
                issue("warning", "custom_table_sql_column_unresolved", f"{label}: column '{column_name}' not found in '{tables[target]['name']}'")
        if len(referenced) == 1:
            target = referenced[0]
            unqualified = _SQL_QUALIFIED.sub(" ", sql)
            for match in _ONE_PART_TOKEN.finditer(unqualified):
                column_oid = tables[target]["columns_by_name"].get(match.group(1).strip().lower())
                if column_oid:
                    changed |= keep((target, column_oid), "custom_table_source", table_oid, label)
    return changed
