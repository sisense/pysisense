import json
import logging
import os
from collections.abc import Mapping
from datetime import datetime
from typing import Any

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
      "message", then "title", then "error"), falling back to the raw body
      text truncated to a safe length.
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
            for key in ("detail", "message", "title", "error"):
                value = body.get(key)
                if isinstance(value, str) and value.strip():
                    reason = value.strip()
                    break
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
            scan("hierarchy", "N/A", hierarchy, dashboard_ds, f"$.hierarchies[{index}]")

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
