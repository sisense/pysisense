"""Machine-readable payload contracts for pysisense dict parameters.

TypedDict definitions describing the shape of dict payloads accepted by SDK
methods, so downstream tooling can derive parameter schemas by introspection
(``typing.get_type_hints``, ``__required_keys__`` / ``__optional_keys__``)
instead of hand-maintaining parallel field tables.

TypedDicts are structural: plain dicts continue to work everywhere, so
annotating a parameter with one of these types is non-breaking for existing
callers. Required/optional fields are expressed with the two-class inheritance
pattern (a ``total=True`` base for required keys, a ``total=False`` subclass
for optional ones), which populates ``__required_keys__`` and
``__optional_keys__`` correctly on all supported Python versions (3.10+).

Free-form payloads whose shape is a language rather than a field list (JAQL
queries, metadata queries, Blox action JSON, encryption bodies) intentionally
remain ``dict[str, Any]``.

Payloads that allow provider- or environment-specific extras (notebooks,
measures, connections) declare only the fields the SDK itself documents;
additional keys are passed through to the API unchanged.
"""

from typing import Any, TypedDict


class _CreateUserRequired(TypedDict):
    email: str
    role: str


class CreateUserPayload(_CreateUserRequired, total=False):
    """Payload for ``AccessManagement.create_user``.

    Required: ``email``, ``role`` (role name, e.g. ``"viewer"`` — resolved to
    ``roleId``). Optional: ``userName``, ``firstName``, ``lastName``,
    ``groups`` (group names, resolved to IDs), ``password``, ``preferences``.
    """

    userName: str
    firstName: str
    lastName: str
    groups: list[str]
    password: str
    preferences: dict[str, Any]


class UpdateUserPayload(TypedDict, total=False):
    """Payload for ``AccessManagement.update_user``.

    All fields optional — only include fields you want to change; omitted
    fields are not modified. ``role`` and ``groups`` are name-based and
    resolved to IDs before the request.
    """

    email: str
    userName: str
    firstName: str
    lastName: str
    role: str
    groups: list[str]
    password: str
    preferences: dict[str, Any]


class _NotebookCreateRequired(TypedDict):
    notebookType: str
    displayName: str


class NotebookCreatePayload(_NotebookCreateRequired, total=False):
    """Payload for ``CustomCode.create_notebook``.

    Required: ``notebookType`` (e.g. ``"CustomCodeTransformation"``),
    ``displayName``. Additional Sisense notebook manifest fields may be
    included and are passed through unchanged.
    """

    description: str


class NotebookUpdatePayload(TypedDict, total=False):
    """Payload for ``CustomCode.update_notebook``.

    All fields optional — only include fields you want to change. Additional
    Sisense notebook manifest fields may be included and are passed through
    unchanged.
    """

    notebookType: str
    displayName: str
    description: str


class _ConnectionRequired(TypedDict):
    provider: str
    name: str
    parameters: dict[str, Any]


class ConnectionPayload(_ConnectionRequired, total=False):
    """Full connection object for ``DataModel.create_connections``.

    Matches the shape produced by ``generate_connections_payload``. Required:
    ``provider``, ``name``, ``parameters`` (provider-specific). Optional:
    ``enabled``, ``createdByUser``, ``description``, ``supportedModelTypes``.
    """

    enabled: bool
    createdByUser: bool
    description: str
    supportedModelTypes: list[str]


class ConnectionUpdatePayload(TypedDict, total=False):
    """Partial connection object for ``DataModel.update_connection``.

    All fields optional — only fields present are sent; omitted fields are not
    modified on the server. Supported keys depend on the Sisense connection
    type.
    """

    name: str
    description: str
    provider: str
    parameters: dict[str, Any]
    enabled: bool


class _AthenaParamsRequired(TypedDict):
    name: str
    region: str
    s3_output_location: str
    aws_access_key: str
    aws_secret_key: str


class AthenaConnectionParams(_AthenaParamsRequired, total=False):
    """``connection_params`` for ``generate_connections_payload("Athena", ...)``."""

    description: str
    schema: str
    additional_parameters: str


class _DataBricksParamsRequired(TypedDict):
    name: str
    connection_string: str
    token: str


class DataBricksConnectionParams(_DataBricksParamsRequired, total=False):
    """``connection_params`` for ``generate_connections_payload("DataBricks", ...)``."""

    description: str
    use_dynamic_schema: bool
    schema: str


class _BigQueryParamsRequired(TypedDict):
    name: str
    service_account_key_path: str


class BigQueryConnectionParams(_BigQueryParamsRequired, total=False):
    """``connection_params`` for ``generate_connections_payload("BigQuery", ...)``."""

    description: str
    use_service_account: bool
    use_proxy_server: bool
    use_dynamic_schema: bool
    record_field_flattening_level: str
    unnest_arrays: bool
    allow_large_results: bool
    use_storage_api: bool
    additional_parameters: str
    database: str


class _RedShiftParamsRequired(TypedDict):
    server: str
    username: str
    password: str


class RedShiftConnectionParams(_RedShiftParamsRequired, total=False):
    """``connection_params`` for ``generate_connections_payload("RedShift", ...)``."""

    name: str
    description: str
    default_database: str
    additional_parameters: str


class DatasourceRef(TypedDict):
    """Datasource reference used inside metadata payloads."""

    title: str
    fullname: str


class _MeasureRequired(TypedDict):
    title: str
    datasource: DatasourceRef


class MeasurePayload(_MeasureRequired, total=False):
    """Payload for ``Metadata.add_datasource_measure``.

    Required: ``title``, ``datasource``. Additional Sisense metadata fields
    (expression, context, table/column references, etc.) may be included and
    are passed through unchanged.
    """


class _PluginSnapshotRequired(TypedDict):
    plugins: list[str]


class PluginSnapshot(_PluginSnapshotRequired, total=False):
    """Snapshot dict produced by ``Plugins.save_snapshot`` and consumed by
    ``Plugins.restore_snapshot``.

    Required: ``plugins`` (``folderName`` values that should be enabled — all
    other plugins will be disabled). Optional: ``created`` (ISO 8601 UTC
    timestamp stamped by ``save_snapshot``).
    """

    created: str
