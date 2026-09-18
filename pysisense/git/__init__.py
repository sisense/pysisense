from ..sisenseclient import SisenseClient
from .branches import GitBranchesMixin
from .commits import GitCommitsMixin
from .core import GitCoreMixin
from .remote import GitRemoteMixin


class Git(GitCoreMixin, GitBranchesMixin, GitCommitsMixin, GitRemoteMixin):
    """Sisense Git Integration: projects, branches, commits, and remote sync.

    Git Integration is an **optional** Sisense feature. An admin must enable
    it (Admin > App Configuration > Feature Management) before any endpoint
    in this class returns project data — until then, calls fail with a
    normal Sisense error response. There is no API endpoint to check
    whether the feature is enabled; the SDK does not attempt to detect it.

    Every method that takes a ``project`` parameter accepts either the
    Git project's 24-character OID or its name, resolved internally via
    ``resolve_git_project_reference``. Linux-only: there is no Windows
    equivalent of this API surface.

    Modules
    -------
    core :
        Project listing and lookup, reference resolution, status checksum
        retrieval, manual unlock, asset/file sync, and discarding
        uncommitted changes.
    branches :
        List, retrieve, create, and check out Git branches of a project.
    commits :
        List, retrieve, create, and check out Git commits of a project.
    remote :
        Remote repository operations — fetch, pull, and push over HTTP(S).
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """Initialize the Git class for managing Sisense Git Integration projects.

        If no Sisense client is provided, a new SisenseClient is created
        using the default ``config.yaml``.

        Parameters
        ----------
        api_client : SisenseClient, optional
            An existing SisenseClient instance. If ``None``, a new client is
            created.
        debug : bool, optional
            Enables debug-level logging when ``True``. Default is ``False``.
        """
        self.api_client = api_client if api_client else SisenseClient(debug=debug)
        self.logger = self.api_client.logger
        self.logger.debug("Git class initialized.")
