from __future__ import annotations

from dataclasses import dataclass
import re


def _workspace_token(workspace_id: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9._-]+", "-", workspace_id.strip()).strip("-._")
    return token or "default"


@dataclass
class WorkspaceContext:
    workspace_id: str
    display_name: str
    schema_source: str
    log_tag: str = ""
    log_root: str = ""

    def __post_init__(self) -> None:
        token = self.workspace_token
        if not self.schema_source:
            self.schema_source = f"workspace:{token}:schema"
        if not self.log_tag:
            self.log_tag = f"workspace:{token}"
        if not self.log_root:
            self.log_root = f".sisyphus/logs/workspaces/{token}/"

    @property
    def workspace_token(self) -> str:
        return _workspace_token(self.workspace_id)

    def log_path(self, filename: str = "runtime.log") -> str:
        clean_name = filename.strip().strip("/\\") or "runtime.log"
        return f"{self.log_root}{clean_name}"

    def log_metadata(self, filename: str = "runtime.log") -> dict[str, str]:
        return {
            "workspace_id": self.workspace_id,
            "workspace_token": self.workspace_token,
            "log_tag": self.log_tag,
            "log_path": self.log_path(filename),
        }

    @classmethod
    def default(cls, workspace_id: str) -> "WorkspaceContext":
        token = _workspace_token(workspace_id)
        return cls(
            workspace_id=workspace_id,
            display_name=workspace_id.replace("-", " ").replace("_", " ").title() or "Default",
            schema_source=f"workspace:{token}:schema",
            log_tag=f"workspace:{token}",
            log_root=f".sisyphus/logs/workspaces/{token}/",
        )


class WorkspaceRegistry:
    def __init__(self) -> None:
        self._registry: dict[str, WorkspaceContext] = {}

    def register(self, workspace_id: str, context: WorkspaceContext) -> None:
        self._registry[workspace_id] = context

    def get(self, workspace_id: str) -> WorkspaceContext:
        return self._registry.get(workspace_id) or self.default_context(workspace_id)

    def default_context(self, workspace_id: str) -> WorkspaceContext:
        return WorkspaceContext.default(workspace_id)


_registry_instance: WorkspaceRegistry | None = None


def get_workspace_registry() -> WorkspaceRegistry:
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = WorkspaceRegistry()
    return _registry_instance


def get_workspace_context(workspace_id: str | None) -> WorkspaceContext:
    return get_workspace_registry().get(workspace_id or "default")


def get_workspace_temp_dir(workspace_id: str | None) -> str:
    """Return the workspace-scoped temp root for isolated scratch files."""
    ctx = get_workspace_context(workspace_id or "default")
    return f".sisyphus/tmp/duckdb/{ctx.workspace_token}/"


def get_workspace_log_dir(workspace_id: str | None) -> str:
    """Return the workspace-scoped log root path (configured value).

    Similar to get_workspace_temp_dir, this returns the configured log_root
    (typically ".sisyphus/logs/workspaces/<token>/").
    """
    ctx = get_workspace_context(workspace_id or "default")
    return ctx.log_root


__all__ = [
    "WorkspaceContext",
    "WorkspaceRegistry",
    "get_workspace_context",
    "get_workspace_registry",
]
