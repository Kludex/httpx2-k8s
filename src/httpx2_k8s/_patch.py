from __future__ import annotations

from typing import Literal, TypeAlias

from httpx2_k8s._models import JsonPatch, MergePatch

DryRun: TypeAlias = Literal["All"]


def patch_params(
    *,
    field_manager: str | None,
    force: bool | None = None,
    dry_run: DryRun | None = None,
) -> dict[str, str | int]:
    """Build common Kubernetes PATCH query parameters."""
    params: dict[str, str | int] = {}
    if field_manager is not None:
        params["fieldManager"] = field_manager
    if force is not None:
        params["force"] = str(force).lower()
    if dry_run is not None:
        params["dryRun"] = dry_run
    return params


def patch_content_type(body: JsonPatch | MergePatch) -> str:
    """Return the Kubernetes media type for a structured patch body."""
    if isinstance(body, JsonPatch):
        return "application/json-patch+json"
    return "application/merge-patch+json"
