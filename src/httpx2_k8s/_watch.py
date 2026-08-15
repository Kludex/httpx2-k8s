from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar, cast

from pydantic import BaseModel, JsonValue, ValidationError

from httpx2_k8s._models import Status

ResourceT = TypeVar("ResourceT", bound=BaseModel)


@dataclass(frozen=True, slots=True)
class WatchEvent(Generic[ResourceT]):
    """A typed resource change delivered by a Kubernetes watch."""

    type: Literal["ADDED", "MODIFIED", "DELETED"]
    object: ResourceT
    resource_version: str | None


@dataclass(frozen=True, slots=True)
class WatchBookmark:
    """A watch checkpoint containing the latest observed resource version."""

    resource_version: str


class WatchError(RuntimeError):
    """A Kubernetes Status delivered as an ERROR watch event."""

    def __init__(self, status: Status) -> None:
        self.status = status
        detail = status.message or status.reason or "Kubernetes watch failed"
        super().__init__(detail)


class WatchProtocolError(ValueError):
    """A malformed or unsupported event was received from a watch stream."""


def _resource_version(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    metadata = value.get("metadata")
    if not isinstance(metadata, dict):
        return None
    resource_version = metadata.get("resourceVersion")
    return resource_version if isinstance(resource_version, str) else None


def decode_watch_line(
    line: str, response_model: type[ResourceT]
) -> WatchEvent[ResourceT] | WatchBookmark:
    """Decode one Kubernetes watch line into a typed event."""
    try:
        raw = json.loads(line)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise WatchProtocolError("Kubernetes watch returned invalid JSON") from exc
    if not isinstance(raw, dict) or not isinstance(raw.get("type"), str) or "object" not in raw:
        raise WatchProtocolError("Kubernetes watch event has an invalid envelope")
    event_type = raw["type"]
    raw_object: JsonValue = cast(JsonValue, raw["object"])
    if event_type == "ERROR":
        try:
            status = Status.model_validate(raw_object)
        except ValidationError as exc:
            raise WatchProtocolError("Kubernetes watch ERROR event has an invalid Status") from exc
        raise WatchError(status)
    resource_version = _resource_version(raw_object)
    if event_type == "BOOKMARK":
        if resource_version is None:
            raise WatchProtocolError("Kubernetes watch bookmark has no resourceVersion")
        return WatchBookmark(resource_version)
    if event_type not in {"ADDED", "MODIFIED", "DELETED"}:
        raise WatchProtocolError(f"Kubernetes watch returned unknown event type {event_type!r}")
    try:
        resource = response_model.model_validate(raw_object)
    except ValidationError as exc:
        raise WatchProtocolError("Kubernetes watch event object failed validation") from exc
    return WatchEvent(
        cast(Literal["ADDED", "MODIFIED", "DELETED"], event_type), resource, resource_version
    )
