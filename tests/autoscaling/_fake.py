from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from httpx2_k8s import (
    CrossVersionObjectReference,
)
from tests._helpers import discard_selected, matches_key_prefix, select_by_label


class FakeAutoscalingAPI:
    """Stateful Autoscaling v1/v2 boundary for HPAs."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str, str], dict[str, object]] = {}
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.put_calls: list[tuple[str, str | None, dict[str, str]]] = []
        self.delete_collection_calls: list[tuple[str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        version = parts[2]
        all_namespaces = parts[3] != "namespaces"
        namespace = "" if all_namespaces else parts[4]
        name = None if all_namespaces or len(parts) < 7 else parts[6]
        subresource = parts[7] if len(parts) == 8 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(
                namespace=namespace,
                resourceVersion="1",
                uid=f"{version}-hpa-uid",
                generation=1,
            )
            body["status"] = self._status(version)
            self.resources[(version, namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            items = [
                body
                for (stored_version, stored_namespace, _), body in self.resources.items()
                if stored_version == version and (all_namespaces or stored_namespace == namespace)
            ]
            return self._response(
                200,
                {
                    "apiVersion": f"autoscaling/{version}",
                    "kind": "HorizontalPodAutoscalerList",
                    "metadata": {
                        "continue": "",
                        "remainingItemCount": 0,
                        "resourceVersion": "2",
                        "selfLink": request.url.path,
                    },
                    "items": items,
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            params = dict(request.url.params)
            self.delete_collection_calls.append((version, params, body))
            selector = params.get("labelSelector")
            selected = select_by_label(
                self.resources,
                in_scope=matches_key_prefix(version, namespace),
                selector=selector,
            )
            if "All" not in cast(list[str], body.get("dryRun", [])):
                discard_selected(self.resources, selected)
            return self._response(
                200,
                {
                    "apiVersion": f"autoscaling/{version}",
                    "kind": "HorizontalPodAutoscalerList",
                    "metadata": {},
                    "items": [current for _, current in selected],
                },
            )

        assert name is not None
        key = (version, namespace, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            current = self.resources[key]
            if subresource == "status":
                current["status"] = body["status"]
            else:
                self.resources[key] = body
                current = body
            metadata = cast(dict[str, object], current["metadata"])
            metadata["resourceVersion"] = str(int(cast(str, metadata["resourceVersion"])) + 1)
            self.put_calls.append((version, subresource, dict(request.url.params)))
            return self._response(200, current)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((version, content_type, dict(request.url.params)))
            current = self.resources[key]
            metadata = cast(dict[str, object], current["metadata"])
            if content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                if subresource == "status":
                    status = cast(dict[str, object], current["status"])
                    status.update(cast(dict[str, object], patch["status"]))
                    metadata["resourceVersion"] = "5"
                else:
                    patch_metadata = cast(dict[str, object], patch["metadata"])
                    metadata["annotations"] = patch_metadata["annotations"]
                    metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        self.resources.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )

    @staticmethod
    def _status(version: str) -> dict[str, object]:
        if version == "v1":
            return {
                "currentCPUUtilizationPercentage": 40,
                "currentReplicas": 2,
                "desiredReplicas": 3,
                "lastScaleTime": "2026-08-14T12:00:00Z",
                "observedGeneration": 1,
            }
        return {
            "conditions": [
                {
                    "type": "AbleToScale",
                    "status": "True",
                    "lastTransitionTime": "2026-08-14T12:00:00Z",
                    "message": "ready",
                    "reason": "SucceededGetScale",
                }
            ],
            "currentMetrics": [
                {
                    "type": "ContainerResource",
                    "containerResource": {
                        "container": "web",
                        "name": "cpu",
                        "current": {"averageUtilization": 40, "averageValue": "100m"},
                    },
                },
                {
                    "type": "External",
                    "external": {
                        "metric": {"name": "queue", "selector": {"matchLabels": {"q": "a"}}},
                        "current": {"value": "10"},
                    },
                },
                {
                    "type": "Object",
                    "object": {
                        "describedObject": {
                            "apiVersion": "v1",
                            "kind": "Service",
                            "name": "web",
                        },
                        "metric": {"name": "requests"},
                        "current": {"value": "20"},
                    },
                },
                {
                    "type": "Pods",
                    "pods": {
                        "metric": {"name": "packets"},
                        "current": {"averageValue": "5"},
                    },
                },
                {
                    "type": "Resource",
                    "resource": {
                        "name": "memory",
                        "current": {"averageUtilization": 50, "averageValue": "128Mi"},
                    },
                },
            ],
            "currentReplicas": 2,
            "desiredReplicas": 3,
            "lastScaleTime": "2026-08-14T12:00:00Z",
            "observedGeneration": 1,
        }


def target() -> CrossVersionObjectReference:
    return CrossVersionObjectReference(api_version="apps/v1", kind="Deployment", name="web")
