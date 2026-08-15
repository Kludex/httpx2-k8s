from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Literal, cast

import httpx2
from pydantic import Field

from httpx2_k8s import CustomResource, KubeModel
from tests._helpers import discard_selected, matches_key_prefix, select_by_label


class WidgetSpec(KubeModel):
    size: int
    features: list[str] = Field(default_factory=list[str])


class Widget(CustomResource[Literal["testing.httpx2-k8s.dev/v1"], Literal["Widget"]]):
    api_version: Literal["testing.httpx2-k8s.dev/v1"] = "testing.httpx2-k8s.dev/v1"
    kind: Literal["Widget"] = "Widget"
    spec: WidgetSpec


class FakeCustomObjectsAPI:
    """Stateful HTTP boundary for cluster and namespaced arbitrary resources."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str], dict[str, object]] = {}
        self.delete_collection_calls: list[tuple[str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[3] == "namespaces":
            namespace = parts[4]
            plural = parts[5]
            name = parts[6] if len(parts) >= 7 else None
        else:
            namespace = ""
            plural = parts[3]
            name = parts[4] if len(parts) >= 5 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(resourceVersion="1", uid=f"{plural}-uid")
            if namespace:
                metadata["namespace"] = namespace
            self.resources[(namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            items = [
                body
                for (stored_namespace, _), body in self.resources.items()
                if stored_namespace == namespace
            ]
            item_kind = cast(str, items[0]["kind"])
            return self._response(
                200,
                {
                    "apiVersion": "testing.httpx2-k8s.dev/v1",
                    "kind": f"{item_kind}List",
                    "metadata": {"continue": "", "remainingItemCount": 0},
                    "items": items,
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            params = dict(request.url.params)
            self.delete_collection_calls.append((namespace, params, body))
            selector = params.get("labelSelector")
            selected = select_by_label(
                self.resources,
                in_scope=matches_key_prefix(namespace),
                selector=selector,
            )
            if "All" in cast(list[str], body.get("dryRun", [])):
                item_kind = cast(str, selected[0][1]["kind"])
                return self._response(
                    200,
                    {
                        "apiVersion": "testing.httpx2-k8s.dev/v1",
                        "kind": f"{item_kind}List",
                        "metadata": {},
                        "items": [current for _, current in selected],
                    },
                )
            discard_selected(self.resources, selected)
            return self._response(
                200,
                {
                    "apiVersion": "testing.httpx2-k8s.dev/v1",
                    "kind": f"{cast(str, selected[0][1]['kind'])}List",
                    "metadata": {},
                    "items": [current for _, current in selected],
                },
            )

        assert name is not None
        key = (namespace, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            metadata["resourceVersion"] = "2"
            if namespace:
                metadata["namespace"] = namespace
            self.resources[key] = body
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            patch = json.loads(request.content)
            current = self.resources[key]
            if content_type == "application/json-patch+json":
                assert not request.url.params
                test_operation, replace_operation = cast(list[dict[str, object]], patch)
                spec = cast(dict[str, object], current["spec"])
                assert test_operation == {
                    "op": "test",
                    "path": "/spec/replicas",
                    "value": spec["replicas"],
                }
                assert replace_operation["op"] == "replace"
                assert replace_operation["path"] == "/spec/replicas"
                spec["replicas"] = replace_operation["value"]
            elif content_type == "application/merge-patch+json":
                assert dict(request.url.params) == {
                    "dryRun": "All",
                    "fieldManager": "merge-manager",
                }
                patch_object = cast(dict[str, object], patch)
                current_spec = cast(dict[str, object], current["spec"])
                current_spec.update(cast(dict[str, object], patch_object["spec"]))
            else:
                assert content_type == "application/apply-patch+yaml"
                expected_params = (
                    {"fieldManager": "apply-manager", "force": "true"}
                    if namespace
                    else {"fieldManager": "cluster-manager"}
                )
                assert dict(request.url.params) == expected_params
                applied = cast(dict[str, object], patch)
                current["spec"] = applied["spec"]
            metadata = cast(dict[str, object], current["metadata"])
            metadata["resourceVersion"] = "3"
            return self._response(200, current)

        assert request.method == "DELETE"
        self.resources.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )
