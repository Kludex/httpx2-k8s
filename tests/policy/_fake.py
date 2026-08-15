from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from tests._helpers import discard_selected, matches_key_prefix, select_by_label


class FakePolicyAPI:
    """Stateful Policy v1 boundary for budgets and Pod eviction."""

    def __init__(self) -> None:
        self.budgets: dict[tuple[str, str], dict[str, object]] = {}
        self.last_eviction: dict[str, object] | None = None
        self.patch_calls: list[tuple[str, dict[str, str]]] = []
        self.put_calls: list[tuple[str | None, dict[str, str]]] = []
        self.delete_collection_calls: list[tuple[dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[-1] == "eviction":
            self.last_eviction = cast(dict[str, object], json.loads(request.content))
            return self._response(
                201,
                {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 201},
            )

        if parts[3] == "namespaces":
            namespace = parts[4]
            name = parts[6] if len(parts) >= 7 else None
            subresource = parts[7] if len(parts) >= 8 else None
            all_namespaces = False
        else:
            namespace = ""
            name = None
            subresource = None
            all_namespaces = True
        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(
                namespace=namespace,
                resourceVersion="1",
                uid="pdb-uid",
                generation=1,
            )
            body["status"] = {
                "conditions": [
                    {
                        "type": "DisruptionAllowed",
                        "status": "False",
                        "lastTransitionTime": "2026-08-14T12:00:00Z",
                        "message": "budget requires one healthy pod",
                        "observedGeneration": 1,
                        "reason": "InsufficientPods",
                    }
                ],
                "currentHealthy": 0,
                "desiredHealthy": 1,
                "disruptedPods": {"old-pod": "2026-08-14T11:59:00Z"},
                "disruptionsAllowed": 0,
                "expectedPods": 1,
                "observedGeneration": 1,
            }
            self.budgets[(namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            items = [
                body
                for (stored_namespace, _), body in self.budgets.items()
                if all_namespaces or stored_namespace == namespace
            ]
            return self._response(
                200,
                {
                    "apiVersion": "policy/v1",
                    "kind": "PodDisruptionBudgetList",
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
            self.delete_collection_calls.append((params, body))
            selected = select_by_label(
                self.budgets,
                in_scope=matches_key_prefix(namespace),
                selector=params.get("labelSelector"),
            )
            if "All" not in cast(list[str], body.get("dryRun", [])):
                discard_selected(self.budgets, selected)
            return self._response(
                200,
                {
                    "apiVersion": "policy/v1",
                    "kind": "PodDisruptionBudgetList",
                    "metadata": {},
                    "items": [current for _, current in selected],
                },
            )

        assert name is not None
        key = (namespace, name)
        if request.method == "GET":
            return self._response(200, self.budgets[key])
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            metadata["resourceVersion"] = "4"
            self.budgets[key] = body
            self.put_calls.append((subresource, dict(request.url.params)))
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((content_type, dict(request.url.params)))
            current = self.budgets[key]
            metadata = cast(dict[str, object], current["metadata"])
            if content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                if subresource == "status":
                    current["status"] = patch["status"]
                else:
                    patch_metadata = cast(dict[str, object], patch["metadata"])
                    metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        self.budgets.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )
