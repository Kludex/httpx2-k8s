from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2


class FakeSchedulingAPI:
    """Stateful Scheduling v1 PriorityClass boundary."""

    def __init__(self) -> None:
        self.resources: dict[str, dict[str, object]] = {}
        self.patch_calls: list[tuple[str, dict[str, str]]] = []
        self.put_calls: list[dict[str, str]] = []
        self.delete_collection_call: tuple[dict[str, str], dict[str, object]] | None = None

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        name = parts[4] if len(parts) == 5 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(resourceVersion="1", uid="priorityclass-uid")
            self.resources[object_name] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            return self._response(
                200,
                {
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClassList",
                    "metadata": {
                        "continue": "",
                        "remainingItemCount": 0,
                        "resourceVersion": "2",
                        "selfLink": request.url.path,
                    },
                    "items": list(self.resources.values()),
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            params = dict(request.url.params)
            self.delete_collection_call = (params, body)
            return self._response(
                200,
                {
                    "apiVersion": "scheduling.k8s.io/v1",
                    "kind": "PriorityClassList",
                    "metadata": {},
                    "items": list(self.resources.values()),
                },
            )

        assert name is not None
        if request.method == "GET":
            return self._response(200, self.resources[name])
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            metadata["resourceVersion"] = "4"
            self.resources[name] = body
            self.put_calls.append(dict(request.url.params))
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((content_type, dict(request.url.params)))
            current = self.resources[name]
            metadata = cast(dict[str, object], current["metadata"])
            if content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                patch_metadata = cast(dict[str, object], patch["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
                metadata["resourceVersion"] = "3"
            else:
                assert content_type == "application/apply-patch+yaml"
                metadata["resourceVersion"] = "2"
            return self._response(200, current)
        self.resources.pop(name)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )
