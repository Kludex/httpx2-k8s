from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from tests._helpers import discard_selected, matches_key_prefix, select_by_label


class FakeNetworkingAPI:
    """Stateful Core/Discovery API boundary for networking resources."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str, str], dict[str, object]] = {}
        self.queries: list[httpx2.QueryParams] = []
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.put_calls: list[tuple[str, dict[str, str]]] = []
        self.delete_collection_calls: list[tuple[str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[0] == "api":
            namespace, resource = parts[3:5]
            name = parts[5] if len(parts) == 6 else None
            all_namespaces = False
        elif parts[3] == "namespaces":
            namespace, resource = parts[4:6]
            name = parts[6] if len(parts) == 7 else None
            all_namespaces = False
        else:
            namespace = ""
            resource = parts[3]
            name = None
            all_namespaces = True

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(namespace=namespace, resourceVersion="1")
            if resource == "services":
                spec = cast(dict[str, object], body["spec"])
                spec.update(clusterIP="10.43.0.10", clusterIPs=["10.43.0.10"])
            self.resources[(resource, namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            self.queries.append(request.url.params)
            kind = {
                "services": "ServiceList",
                "endpoints": "EndpointsList",
                "endpointslices": "EndpointSliceList",
            }[resource]
            api_version = "discovery.k8s.io/v1" if resource == "endpointslices" else "v1"
            items = [
                body
                for (stored_resource, stored_namespace, _), body in self.resources.items()
                if stored_resource == resource and (all_namespaces or stored_namespace == namespace)
            ]
            return self._response(
                200,
                {
                    "apiVersion": api_version,
                    "kind": kind,
                    "metadata": {"resourceVersion": "2"},
                    "items": items,
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            params = dict(request.url.params)
            self.delete_collection_calls.append((resource, params, body))
            selector = params.get("labelSelector")
            selected = select_by_label(
                self.resources,
                in_scope=matches_key_prefix(resource, namespace),
                selector=selector,
            )
            if "All" not in cast(list[str], body.get("dryRun", [])):
                discard_selected(self.resources, selected)
            kind = {
                "services": "ServiceList",
                "endpoints": "EndpointsList",
                "endpointslices": "EndpointSliceList",
            }[resource]
            api_version = "discovery.k8s.io/v1" if resource == "endpointslices" else "v1"
            return self._response(
                200,
                {
                    "apiVersion": api_version,
                    "kind": kind,
                    "metadata": {},
                    "items": [current for _, current in selected],
                },
            )

        assert name is not None
        key = (resource, namespace, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            metadata["resourceVersion"] = "4"
            self.resources[key] = body
            self.put_calls.append((resource, dict(request.url.params)))
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((resource, content_type, dict(request.url.params)))
            current = self.resources[key]
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
        deleted = self.resources.pop(key)
        if resource == "services":
            deleted["status"] = {"loadBalancer": dict[str, object]()}
            return self._response(200, deleted)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )
