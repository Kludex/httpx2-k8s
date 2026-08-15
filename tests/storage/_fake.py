from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2

from tests._helpers import discard_selected, matches_key_prefix, select_by_label


class FakeStorageV1API:
    """Stateful boundary for all stable Storage v1 resources."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str, str], dict[str, object]] = {}
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.put_calls: list[tuple[str, str | None, dict[str, str]]] = []
        self.delete_collection_calls: list[tuple[str, str, dict[str, str], dict[str, object]]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        if parts[3] == "namespaces":
            namespace = parts[4]
            resource = parts[5]
            name = parts[6] if len(parts) >= 7 else None
            all_namespaces = False
            subresource = parts[7] if len(parts) == 8 else None
        else:
            namespace = ""
            resource = parts[3]
            name = parts[4] if len(parts) >= 5 else None
            all_namespaces = resource == "csistoragecapacities"
            subresource = parts[5] if len(parts) == 6 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(resourceVersion="1", uid=f"{resource}-uid")
            if namespace:
                metadata["namespace"] = namespace
            if resource == "volumeattachments":
                body["status"] = {
                    "attached": False,
                    "attachError": {
                        "message": "driver is offline",
                        "time": "2026-08-14T12:00:00Z",
                    },
                    "attachmentMetadata": {"device": "/dev/test"},
                    "detachError": {
                        "message": "not attached",
                        "time": "2026-08-14T12:01:00Z",
                    },
                }
            self.resources[(resource, namespace, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            singular = {
                "csidrivers": "CSIDriver",
                "csinodes": "CSINode",
                "csistoragecapacities": "CSIStorageCapacity",
                "storageclasses": "StorageClass",
                "volumeattachments": "VolumeAttachment",
                "volumeattributesclasses": "VolumeAttributesClass",
            }[resource]
            items = [
                body
                for (stored_resource, stored_namespace, _), body in self.resources.items()
                if stored_resource == resource and (all_namespaces or stored_namespace == namespace)
            ]
            return self._response(
                200,
                {
                    "apiVersion": "storage.k8s.io/v1",
                    "kind": f"{singular}List",
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
            self.delete_collection_calls.append((resource, namespace, params, body))
            selector = params.get("labelSelector")
            selected = select_by_label(
                self.resources,
                in_scope=matches_key_prefix(resource, namespace),
                selector=selector,
            )
            if "All" not in cast(list[str], body.get("dryRun", [])):
                discard_selected(self.resources, selected)
            singular = {
                "csidrivers": "CSIDriver",
                "csinodes": "CSINode",
                "csistoragecapacities": "CSIStorageCapacity",
                "storageclasses": "StorageClass",
                "volumeattachments": "VolumeAttachment",
                "volumeattributesclasses": "VolumeAttributesClass",
            }[resource]
            return self._response(
                200,
                {
                    "apiVersion": "storage.k8s.io/v1",
                    "kind": f"{singular}List",
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
            current = self.resources[key]
            if subresource == "status":
                current["status"] = body["status"]
            else:
                self.resources[key] = body
                current = body
            metadata = cast(dict[str, object], current["metadata"])
            metadata["resourceVersion"] = str(int(cast(str, metadata["resourceVersion"])) + 1)
            self.put_calls.append((resource, subresource, dict(request.url.params)))
            return self._response(200, current)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((resource, content_type, dict(request.url.params)))
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
        if resource in {
            "csidrivers",
            "csinodes",
            "storageclasses",
            "volumeattachments",
            "volumeattributesclasses",
        }:
            return self._response(200, self.resources.pop(key))
        self.resources.pop(key)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )
