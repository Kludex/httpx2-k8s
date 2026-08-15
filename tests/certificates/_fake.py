from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from typing import cast

import httpx2


class FakeCertificatesAPI:
    """Stateful HTTP boundary for a CertificateSigningRequest lifecycle."""

    def __init__(self) -> None:
        self.resource: dict[str, object] | None = None
        self.patch_calls: list[tuple[str | None, str, dict[str, str]]] = []
        self.delete_collection_call: tuple[dict[str, str], dict[str, object]] | None = None

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        name = parts[4] if len(parts) >= 5 else None
        subresource = parts[5] if len(parts) == 6 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            spec = cast(dict[str, object], body["spec"])
            assert spec["request"] == base64.b64encode(b"pem certificate request").decode()
            spec.update(
                extra={"authentication.kubernetes.io/pod-name": ["issuer"]},
                groups=["system:authenticated"],
                uid="requester-uid",
                username="alice",
            )
            metadata = cast(dict[str, object], body["metadata"])
            metadata.update(resourceVersion="1", uid="csr-uid")
            self.resource = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            assert dict(request.url.params) == {
                "labelSelector": "owner=tests",
                "fieldSelector": "metadata.name=client request",
                "limit": "1",
                "continue": "next",
            }
            return self._response(
                200,
                {
                    "apiVersion": "certificates.k8s.io/v1",
                    "kind": "CertificateSigningRequestList",
                    "metadata": {"continue": "", "remainingItemCount": 0},
                    "items": [self.resource],
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            params = dict(request.url.params)
            self.delete_collection_call = (params, body)
            assert self.resource is not None
            metadata = cast(dict[str, object], self.resource["metadata"])
            labels = cast(dict[str, str], metadata["labels"])
            assert labels["owner"] == "tests"
            assert params["labelSelector"] == "owner=tests"
            assert "All" in cast(list[str], body["dryRun"])
            return self._response(
                200,
                {
                    "apiVersion": "certificates.k8s.io/v1",
                    "kind": "CertificateSigningRequestList",
                    "metadata": {},
                    "items": [self.resource],
                },
            )

        assert self.resource is not None
        assert name == "client request"
        if request.method == "GET":
            return self._response(200, self.resource)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((subresource, content_type, dict(request.url.params)))
            metadata = cast(dict[str, object], self.resource["metadata"])
            if content_type == "application/merge-patch+json":
                patch = cast(dict[str, object], json.loads(request.content))
                if subresource is None:
                    patch_metadata = cast(dict[str, object], patch["metadata"])
                    metadata["annotations"] = patch_metadata["annotations"]
                else:
                    status = cast(dict[str, object], self.resource.setdefault("status", {}))
                    status.update(cast(dict[str, object], patch["status"]))
            else:
                assert content_type == "application/apply-patch+yaml"
            metadata["resourceVersion"] = str(int(cast(str, metadata["resourceVersion"])) + 1)
            return self._response(200, self.resource)
        if request.method == "PUT":
            body = cast(dict[str, object], json.loads(request.content))
            if subresource == "approval" or subresource == "status":
                self.resource["status"] = body["status"]
            else:
                self.resource = body
            metadata = cast(dict[str, object], self.resource["metadata"])
            metadata["resourceVersion"] = str(int(cast(str, metadata["resourceVersion"])) + 1)
            return self._response(200, self.resource)

        assert request.method == "DELETE"
        self.resource = None
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )
