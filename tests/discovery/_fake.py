from collections.abc import Mapping

import httpx2


class FakeDiscoveryAPI:
    """High-level Kubernetes discovery and OpenAPI v3 boundary."""

    @staticmethod
    def _response(body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(200, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        path = request.url.path
        if path == "/api":
            return self._response(
                {
                    "apiVersion": "v1",
                    "kind": "APIVersions",
                    "versions": ["v1"],
                    "serverAddressByClientCIDRs": [
                        {
                            "clientCIDR": "0.0.0.0/0",
                            "serverAddress": "kubernetes.example.test:443",
                        }
                    ],
                }
            )
        if path == "/apis":
            return self._response(
                {
                    "apiVersion": "v1",
                    "kind": "APIGroupList",
                    "groups": [self._group()],
                }
            )
        if path == "/apis/apps%20group" or path == "/apis/apps group":
            return self._response(self._group())
        if path == "/api/v1":
            return self._response(self._resources("v1", "Namespace", False))
        if path in {"/apis/apps%20group/v1", "/apis/apps group/v1"}:
            return self._response(self._resources("apps group/v1", "Deployment", True))
        if path == "/openapi/v3":
            return self._response(
                {
                    "paths": {
                        "api/v1": {"serverRelativeURL": "/openapi/v3/api/v1?hash=core"},
                        "apis/apps group/v1": {
                            "serverRelativeURL": "/openapi/v3/apis/apps%20group/v1?hash=apps"
                        },
                    }
                }
            )
        if path == "/openapi/v3/api/v1":
            return self._response(self._document("core-v1"))
        if path in {
            "/openapi/v3/apis/apps%20group/v1",
            "/openapi/v3/apis/apps group/v1",
        }:
            return self._response(self._document("apps-v1"))
        raise AssertionError(f"unexpected discovery path: {path}")

    @staticmethod
    def _group() -> dict[str, object]:
        return {
            "apiVersion": "v1",
            "kind": "APIGroup",
            "name": "apps group",
            "versions": [{"groupVersion": "apps group/v1", "version": "v1"}],
            "preferredVersion": {"groupVersion": "apps group/v1", "version": "v1"},
            "serverAddressByClientCIDRs": [
                {
                    "clientCIDR": "10.0.0.0/8",
                    "serverAddress": "internal.example.test:443",
                }
            ],
        }

    @staticmethod
    def _resources(group_version: str, kind: str, namespaced: bool) -> dict[str, object]:
        resource_name = kind.lower() + "s"
        return {
            "apiVersion": "v1",
            "kind": "APIResourceList",
            "groupVersion": group_version,
            "resources": [
                {
                    "categories": ["all"],
                    "group": "apps group" if namespaced else "",
                    "kind": kind,
                    "name": resource_name,
                    "namespaced": namespaced,
                    "shortNames": [resource_name[:3]],
                    "singularName": kind.lower(),
                    "storageVersionHash": "hash-1",
                    "verbs": ["create", "get", "list"],
                    "version": "v1",
                }
            ],
        }

    @staticmethod
    def _document(version: str) -> dict[str, object]:
        return {
            "openapi": "3.0.0",
            "info": {
                "title": "Kubernetes",
                "version": version,
                "description": "test schema",
            },
            "components": {
                "schemas": {
                    "io.k8s.test": {"type": "object", "properties": {"name": {"type": "string"}}}
                }
            },
            "paths": {"/version": {"get": {"responses": {"200": {"description": "ok"}}}}},
        }
