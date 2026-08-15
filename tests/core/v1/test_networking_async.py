from __future__ import annotations

import json
from collections.abc import Mapping
from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    EndpointAddress,
    EndpointPort,
    Endpoints,
    EndpointSubset,
    LoadBalancerIngress,
    LoadBalancerStatus,
    MergePatch,
    ObjectMeta,
    Service,
    ServicePort,
    ServiceSpec,
    ServiceStatus,
)


class FakeServiceAPI:
    """Stateful Service and legacy Endpoints API reached through HTTPX2."""

    def __init__(self) -> None:
        self.resources: dict[tuple[str, str], dict[str, object]] = {}
        self.list_queries: list[httpx2.QueryParams] = []
        self.patch_calls: list[tuple[str, str, dict[str, str]]] = []
        self.collection_delete_bodies: list[dict[str, object]] = []

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        parts = request.url.path.strip("/").split("/")
        namespace = parts[3]
        resource = parts[4]
        name = parts[5] if len(parts) >= 6 else None
        subresource = parts[6] if len(parts) == 7 else None

        if request.method == "POST":
            body = cast(dict[str, object], json.loads(request.content))
            metadata = cast(dict[str, object], body["metadata"])
            object_name = cast(str, metadata["name"])
            metadata.update(namespace=namespace, resourceVersion="1")
            self.resources[(resource, object_name)] = body
            return self._response(201, body)

        if request.method == "GET" and name is None:
            self.list_queries.append(request.url.params)
            singular = "Service" if resource == "services" else "Endpoints"
            items = [
                body
                for (stored_resource, _), body in self.resources.items()
                if stored_resource == resource
            ]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": f"{singular}List",
                    "metadata": {"resourceVersion": "2"},
                    "items": items,
                },
            )

        if request.method == "DELETE" and name is None:
            body = cast(dict[str, object], json.loads(request.content))
            self.collection_delete_bodies.append(body)
            singular = "Service" if resource == "services" else "Endpoints"
            items = [
                stored
                for (stored_resource, _), stored in self.resources.items()
                if stored_resource == resource
            ]
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": f"{singular}List",
                    "metadata": {},
                    "items": items,
                },
            )

        assert name is not None
        key = (resource, name)
        if request.method == "GET":
            return self._response(200, self.resources[key])
        if request.method == "PUT":
            assert subresource == "status"
            body = cast(dict[str, object], json.loads(request.content))
            self.resources[key] = body
            return self._response(200, body)
        if request.method == "PATCH":
            content_type = request.headers["content-type"]
            self.patch_calls.append((request.url.path, content_type, dict(request.url.params)))
            patch = cast(dict[str, object], json.loads(request.content))
            if content_type == "application/apply-patch+yaml":
                metadata = cast(dict[str, object], patch["metadata"])
                metadata.update(namespace=namespace, resourceVersion="2")
                self.resources[key] = patch
            elif subresource == "status":
                self.resources[key]["status"] = patch["status"]
            else:
                metadata = cast(dict[str, object], self.resources[key]["metadata"])
                patch_metadata = cast(dict[str, object], patch["metadata"])
                metadata["annotations"] = patch_metadata["annotations"]
            return self._response(200, self.resources[key])

        deleted = self.resources.pop(key)
        if resource == "services":
            return self._response(200, deleted)
        return self._response(
            200,
            {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200},
        )


@pytest.mark.anyio
async def test_async_service_and_endpoints_lifecycles_through_httpx2() -> None:
    api_server = FakeServiceAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        service = await client.core_v1.create_namespaced_service(
            "team one",
            Service(
                metadata=ObjectMeta(name="web service", labels={"owner": "async-tests"}),
                spec=ServiceSpec(
                    selector={"app": "web"},
                    ports=[ServicePort(name="http", port=80, target_port=8080)],
                ),
            ),
        )
        assert service.metadata.namespace == "team one"
        assert (await client.core_v1.read_namespaced_service("web service", "team one")) == service
        service = await client.core_v1.apply_namespaced_service(
            "web service",
            "team one",
            service,
            field_manager="async-services",
            force=True,
            dry_run="All",
        )
        service = await client.core_v1.patch_namespaced_service(
            "web service",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "service"}}}),
            field_manager="async-services",
        )
        assert service.metadata.annotations == {"patched": "service"}
        service_with_status = service.model_copy(
            update={
                "status": ServiceStatus(
                    load_balancer=LoadBalancerStatus(ingress=[LoadBalancerIngress(ip="192.0.2.10")])
                )
            }
        )
        service = await client.core_v1.replace_namespaced_service_status(
            "web service", "team one", service_with_status
        )
        service = await client.core_v1.patch_namespaced_service_status(
            "web service",
            "team one",
            MergePatch(document={"status": {"loadBalancer": {"ingress": [{"ip": "192.0.2.11"}]}}}),
            field_manager="async-status",
            dry_run="All",
        )
        assert service.status is not None
        assert service.status.load_balancer.ingress[0].ip == "192.0.2.11"
        assert (
            await client.core_v1.list_namespaced_service(
                "team one",
                label_selector="owner=async-tests",
                field_selector="metadata.name=web service",
                limit=1,
                continue_token="service-next",
            )
        ).items == [service]
        assert (
            await client.core_v1.delete_collection_namespaced_service(
                "team one",
                DeleteOptions(dry_run=["All"]),
                label_selector="owner=async-tests",
            )
        ).items == [service]
        assert (
            await client.core_v1.delete_namespaced_service("web service", "team one")
        ).metadata.name == "web service"

        endpoints = await client.core_v1.create_namespaced_endpoints(
            "team one",
            Endpoints(
                metadata=ObjectMeta(name="web endpoints", labels={"owner": "async-tests"}),
                subsets=[
                    EndpointSubset(
                        addresses=[EndpointAddress(ip="192.0.2.20")],
                        ports=[EndpointPort(name="http", port=8080)],
                    )
                ],
            ),
        )
        assert (
            await client.core_v1.read_namespaced_endpoints("web endpoints", "team one")
        ) == endpoints
        endpoints = await client.core_v1.apply_namespaced_endpoints(
            "web endpoints",
            "team one",
            endpoints,
            field_manager="async-endpoints",
            force=False,
        )
        endpoints = await client.core_v1.patch_namespaced_endpoints(
            "web endpoints",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "endpoints"}}}),
            dry_run="All",
        )
        assert endpoints.metadata.annotations == {"patched": "endpoints"}
        assert (
            await client.core_v1.list_namespaced_endpoints(
                "team one", label_selector="owner=async-tests"
            )
        ).items == [endpoints]
        assert (
            await client.core_v1.delete_collection_namespaced_endpoints(
                "team one",
                DeleteOptions(propagation_policy="Background"),
                field_selector="metadata.name=web endpoints",
            )
        ).items == [endpoints]
        assert (
            await client.core_v1.delete_namespaced_endpoints("web endpoints", "team one")
        ).status == "Success"

    assert dict(api_server.list_queries[0].multi_items()) == {
        "continue": "service-next",
        "fieldSelector": "metadata.name=web service",
        "labelSelector": "owner=async-tests",
        "limit": "1",
    }
    assert api_server.patch_calls == [
        (
            "/api/v1/namespaces/team one/services/web service",
            "application/apply-patch+yaml",
            {"fieldManager": "async-services", "force": "true", "dryRun": "All"},
        ),
        (
            "/api/v1/namespaces/team one/services/web service",
            "application/merge-patch+json",
            {"fieldManager": "async-services"},
        ),
        (
            "/api/v1/namespaces/team one/services/web service/status",
            "application/merge-patch+json",
            {"fieldManager": "async-status", "dryRun": "All"},
        ),
        (
            "/api/v1/namespaces/team one/endpoints/web endpoints",
            "application/apply-patch+yaml",
            {"fieldManager": "async-endpoints", "force": "false"},
        ),
        (
            "/api/v1/namespaces/team one/endpoints/web endpoints",
            "application/merge-patch+json",
            {"dryRun": "All"},
        ),
    ]
    assert api_server.collection_delete_bodies == [
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": [],
            "propagationPolicy": "Background",
        },
    ]
