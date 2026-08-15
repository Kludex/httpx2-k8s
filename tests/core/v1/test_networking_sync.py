import httpx2

from httpx2_k8s import (
    EndpointAddress,
    EndpointPort,
    Endpoints,
    EndpointSubset,
    KubeClient,
    MergePatch,
    ObjectMeta,
    ObjectReference,
    Service,
    ServicePort,
    ServiceSpec,
)
from tests._networking_fake import FakeNetworkingAPI


def test_service_and_endpoints_lifecycle_through_httpx2() -> None:
    api_server = FakeNetworkingAPI()
    transport = httpx2.MockTransport(api_server)

    with KubeClient("https://kubernetes.invalid", transport=transport) as client:
        service = client.core_v1.create_namespaced_service(
            "team one",
            Service(
                metadata=ObjectMeta(name="web service", labels={"owner": "tests"}),
                spec=ServiceSpec(
                    selector={"app": "web"},
                    ports=[
                        ServicePort(
                            name="http",
                            port=80,
                            protocol="TCP",
                            app_protocol="http",
                            target_port="http",
                        )
                    ],
                    external_traffic_policy="Cluster",
                    internal_traffic_policy="Cluster",
                    session_affinity="None",
                    publish_not_ready_addresses=True,
                ),
            ),
        )
        assert service.spec is not None
        assert service.spec.cluster_ip == "10.43.0.10"
        assert service.spec.cluster_ips == ["10.43.0.10"]
        assert client.core_v1.read_namespaced_service("web service", "team one") == service
        service = client.core_v1.apply_namespaced_service(
            "web service",
            "team one",
            service,
            field_manager="networking-tests",
            force=True,
        )
        service = client.core_v1.patch_namespaced_service(
            "web service",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "service"}}}),
            dry_run="All",
        )
        assert service.metadata.annotations == {"patched": "service"}
        services = client.core_v1.list_namespaced_service(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=web service",
            limit=1,
            continue_token="next",
        )
        assert services.items == [service]
        assert dict(api_server.queries[-1].multi_items()) == {
            "continue": "next",
            "fieldSelector": "metadata.name=web service",
            "labelSelector": "owner=tests",
            "limit": "1",
        }

        endpoints = client.core_v1.create_namespaced_endpoints(
            "team one",
            Endpoints(
                metadata=ObjectMeta(name="web service", labels={"owner": "tests"}),
                subsets=[
                    EndpointSubset(
                        addresses=[
                            EndpointAddress(
                                ip="10.0.0.10",
                                hostname="web-0",
                                node_name="node-1",
                                target_ref=ObjectReference(
                                    api_version="v1",
                                    kind="Pod",
                                    name="web-0",
                                    namespace="team one",
                                    uid="pod-uid",
                                    field_path="spec.containers{web}",
                                    resource_version="1",
                                ),
                            )
                        ],
                        not_ready_addresses=[EndpointAddress(ip="10.0.0.11")],
                        ports=[
                            EndpointPort(
                                name="http", port=8080, protocol="TCP", app_protocol="http"
                            )
                        ],
                    )
                ],
            ),
        )
        assert endpoints.subsets[0].addresses[0].target_ref is not None
        assert client.core_v1.read_namespaced_endpoints("web service", "team one") == endpoints
        endpoints = client.core_v1.apply_namespaced_endpoints(
            "web service",
            "team one",
            endpoints,
            field_manager="networking-tests",
            force=False,
            dry_run="All",
        )
        endpoints = client.core_v1.patch_namespaced_endpoints(
            "web service",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "endpoints"}}}),
            field_manager="networking-tests",
        )
        assert endpoints.metadata.annotations == {"patched": "endpoints"}
        assert client.core_v1.list_namespaced_endpoints("team one").items == [endpoints]
        assert (
            client.core_v1.delete_namespaced_endpoints("web service", "team one").status
            == "Success"
        )
        deleted_service = client.core_v1.delete_namespaced_service("web service", "team one")
        assert deleted_service.metadata.name == "web service"
        assert deleted_service.status is not None

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "services",
        "services",
        "endpoints",
        "endpoints",
    ]
    assert [content_type for _, content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
    ] * 2
