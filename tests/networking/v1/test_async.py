import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    HTTPIngressPath,
    HTTPIngressRuleValue,
    Ingress,
    IngressBackend,
    IngressClass,
    IngressClassSpec,
    IngressRule,
    IngressServiceBackend,
    IngressSpec,
    IPAddress,
    IPAddressSpec,
    LabelSelector,
    MergePatch,
    NetworkPolicy,
    NetworkPolicySpec,
    ObjectMeta,
    ParentReference,
    ServiceBackendPort,
    ServiceCIDR,
    ServiceCIDRSpec,
    ServiceCIDRStatus,
    Status,
)
from tests.networking._fake import FakeNetworkingAPI


@pytest.mark.anyio
async def test_async_networking_v1_lifecycles_through_httpx2() -> None:
    api_server = FakeNetworkingAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.networking_v1 is client.networking_v1

        ingress_class = await client.networking_v1.create_ingress_class(
            IngressClass(
                metadata=ObjectMeta(name="async class", labels={"owner": "async-tests"}),
                spec=IngressClassSpec(controller="httpx2-k8s.invalid/async-controller"),
            )
        )
        assert await client.networking_v1.read_ingress_class("async class") == ingress_class
        ingress_class = await client.networking_v1.apply_ingress_class(
            "async class",
            ingress_class,
            field_manager="async-networking-tests",
            force=True,
        )
        ingress_class = await client.networking_v1.patch_ingress_class(
            "async class",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-class"}}}),
            field_manager="async-networking-tests",
            dry_run="All",
        )
        assert ingress_class.metadata.annotations == {"patched": "async-class"}
        ingress_class = await client.networking_v1.replace_ingress_class(
            "async class",
            ingress_class,
            field_manager="async-networking-tests",
            dry_run="All",
        )
        assert (
            await client.networking_v1.list_ingress_class(
                label_selector="owner=async-tests",
                field_selector="metadata.name=async class",
                limit=1,
                continue_token="async-class-next",
            )
        ).items == [ingress_class]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_ingress_class(
                    DeleteOptions(dry_run=["All"]),
                    label_selector="owner=async-tests",
                )
            ).items
        ] == ["async class"]
        assert (await client.networking_v1.delete_ingress_class("async class")).status == "Success"

        ingress = await client.networking_v1.create_namespaced_ingress(
            "async team",
            Ingress(
                metadata=ObjectMeta(name="async ingress", labels={"owner": "async-tests"}),
                spec=IngressSpec(
                    rules=[
                        IngressRule(
                            host="async.example.test",
                            http=HTTPIngressRuleValue(
                                paths=[
                                    HTTPIngressPath(
                                        path="/",
                                        path_type="Prefix",
                                        backend=IngressBackend(
                                            service=IngressServiceBackend(
                                                name="async-web",
                                                port=ServiceBackendPort(number=80),
                                            )
                                        ),
                                    )
                                ]
                            ),
                        )
                    ]
                ),
            ),
        )
        assert ingress.status is not None
        assert (
            await client.networking_v1.read_namespaced_ingress("async ingress", "async team")
        ) == ingress
        ingress = await client.networking_v1.apply_namespaced_ingress(
            "async ingress",
            "async team",
            ingress,
            field_manager="async-networking-tests",
            force=False,
            dry_run="All",
        )
        ingress = await client.networking_v1.patch_namespaced_ingress(
            "async ingress",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-ingress"}}}),
        )
        assert ingress.metadata.annotations == {"patched": "async-ingress"}
        ingress = await client.networking_v1.replace_namespaced_ingress(
            "async ingress",
            "async team",
            ingress,
            field_manager="async-networking-tests",
            dry_run="All",
        )
        assert (
            await client.networking_v1.read_namespaced_ingress_status("async ingress", "async team")
        ) == ingress
        ingress = await client.networking_v1.replace_namespaced_ingress_status(
            "async ingress",
            "async team",
            ingress,
            field_manager="async-networking-status-tests",
            dry_run="All",
        )
        ingress = await client.networking_v1.patch_namespaced_ingress_status(
            "async ingress",
            "async team",
            MergePatch(document={"status": {"loadBalancer": {"ingress": []}}}),
            field_manager="async-networking-status-tests",
            dry_run="All",
        )
        assert ingress.status is not None
        assert ingress.status.load_balancer.ingress == []
        assert (
            await client.networking_v1.list_namespaced_ingress(
                "async team", label_selector="owner=async-tests"
            )
        ).items == [ingress]
        assert (
            await client.networking_v1.list_ingress_for_all_namespaces(
                label_selector="owner=async-tests",
                field_selector="metadata.name=async ingress",
                limit=1,
                continue_token="async-ingress-next",
            )
        ).items == [ingress]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_namespaced_ingress(
                    "async team",
                    DeleteOptions(dry_run=["All"]),
                    label_selector="owner=async-tests",
                )
            ).items
        ] == ["async ingress"]
        assert (
            await client.networking_v1.delete_namespaced_ingress("async ingress", "async team")
        ).status == "Success"

        policy = await client.networking_v1.create_namespaced_network_policy(
            "async team",
            NetworkPolicy(
                metadata=ObjectMeta(name="async policy", labels={"owner": "async-tests"}),
                spec=NetworkPolicySpec(
                    pod_selector=LabelSelector(match_labels={"app": "async-web"}),
                    policy_types=["Ingress", "Egress"],
                ),
            ),
        )
        assert (
            await client.networking_v1.read_namespaced_network_policy("async policy", "async team")
        ) == policy
        policy = await client.networking_v1.apply_namespaced_network_policy(
            "async policy",
            "async team",
            policy,
            field_manager="async-networking-tests",
        )
        policy = await client.networking_v1.patch_namespaced_network_policy(
            "async policy",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-policy"}}}),
            field_manager="async-networking-tests",
            dry_run="All",
        )
        assert policy.metadata.annotations == {"patched": "async-policy"}
        policy = await client.networking_v1.replace_namespaced_network_policy(
            "async policy",
            "async team",
            policy,
            field_manager="async-networking-tests",
            dry_run="All",
        )
        assert (
            await client.networking_v1.list_namespaced_network_policy(
                "async team", label_selector="owner=async-tests"
            )
        ).items == [policy]
        assert (
            await client.networking_v1.list_network_policy_for_all_namespaces(
                label_selector="owner=async-tests",
                field_selector="metadata.name=async policy",
                limit=1,
                continue_token="async-policy-next",
            )
        ).items == [policy]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_namespaced_network_policy(
                    "async team",
                    DeleteOptions(dry_run=["All"]),
                    label_selector="owner=async-tests",
                )
            ).items
        ] == ["async policy"]
        assert (
            await client.networking_v1.delete_namespaced_network_policy(
                "async policy", "async team"
            )
        ).status == "Success"

        ip_address = await client.networking_v1.create_ip_address(
            IPAddress(
                metadata=ObjectMeta(name="2001:db8::80", labels={"owner": "async-tests"}),
                spec=IPAddressSpec(
                    parent_ref=ParentReference(
                        group="",
                        resource="services",
                        namespace="async team",
                        name="async-web",
                    )
                ),
            )
        )
        assert await client.networking_v1.read_ip_address("2001:db8::80") == ip_address
        ip_address = await client.networking_v1.apply_ip_address(
            "2001:db8::80",
            ip_address,
            field_manager="async-networking-tests",
            force=True,
            dry_run="All",
        )
        ip_address = await client.networking_v1.patch_ip_address(
            "2001:db8::80",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-ip"}}}),
            field_manager="async-networking-tests",
            dry_run="All",
        )
        ip_address = await client.networking_v1.replace_ip_address(
            "2001:db8::80",
            ip_address,
            field_manager="async-networking-tests",
            dry_run="All",
        )
        assert (
            await client.networking_v1.list_ip_address(
                label_selector="owner=async-tests",
                field_selector="metadata.name=2001:db8::80",
                limit=1,
                continue_token="async-ip-next",
            )
        ).items == [ip_address]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_ip_address(
                    DeleteOptions(dry_run=["All"]), label_selector="owner=async-tests"
                )
            ).items
        ] == ["2001:db8::80"]
        assert (await client.networking_v1.delete_ip_address("2001:db8::80")).status == "Success"

        service_cidr = await client.networking_v1.create_service_cidr(
            ServiceCIDR(
                metadata=ObjectMeta(name="async-secondary", labels={"owner": "async-tests"}),
                spec=ServiceCIDRSpec(cidrs=["2001:db8:1::/112"]),
                status=ServiceCIDRStatus(),
            )
        )
        assert await client.networking_v1.read_service_cidr("async-secondary") == service_cidr
        service_cidr = await client.networking_v1.apply_service_cidr(
            "async-secondary",
            service_cidr,
            field_manager="async-networking-tests",
            force=False,
            dry_run="All",
        )
        service_cidr = await client.networking_v1.patch_service_cidr(
            "async-secondary",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-cidr"}}}),
            field_manager="async-networking-tests",
            dry_run="All",
        )
        service_cidr = await client.networking_v1.replace_service_cidr(
            "async-secondary",
            service_cidr,
            field_manager="async-networking-tests",
            dry_run="All",
        )
        assert (
            await client.networking_v1.read_service_cidr_status("async-secondary")
        ) == service_cidr
        service_cidr = await client.networking_v1.replace_service_cidr_status(
            "async-secondary",
            service_cidr,
            field_manager="async-networking-status-tests",
            dry_run="All",
        )
        service_cidr = await client.networking_v1.patch_service_cidr_status(
            "async-secondary",
            MergePatch(document={"status": {"conditions": []}}),
            field_manager="async-networking-status-tests",
            dry_run="All",
        )
        assert service_cidr.status is not None
        assert service_cidr.status.conditions == []
        assert (
            await client.networking_v1.list_service_cidr(
                label_selector="owner=async-tests",
                field_selector="metadata.name=async-secondary",
                limit=1,
                continue_token="async-cidr-next",
            )
        ).items == [service_cidr]
        assert [
            item.metadata.name
            for item in (
                await client.networking_v1.delete_collection_service_cidr(
                    DeleteOptions(dry_run=["All"]), label_selector="owner=async-tests"
                )
            ).items
        ] == ["async-secondary"]
        deleted_service_cidr = (
            await client.networking_v1.delete_service_cidr("async-secondary")
        ).result
        assert isinstance(deleted_service_cidr, Status)
        assert deleted_service_cidr.status == "Success"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "ingressclasses",
        "ingressclasses",
        "ingresses",
        "ingresses",
        "ingresses",
        "networkpolicies",
        "networkpolicies",
        "ipaddresses",
        "ipaddresses",
        "servicecidrs",
        "servicecidrs",
        "servicecidrs",
    ]
    assert api_server.put_calls == [
        (
            "ingressclasses",
            None,
            {"fieldManager": "async-networking-tests", "dryRun": "All"},
        ),
        (
            "ingresses",
            None,
            {"fieldManager": "async-networking-tests", "dryRun": "All"},
        ),
        (
            "ingresses",
            "status",
            {"fieldManager": "async-networking-status-tests", "dryRun": "All"},
        ),
        (
            "networkpolicies",
            None,
            {"fieldManager": "async-networking-tests", "dryRun": "All"},
        ),
        (
            "ipaddresses",
            None,
            {"fieldManager": "async-networking-tests", "dryRun": "All"},
        ),
        (
            "servicecidrs",
            None,
            {"fieldManager": "async-networking-tests", "dryRun": "All"},
        ),
        (
            "servicecidrs",
            "status",
            {"fieldManager": "async-networking-status-tests", "dryRun": "All"},
        ),
    ]
    assert [resource for resource, _, _, _ in api_server.delete_collection_calls] == [
        "ingressclasses",
        "ingresses",
        "networkpolicies",
        "ipaddresses",
        "servicecidrs",
    ]
