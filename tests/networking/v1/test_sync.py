from datetime import UTC, datetime

import httpx2

from httpx2_k8s import (
    Condition,
    DeleteOptions,
    HTTPIngressPath,
    HTTPIngressRuleValue,
    Ingress,
    IngressBackend,
    IngressClass,
    IngressClassParametersReference,
    IngressClassSpec,
    IngressRule,
    IngressServiceBackend,
    IngressSpec,
    IngressTLS,
    IPAddress,
    IPAddressSpec,
    IPBlock,
    KubeClient,
    LabelSelector,
    LabelSelectorRequirement,
    MergePatch,
    NetworkPolicy,
    NetworkPolicyEgressRule,
    NetworkPolicyIngressRule,
    NetworkPolicyPeer,
    NetworkPolicyPort,
    NetworkPolicySpec,
    ObjectMeta,
    ParentReference,
    ServiceBackendPort,
    ServiceCIDR,
    ServiceCIDRSpec,
    ServiceCIDRStatus,
    Status,
    TypedLocalObjectReference,
)
from tests.networking._fake import FakeNetworkingAPI


def test_networking_v1_lifecycles_through_httpx2() -> None:
    api_server = FakeNetworkingAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.networking_v1 is client.networking_v1

        ingress_class = client.networking_v1.create_ingress_class(
            IngressClass(
                metadata=ObjectMeta(name="custom class", labels={"owner": "tests"}),
                spec=IngressClassSpec(
                    controller="httpx2-k8s.invalid/controller",
                    parameters=IngressClassParametersReference(
                        api_group="configuration.httpx2-k8s.invalid",
                        kind="IngressParameters",
                        name="shared",
                        namespace="team one",
                        scope="Namespace",
                    ),
                ),
            )
        )
        assert ingress_class.spec.parameters is not None
        assert ingress_class.spec.parameters.scope == "Namespace"
        assert client.networking_v1.read_ingress_class("custom class") == ingress_class
        ingress_class = client.networking_v1.apply_ingress_class(
            "custom class",
            ingress_class,
            field_manager="networking-tests",
            force=True,
        )
        ingress_class = client.networking_v1.patch_ingress_class(
            "custom class",
            MergePatch(document={"metadata": {"annotations": {"patched": "class"}}}),
            field_manager="networking-tests",
            dry_run="All",
        )
        assert ingress_class.metadata.annotations == {"patched": "class"}
        ingress_class = client.networking_v1.replace_ingress_class(
            "custom class",
            ingress_class,
            field_manager="networking-tests",
            dry_run="All",
        )
        classes = client.networking_v1.list_ingress_class(
            label_selector="owner=tests",
            field_selector="metadata.name=custom class",
            limit=1,
            continue_token="next",
        )
        assert classes.items == [ingress_class]
        assert classes.metadata.remaining_item_count == 0
        assert [
            item.metadata.name
            for item in client.networking_v1.delete_collection_ingress_class(
                DeleteOptions(dry_run=["All"], propagation_policy="Background"),
                label_selector="owner=tests",
                field_selector="metadata.name=custom class",
                limit=1,
                continue_token="class-next",
            ).items
        ] == ["custom class"]
        assert client.networking_v1.delete_ingress_class("custom class").status == "Success"

        ingress = client.networking_v1.create_namespaced_ingress(
            "team one",
            Ingress(
                metadata=ObjectMeta(name="web ingress", labels={"owner": "tests"}),
                spec=IngressSpec(
                    ingress_class_name="custom class",
                    default_backend=IngressBackend(
                        resource=TypedLocalObjectReference(
                            api_group="storage.example.test", kind="Bucket", name="fallback"
                        )
                    ),
                    rules=[
                        IngressRule(
                            host="app.example.test",
                            http=HTTPIngressRuleValue(
                                paths=[
                                    HTTPIngressPath(
                                        path="/api",
                                        path_type="Prefix",
                                        backend=IngressBackend(
                                            service=IngressServiceBackend(
                                                name="web",
                                                port=ServiceBackendPort(name="http", number=80),
                                            )
                                        ),
                                    )
                                ]
                            ),
                        )
                    ],
                    tls=[IngressTLS(hosts=["app.example.test"], secret_name="web-tls")],
                ),
            ),
        )
        assert ingress.status is not None
        load_balancer = ingress.status.load_balancer.ingress[0]
        assert load_balancer.hostname == "edge.example.test"
        assert load_balancer.ports[0].error == "pending"
        assert client.networking_v1.read_namespaced_ingress("web ingress", "team one") == ingress
        ingress = client.networking_v1.apply_namespaced_ingress(
            "web ingress",
            "team one",
            ingress,
            field_manager="networking-tests",
            force=False,
            dry_run="All",
        )
        ingress = client.networking_v1.patch_namespaced_ingress(
            "web ingress",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "ingress"}}}),
        )
        assert ingress.metadata.annotations == {"patched": "ingress"}
        ingress = client.networking_v1.replace_namespaced_ingress(
            "web ingress",
            "team one",
            ingress,
            field_manager="networking-tests",
            dry_run="All",
        )
        assert (
            client.networking_v1.read_namespaced_ingress_status("web ingress", "team one")
            == ingress
        )
        ingress = client.networking_v1.replace_namespaced_ingress_status(
            "web ingress",
            "team one",
            ingress,
            field_manager="networking-status-tests",
            dry_run="All",
        )
        ingress = client.networking_v1.patch_namespaced_ingress_status(
            "web ingress",
            "team one",
            MergePatch(document={"status": {"loadBalancer": {"ingress": []}}}),
            field_manager="networking-status-tests",
            dry_run="All",
        )
        assert ingress.status is not None
        assert ingress.status.load_balancer.ingress == []
        ingresses = client.networking_v1.list_namespaced_ingress(
            "team one", label_selector="owner=tests"
        )
        assert ingresses.items == [ingress]
        assert ingresses.metadata.continue_ == ""
        assert client.networking_v1.list_ingress_for_all_namespaces(
            label_selector="owner=tests",
            field_selector="metadata.name=web ingress",
            limit=1,
            continue_token="ingress-next",
        ).items == [ingress]
        assert [
            item.metadata.name
            for item in client.networking_v1.delete_collection_namespaced_ingress(
                "team one",
                DeleteOptions(dry_run=["All"], grace_period_seconds=0),
                label_selector="owner=tests",
            ).items
        ] == ["web ingress"]
        assert (
            client.networking_v1.delete_namespaced_ingress("web ingress", "team one").status
            == "Success"
        )

        policy = client.networking_v1.create_namespaced_network_policy(
            "team one",
            NetworkPolicy(
                metadata=ObjectMeta(name="restricted policy", labels={"owner": "tests"}),
                spec=NetworkPolicySpec(
                    pod_selector=LabelSelector(
                        match_labels={"app": "web"},
                        match_expressions=[
                            LabelSelectorRequirement(key="tier", operator="In", values=["frontend"])
                        ],
                    ),
                    ingress=[
                        NetworkPolicyIngressRule(
                            from_=[
                                NetworkPolicyPeer(
                                    namespace_selector=LabelSelector(
                                        match_labels={"environment": "test"}
                                    ),
                                    pod_selector=LabelSelector(match_labels={"role": "client"}),
                                )
                            ],
                            ports=[NetworkPolicyPort(port=8080, end_port=8081, protocol="TCP")],
                        )
                    ],
                    egress=[
                        NetworkPolicyEgressRule(
                            to=[
                                NetworkPolicyPeer(
                                    ip_block=IPBlock(cidr="10.0.0.0/24", except_=["10.0.0.10/32"])
                                )
                            ],
                            ports=[NetworkPolicyPort(port="dns", protocol="UDP")],
                        )
                    ],
                    policy_types=["Ingress", "Egress"],
                ),
            ),
        )
        assert policy.spec.egress[0].to[0].ip_block is not None
        assert policy.spec.egress[0].to[0].ip_block.except_ == ["10.0.0.10/32"]
        assert (
            client.networking_v1.read_namespaced_network_policy("restricted policy", "team one")
            == policy
        )
        policy = client.networking_v1.apply_namespaced_network_policy(
            "restricted policy",
            "team one",
            policy,
            field_manager="networking-tests",
        )
        policy = client.networking_v1.patch_namespaced_network_policy(
            "restricted policy",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "policy"}}}),
            field_manager="networking-tests",
        )
        assert policy.metadata.annotations == {"patched": "policy"}
        policy = client.networking_v1.replace_namespaced_network_policy(
            "restricted policy",
            "team one",
            policy,
            field_manager="networking-tests",
            dry_run="All",
        )
        assert client.networking_v1.list_namespaced_network_policy("team one").items == [policy]
        assert client.networking_v1.list_network_policy_for_all_namespaces(
            label_selector="owner=tests",
            field_selector="metadata.name=restricted policy",
            limit=1,
            continue_token="policy-next",
        ).items == [policy]
        assert [
            item.metadata.name
            for item in client.networking_v1.delete_collection_namespaced_network_policy(
                "team one",
                DeleteOptions(dry_run=["All"], propagation_policy="Foreground"),
                label_selector="owner=tests",
            ).items
        ] == ["restricted policy"]
        assert (
            client.networking_v1.delete_namespaced_network_policy(
                "restricted policy", "team one"
            ).status
            == "Success"
        )

        ip_address = client.networking_v1.create_ip_address(
            IPAddress(
                metadata=ObjectMeta(name="192.0.2.80", labels={"owner": "tests"}),
                spec=IPAddressSpec(
                    parent_ref=ParentReference(
                        group="",
                        resource="services",
                        namespace="team one",
                        name="web",
                    )
                ),
            )
        )
        assert ip_address.spec.parent_ref.namespace == "team one"
        assert client.networking_v1.read_ip_address("192.0.2.80") == ip_address
        ip_address = client.networking_v1.apply_ip_address(
            "192.0.2.80",
            ip_address,
            field_manager="networking-tests",
            force=True,
            dry_run="All",
        )
        ip_address = client.networking_v1.patch_ip_address(
            "192.0.2.80",
            MergePatch(document={"metadata": {"annotations": {"patched": "ip"}}}),
            field_manager="networking-tests",
            dry_run="All",
        )
        ip_address = client.networking_v1.replace_ip_address(
            "192.0.2.80",
            ip_address,
            field_manager="networking-tests",
            dry_run="All",
        )
        assert client.networking_v1.list_ip_address(
            label_selector="owner=tests",
            field_selector="metadata.name=192.0.2.80",
            limit=1,
            continue_token="ip-next",
        ).items == [ip_address]
        assert [
            item.metadata.name
            for item in client.networking_v1.delete_collection_ip_address(
                DeleteOptions(dry_run=["All"]), label_selector="owner=tests"
            ).items
        ] == ["192.0.2.80"]
        assert client.networking_v1.delete_ip_address("192.0.2.80").status == "Success"

        service_cidr = client.networking_v1.create_service_cidr(
            ServiceCIDR(
                metadata=ObjectMeta(name="secondary", labels={"owner": "tests"}),
                spec=ServiceCIDRSpec(cidrs=["198.51.100.0/24"]),
                status=ServiceCIDRStatus(
                    conditions=[
                        Condition(
                            type="Ready",
                            status="True",
                            last_transition_time=datetime(2026, 1, 1, tzinfo=UTC),
                            reason="Accepted",
                            message="CIDR is available",
                            observed_generation=1,
                        )
                    ]
                ),
            )
        )
        assert service_cidr.status is not None
        assert service_cidr.status.conditions[0].reason == "Accepted"
        assert client.networking_v1.read_service_cidr("secondary") == service_cidr
        service_cidr = client.networking_v1.apply_service_cidr(
            "secondary",
            service_cidr,
            field_manager="networking-tests",
            force=False,
            dry_run="All",
        )
        service_cidr = client.networking_v1.patch_service_cidr(
            "secondary",
            MergePatch(document={"metadata": {"annotations": {"patched": "cidr"}}}),
            field_manager="networking-tests",
            dry_run="All",
        )
        service_cidr = client.networking_v1.replace_service_cidr(
            "secondary",
            service_cidr,
            field_manager="networking-tests",
            dry_run="All",
        )
        assert client.networking_v1.read_service_cidr_status("secondary") == service_cidr
        service_cidr = client.networking_v1.replace_service_cidr_status(
            "secondary",
            service_cidr,
            field_manager="networking-status-tests",
            dry_run="All",
        )
        service_cidr = client.networking_v1.patch_service_cidr_status(
            "secondary",
            MergePatch(document={"status": {"conditions": []}}),
            field_manager="networking-status-tests",
            dry_run="All",
        )
        assert service_cidr.status is not None
        assert service_cidr.status.conditions == []
        assert client.networking_v1.list_service_cidr(
            label_selector="owner=tests",
            field_selector="metadata.name=secondary",
            limit=1,
            continue_token="cidr-next",
        ).items == [service_cidr]
        assert [
            item.metadata.name
            for item in client.networking_v1.delete_collection_service_cidr(
                DeleteOptions(dry_run=["All"]), label_selector="owner=tests"
            ).items
        ] == ["secondary"]
        deleted_service_cidr = client.networking_v1.delete_service_cidr("secondary").result
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
        ("ingressclasses", None, {"fieldManager": "networking-tests", "dryRun": "All"}),
        ("ingresses", None, {"fieldManager": "networking-tests", "dryRun": "All"}),
        (
            "ingresses",
            "status",
            {"fieldManager": "networking-status-tests", "dryRun": "All"},
        ),
        ("networkpolicies", None, {"fieldManager": "networking-tests", "dryRun": "All"}),
        ("ipaddresses", None, {"fieldManager": "networking-tests", "dryRun": "All"}),
        ("servicecidrs", None, {"fieldManager": "networking-tests", "dryRun": "All"}),
        (
            "servicecidrs",
            "status",
            {"fieldManager": "networking-status-tests", "dryRun": "All"},
        ),
    ]
    assert [resource for resource, _, _, _ in api_server.delete_collection_calls] == [
        "ingressclasses",
        "ingresses",
        "networkpolicies",
        "ipaddresses",
        "servicecidrs",
    ]
    assert api_server.delete_collection_calls[0] == (
        "ingressclasses",
        "",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.name=custom class",
            "limit": "1",
            "continue": "class-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
