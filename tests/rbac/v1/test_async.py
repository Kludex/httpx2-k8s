from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import (
    AggregationRule,
    AsyncKubeClient,
    ClusterRole,
    ClusterRoleBinding,
    DeleteOptions,
    LabelSelector,
    MergePatch,
    ObjectMeta,
    PolicyRule,
    Role,
    RoleBinding,
    RoleRef,
    Subject,
)
from tests.rbac._fake import FakeRBACAPI


@pytest.mark.anyio
async def test_async_rbac_v1_lifecycles_through_httpx2() -> None:
    api_server = FakeRBACAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.rbac_v1 is client.rbac_v1

        role = await client.rbac_v1.create_namespaced_role(
            "team one",
            Role(
                metadata=ObjectMeta(name="config reader", labels={"owner": "tests"}),
                rules=[
                    PolicyRule(
                        api_groups=[""],
                        resources=["configmaps"],
                        resource_names=["settings"],
                        verbs=["get", "list"],
                    )
                ],
            ),
        )
        assert role.rules[0].resource_names == ["settings"]
        assert (await client.rbac_v1.read_namespaced_role("config reader", "team one")) == role
        role = await client.rbac_v1.apply_namespaced_role(
            "config reader", "team one", role, field_manager="rbac-tests", force=True
        )
        role = await client.rbac_v1.patch_namespaced_role(
            "config reader",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "role"}}}),
            field_manager="rbac-tests",
            dry_run="All",
        )
        assert role.metadata.annotations == {"patched": "role"}
        role = await client.rbac_v1.replace_namespaced_role(
            "config reader",
            "team one",
            role,
            field_manager="rbac-tests",
            dry_run="All",
        )
        roles = await client.rbac_v1.list_namespaced_role(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=config reader",
            limit=1,
            continue_token="next",
        )
        assert roles.items == [role]
        assert roles.metadata.remaining_item_count == 0
        assert (
            await client.rbac_v1.list_role_for_all_namespaces(
                label_selector="owner=tests",
                field_selector="metadata.name=config reader",
                limit=1,
                continue_token="all-role-next",
            )
        ).items == [role]
        assert [
            item.metadata.name
            for item in (
                await client.rbac_v1.delete_collection_namespaced_role(
                    "team one",
                    DeleteOptions(dry_run=["All"], propagation_policy="Background"),
                    label_selector="owner=tests",
                    field_selector="metadata.namespace=team one",
                    limit=1,
                    continue_token="role-next",
                )
            ).items
        ] == ["config reader"]

        role_binding = await client.rbac_v1.create_namespaced_role_binding(
            "team one",
            RoleBinding(
                metadata=ObjectMeta(name="read configs", labels={"owner": "tests"}),
                role_ref=RoleRef(kind="Role", name="config reader"),
                subjects=[
                    Subject(kind="ServiceAccount", name="default", namespace="team one"),
                    Subject(api_group="rbac.authorization.k8s.io", kind="Group", name="developers"),
                ],
            ),
        )
        assert role_binding.subjects[0].kind == "ServiceAccount"
        assert (
            await client.rbac_v1.read_namespaced_role_binding("read configs", "team one")
        ) == role_binding
        role_binding = await client.rbac_v1.apply_namespaced_role_binding(
            "read configs",
            "team one",
            role_binding,
            field_manager="rbac-tests",
            force=False,
            dry_run="All",
        )
        role_binding = await client.rbac_v1.patch_namespaced_role_binding(
            "read configs",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "role-binding"}}}),
        )
        assert role_binding.metadata.annotations == {"patched": "role-binding"}
        role_binding = await client.rbac_v1.replace_namespaced_role_binding(
            "read configs",
            "team one",
            role_binding,
            field_manager="rbac-tests",
            dry_run="All",
        )
        assert (await client.rbac_v1.list_namespaced_role_binding("team one")).items == [
            role_binding
        ]
        assert (
            await client.rbac_v1.list_role_binding_for_all_namespaces(
                label_selector="owner=tests",
                field_selector="metadata.name=read configs",
                limit=1,
                continue_token="all-binding-next",
            )
        ).items == [role_binding]
        assert [
            item.metadata.name
            for item in (
                await client.rbac_v1.delete_collection_namespaced_role_binding(
                    "team one",
                    DeleteOptions(dry_run=["All"], grace_period_seconds=0),
                    label_selector="owner=tests",
                )
            ).items
        ] == ["read configs"]
        assert (
            await client.rbac_v1.delete_namespaced_role_binding("read configs", "team one")
        ).status == "Success"
        assert (
            await client.rbac_v1.delete_namespaced_role("config reader", "team one")
        ).status == "Success"

        cluster_role = await client.rbac_v1.create_cluster_role(
            ClusterRole(
                metadata=ObjectMeta(name="health reader", labels={"owner": "tests"}),
                aggregation_rule=AggregationRule(
                    cluster_role_selectors=[LabelSelector(match_labels={"aggregate": "health"})]
                ),
                rules=[PolicyRule(non_resource_urls=["/healthz", "/readyz/*"], verbs=["get"])],
            )
        )
        assert cluster_role.aggregation_rule is not None
        assert (await client.rbac_v1.read_cluster_role("health reader")) == cluster_role
        cluster_role = await client.rbac_v1.apply_cluster_role(
            "health reader", cluster_role, field_manager="rbac-tests"
        )
        cluster_role = await client.rbac_v1.patch_cluster_role(
            "health reader",
            MergePatch(document={"metadata": {"annotations": {"patched": "cluster-role"}}}),
            field_manager="rbac-tests",
        )
        assert cluster_role.metadata.annotations == {"patched": "cluster-role"}
        cluster_role = await client.rbac_v1.replace_cluster_role(
            "health reader",
            cluster_role,
            field_manager="rbac-tests",
            dry_run="All",
        )
        assert (await client.rbac_v1.list_cluster_role(label_selector="owner=tests")).items == [
            cluster_role
        ]
        assert [
            item.metadata.name
            for item in (
                await client.rbac_v1.delete_collection_cluster_role(
                    DeleteOptions(dry_run=["All"], propagation_policy="Foreground"),
                    label_selector="owner=tests",
                )
            ).items
        ] == ["health reader"]

        cluster_binding = await client.rbac_v1.create_cluster_role_binding(
            ClusterRoleBinding(
                metadata=ObjectMeta(name="health readers", labels={"owner": "tests"}),
                role_ref=RoleRef(kind="ClusterRole", name="health reader"),
                subjects=[
                    Subject(
                        api_group="rbac.authorization.k8s.io",
                        kind="User",
                        name="integration-user",
                    )
                ],
            )
        )
        assert cluster_binding.role_ref.kind == "ClusterRole"
        assert (await client.rbac_v1.read_cluster_role_binding("health readers")) == cluster_binding
        cluster_binding = await client.rbac_v1.apply_cluster_role_binding(
            "health readers",
            cluster_binding,
            field_manager="rbac-tests",
            force=True,
        )
        cluster_binding = await client.rbac_v1.patch_cluster_role_binding(
            "health readers",
            MergePatch(document={"metadata": {"annotations": {"patched": "cluster-binding"}}}),
        )
        assert cluster_binding.metadata.annotations == {"patched": "cluster-binding"}
        cluster_binding = await client.rbac_v1.replace_cluster_role_binding(
            "health readers",
            cluster_binding,
            field_manager="rbac-tests",
            dry_run="All",
        )
        assert (await client.rbac_v1.list_cluster_role_binding()).items == [cluster_binding]
        assert [
            item.metadata.name
            for item in (
                await client.rbac_v1.delete_collection_cluster_role_binding(
                    DeleteOptions(dry_run=["All"], propagation_policy="Orphan"),
                    label_selector="owner=tests",
                )
            ).items
        ] == ["health readers"]
        assert (
            await client.rbac_v1.delete_cluster_role_binding("health readers")
        ).status == "Success"
        assert (await client.rbac_v1.delete_cluster_role("health reader")).status == "Success"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "roles",
        "roles",
        "rolebindings",
        "rolebindings",
        "clusterroles",
        "clusterroles",
        "clusterrolebindings",
        "clusterrolebindings",
    ]
    assert api_server.put_calls == [
        ("roles", "team one", {"fieldManager": "rbac-tests", "dryRun": "All"}),
        ("rolebindings", "team one", {"fieldManager": "rbac-tests", "dryRun": "All"}),
        ("clusterroles", "", {"fieldManager": "rbac-tests", "dryRun": "All"}),
        ("clusterrolebindings", "", {"fieldManager": "rbac-tests", "dryRun": "All"}),
    ]
    assert [resource for resource, _, _, _ in api_server.delete_collection_calls] == [
        "roles",
        "rolebindings",
        "clusterroles",
        "clusterrolebindings",
    ]
    assert api_server.delete_collection_calls[0] == (
        "roles",
        "team one",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.namespace=team one",
            "limit": "1",
            "continue": "role-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
