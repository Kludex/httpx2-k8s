from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    ControllerRevision,
    DaemonSet,
    DaemonSetSpec,
    DaemonSetUpdateStrategy,
    DeleteOptions,
    Deployment,
    DeploymentSpec,
    DeploymentStrategy,
    LabelSelector,
    MergePatch,
    ObjectMeta,
    PersistentVolumeClaim,
    PersistentVolumeClaimSpec,
    ReplicaSet,
    ReplicaSetSpec,
    RollingUpdateDaemonSet,
    RollingUpdateDeployment,
    RollingUpdateStatefulSetStrategy,
    StatefulSet,
    StatefulSetPersistentVolumeClaimRetentionPolicy,
    StatefulSetSpec,
    StatefulSetUpdateStrategy,
    VolumeResourceRequirements,
)
from tests.apps._fake import FakeAppsAPI, template


@pytest.mark.anyio
async def test_async_apps_v1_controller_lifecycles_through_httpx2() -> None:
    api_server = FakeAppsAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.apps_v1 is client.apps_v1

        deployment = await client.apps_v1.create_namespaced_deployment(
            "team one",
            Deployment(
                metadata=ObjectMeta(name="web deployment", labels={"owner": "tests"}),
                spec=DeploymentSpec(
                    replicas=0,
                    selector=LabelSelector(match_labels={"app": "deployment"}),
                    template=template("deployment"),
                    min_ready_seconds=5,
                    paused=False,
                    progress_deadline_seconds=60,
                    revision_history_limit=3,
                    strategy=DeploymentStrategy(
                        type="RollingUpdate",
                        rolling_update=RollingUpdateDeployment(max_surge="25%", max_unavailable=0),
                    ),
                ),
            ),
        )
        assert deployment.status is not None
        assert deployment.status.observed_generation == 1
        assert (
            await client.apps_v1.read_namespaced_deployment("web deployment", "team one")
        ) == deployment
        deployment = await client.apps_v1.apply_namespaced_deployment(
            "web deployment",
            "team one",
            deployment,
            field_manager="apps-tests",
            force=True,
        )
        deployment = await client.apps_v1.patch_namespaced_deployment(
            "web deployment",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "deployment"}}}),
            field_manager="apps-tests",
            dry_run="All",
        )
        assert deployment.metadata.annotations == {"patched": "deployment"}
        deployments = await client.apps_v1.list_namespaced_deployment(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=web deployment",
            limit=1,
            continue_token="next",
        )
        assert deployments.items == [deployment]
        assert deployments.metadata.continue_ == ""
        assert deployments.metadata.remaining_item_count == 0
        assert deployments.metadata.self_link is not None
        assert [
            item.metadata.name
            for item in (
                await client.apps_v1.delete_collection_namespaced_deployment(
                    "team one",
                    DeleteOptions(dry_run=["All"]),
                    label_selector="owner=tests",
                    field_selector="metadata.namespace=team one",
                    limit=1,
                    continue_token="deployment-next",
                )
            ).items
        ] == ["web deployment"]
        assert ("deployments", "team one", "web deployment") in api_server.resources
        assert (
            await client.apps_v1.delete_namespaced_deployment("web deployment", "team one")
        ).status == "Success"

        replica_set = await client.apps_v1.create_namespaced_replica_set(
            "team one",
            ReplicaSet(
                metadata=ObjectMeta(name="web replicas", labels={"owner": "tests"}),
                spec=ReplicaSetSpec(
                    replicas=0,
                    min_ready_seconds=5,
                    selector=LabelSelector(match_labels={"app": "replicas"}),
                    template=template("replicas"),
                ),
            ),
        )
        assert replica_set.status is not None
        assert replica_set.status.replicas == 0
        assert (
            await client.apps_v1.read_namespaced_replica_set("web replicas", "team one")
        ) == replica_set
        replica_set = await client.apps_v1.apply_namespaced_replica_set(
            "web replicas", "team one", replica_set, field_manager="apps-tests"
        )
        replica_set = await client.apps_v1.patch_namespaced_replica_set(
            "web replicas",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "replica-set"}}}),
        )
        assert replica_set.metadata.annotations == {"patched": "replica-set"}
        assert (await client.apps_v1.list_namespaced_replica_set("team one")).items == [replica_set]
        assert [
            item.metadata.name
            for item in (
                await client.apps_v1.delete_collection_namespaced_replica_set(
                    "team one",
                    DeleteOptions(dry_run=["All"], grace_period_seconds=0),
                    label_selector="owner=tests",
                )
            ).items
        ] == ["web replicas"]
        assert (
            await client.apps_v1.delete_namespaced_replica_set("web replicas", "team one")
        ).status == "Success"

        stateful_set = await client.apps_v1.create_namespaced_stateful_set(
            "team one",
            StatefulSet(
                metadata=ObjectMeta(name="database", labels={"owner": "tests"}),
                spec=StatefulSetSpec(
                    replicas=0,
                    service_name="database",
                    selector=LabelSelector(match_labels={"app": "database"}),
                    template=template("database"),
                    min_ready_seconds=5,
                    ordinals={"start": 2},
                    persistent_volume_claim_retention_policy=(
                        StatefulSetPersistentVolumeClaimRetentionPolicy(
                            when_deleted="Retain", when_scaled="Delete"
                        )
                    ),
                    pod_management_policy="Parallel",
                    revision_history_limit=3,
                    update_strategy=StatefulSetUpdateStrategy(
                        type="RollingUpdate",
                        rolling_update=RollingUpdateStatefulSetStrategy(
                            max_unavailable=1, partition=1
                        ),
                    ),
                    volume_claim_templates=[
                        PersistentVolumeClaim(
                            metadata=ObjectMeta(name="data"),
                            spec=PersistentVolumeClaimSpec(
                                access_modes=["ReadWriteOnce"],
                                resources=VolumeResourceRequirements(requests={"storage": "1Gi"}),
                            ),
                        )
                    ],
                ),
            ),
        )
        assert stateful_set.status is not None
        assert stateful_set.spec.ordinals == {"start": 2}
        assert (
            await client.apps_v1.read_namespaced_stateful_set("database", "team one")
        ) == stateful_set
        stateful_set = await client.apps_v1.apply_namespaced_stateful_set(
            "database",
            "team one",
            stateful_set,
            field_manager="apps-tests",
            force=False,
        )
        stateful_set = await client.apps_v1.patch_namespaced_stateful_set(
            "database",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "stateful-set"}}}),
            field_manager="apps-tests",
        )
        assert stateful_set.metadata.annotations == {"patched": "stateful-set"}
        assert (await client.apps_v1.list_namespaced_stateful_set("team one")).items == [
            stateful_set
        ]
        assert [
            item.metadata.name
            for item in (
                await client.apps_v1.delete_collection_namespaced_stateful_set(
                    "team one",
                    DeleteOptions(dry_run=["All"], propagation_policy="Foreground"),
                    label_selector="owner=tests",
                )
            ).items
        ] == ["database"]
        assert (
            await client.apps_v1.delete_namespaced_stateful_set("database", "team one")
        ).status == "Success"

        daemon_set = await client.apps_v1.create_namespaced_daemon_set(
            "team one",
            DaemonSet(
                metadata=ObjectMeta(name="agent", labels={"owner": "tests"}),
                spec=DaemonSetSpec(
                    selector=LabelSelector(match_labels={"app": "agent"}),
                    template=template("agent"),
                    min_ready_seconds=5,
                    revision_history_limit=3,
                    update_strategy=DaemonSetUpdateStrategy(
                        type="RollingUpdate",
                        rolling_update=RollingUpdateDaemonSet(max_surge=0, max_unavailable="25%"),
                    ),
                ),
            ),
        )
        assert daemon_set.status is not None
        assert daemon_set.status.desired_number_scheduled == 0
        assert (await client.apps_v1.read_namespaced_daemon_set("agent", "team one")) == daemon_set
        daemon_set = await client.apps_v1.apply_namespaced_daemon_set(
            "agent", "team one", daemon_set, field_manager="apps-tests", dry_run="All"
        )
        daemon_set = await client.apps_v1.patch_namespaced_daemon_set(
            "agent",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "daemon-set"}}}),
        )
        assert daemon_set.metadata.annotations == {"patched": "daemon-set"}
        assert (await client.apps_v1.list_namespaced_daemon_set("team one")).items == [daemon_set]
        assert [
            item.metadata.name
            for item in (
                await client.apps_v1.delete_collection_namespaced_daemon_set(
                    "team one",
                    DeleteOptions(dry_run=["All"], orphan_dependents=False),
                    label_selector="owner=tests",
                )
            ).items
        ] == ["agent"]
        assert (
            await client.apps_v1.delete_namespaced_daemon_set("agent", "team one")
        ).status == "Success"

        revision = await client.apps_v1.create_namespaced_controller_revision(
            "team one",
            ControllerRevision(
                metadata=ObjectMeta(name="history", labels={"owner": "tests"}),
                revision=7,
                data={"spec": {"replicas": 0}, "active": True},
            ),
        )
        assert revision.data == {"spec": {"replicas": 0}, "active": True}
        assert (
            await client.apps_v1.read_namespaced_controller_revision("history", "team one")
        ) == revision
        revision = await client.apps_v1.apply_namespaced_controller_revision(
            "history", "team one", revision, field_manager="apps-tests", force=True
        )
        revision = await client.apps_v1.patch_namespaced_controller_revision(
            "history",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "revision"}}}),
            field_manager="apps-tests",
        )
        assert revision.metadata.annotations == {"patched": "revision"}
        assert (await client.apps_v1.list_namespaced_controller_revision("team one")).items == [
            revision
        ]
        assert [
            item.metadata.name
            for item in (
                await client.apps_v1.delete_collection_namespaced_controller_revision(
                    "team one",
                    DeleteOptions(dry_run=["All"], propagation_policy="Orphan"),
                    label_selector="owner=tests",
                )
            ).items
        ] == ["history"]
        assert (
            await client.apps_v1.delete_namespaced_controller_revision("history", "team one")
        ).status == "Success"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "deployments",
        "deployments",
        "replicasets",
        "replicasets",
        "statefulsets",
        "statefulsets",
        "daemonsets",
        "daemonsets",
        "controllerrevisions",
        "controllerrevisions",
    ]
    assert [resource for resource, _, _ in api_server.delete_collection_calls] == [
        "deployments",
        "replicasets",
        "statefulsets",
        "daemonsets",
        "controllerrevisions",
    ]
    assert api_server.delete_collection_calls[0] == (
        "deployments",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.namespace=team one",
            "limit": "1",
            "continue": "deployment-next",
        },
        {"apiVersion": "v1", "kind": "DeleteOptions", "dryRun": ["All"]},
    )
