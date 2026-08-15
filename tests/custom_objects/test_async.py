from typing import cast

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    CustomResourceList,
    DeleteOptions,
    JsonPatch,
    JsonPatchOperation,
    MergePatch,
    ObjectMeta,
    Status,
    Unstructured,
    UnstructuredList,
)
from tests.custom_objects._fake import FakeCustomObjectsAPI, Widget, WidgetSpec


@pytest.mark.anyio
async def test_async_cluster_and_namespaced_custom_objects_through_httpx2() -> None:
    api_server = FakeCustomObjectsAPI()
    group = "testing.httpx2-k8s.dev"

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.custom_objects is client.custom_objects

        cluster = await client.custom_objects.create_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            Unstructured.model_validate(
                {
                    "apiVersion": f"{group}/v1",
                    "kind": "ClusterWidget",
                    "metadata": {"name": "global widget", "labels": {"owner": "tests"}},
                    "spec": {"region": "global", "replicas": 2},
                }
            ),
        )
        assert cluster.model_extra == {"spec": {"region": "global", "replicas": 2}}
        assert (
            await client.custom_objects.read_cluster_custom_object(
                group,
                "v1",
                "clusterwidgets",
                "global widget",
                response_model=Unstructured,
            )
            == cluster
        )
        assert cluster.model_extra is not None
        cluster_spec = cast(dict[str, object], cluster.model_extra["spec"])
        cluster_spec["replicas"] = 3
        cluster = await client.custom_objects.replace_cluster_custom_object(
            group, "v1", "clusterwidgets", "global widget", cluster
        )
        assert cluster.model_extra is not None
        assert cast(dict[str, object], cluster.model_extra["spec"])["replicas"] == 3
        cluster = await client.custom_objects.patch_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            "global widget",
            JsonPatch(
                operations=[
                    JsonPatchOperation(op="test", path="/spec/replicas", value=3),
                    JsonPatchOperation(op="replace", path="/spec/replicas", value=5),
                ]
            ),
            response_model=Unstructured,
        )
        assert cluster.model_extra is not None
        assert cast(dict[str, object], cluster.model_extra["spec"])["replicas"] == 5
        cluster_spec = cast(dict[str, object], cluster.model_extra["spec"])
        cluster_spec["replicas"] = 6
        cluster = await client.custom_objects.apply_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            "global widget",
            cluster,
            field_manager="cluster-manager",
        )
        assert cluster.model_extra is not None
        assert cast(dict[str, object], cluster.model_extra["spec"])["replicas"] == 6
        clusters = await client.custom_objects.list_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            response_model=UnstructuredList,
            label_selector="owner=tests",
            field_selector="metadata.name=global widget",
            limit=1,
            continue_token="next",
        )
        assert clusters.items == [cluster]
        assert clusters.metadata.remaining_item_count == 0
        assert (
            await client.custom_objects.delete_cluster_custom_object(
                group,
                "v1",
                "clusterwidgets",
                "global widget",
                response_model=Status,
            )
        ).status == "Success"

        await client.custom_objects.create_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            Unstructured.model_validate(
                {
                    "apiVersion": f"{group}/v1",
                    "kind": "ClusterWidget",
                    "metadata": {
                        "name": "collection one",
                        "labels": {"collection": "cluster"},
                    },
                    "spec": {"replicas": 1},
                }
            ),
        )
        await client.custom_objects.create_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            Unstructured.model_validate(
                {
                    "apiVersion": f"{group}/v1",
                    "kind": "ClusterWidget",
                    "metadata": {
                        "name": "collection two",
                        "labels": {"collection": "cluster"},
                    },
                    "spec": {"replicas": 1},
                }
            ),
        )
        dry_run_clusters = await client.custom_objects.delete_collection_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            DeleteOptions(dry_run=["All"]),
            response_model=UnstructuredList,
            label_selector="collection=cluster",
            field_selector="metadata.namespace=",
            limit=2,
            continue_token="cluster-next",
        )
        assert {item.metadata.name for item in dry_run_clusters.items} == {
            "collection one",
            "collection two",
        }
        assert ("", "collection one") in api_server.resources
        assert ("", "collection two") in api_server.resources
        deleted_clusters = await client.custom_objects.delete_collection_cluster_custom_object(
            group,
            "v1",
            "clusterwidgets",
            DeleteOptions(grace_period_seconds=0, propagation_policy="Background"),
            response_model=UnstructuredList,
            label_selector="collection=cluster",
        )
        assert {item.metadata.name for item in deleted_clusters.items} == {
            "collection one",
            "collection two",
        }
        assert ("", "collection one") not in api_server.resources
        assert ("", "collection two") not in api_server.resources

        widget = await client.custom_objects.create_namespaced_custom_object(
            group,
            "v1",
            "team one",
            "widgets",
            Widget(
                metadata=ObjectMeta(name="typed widget", labels={"owner": "tests"}),
                spec=WidgetSpec(size=2, features=["typed", "strict"]),
            ),
        )
        assert widget.spec.size == 2
        assert widget.metadata.namespace == "team one"
        assert (
            await client.custom_objects.read_namespaced_custom_object(
                group,
                "v1",
                "team one",
                "widgets",
                "typed widget",
                response_model=Widget,
            )
            == widget
        )
        widget.spec.size = 4
        widget = await client.custom_objects.replace_namespaced_custom_object(
            group, "v1", "team one", "widgets", "typed widget", widget
        )
        assert widget.spec.size == 4
        widget = await client.custom_objects.patch_namespaced_custom_object(
            group,
            "v1",
            "team one",
            "widgets",
            "typed widget",
            MergePatch(document={"spec": {"size": 6}}),
            response_model=Widget,
            field_manager="merge-manager",
            dry_run="All",
        )
        assert widget.spec.size == 6
        widget.spec.size = 8
        widget = await client.custom_objects.apply_namespaced_custom_object(
            group,
            "v1",
            "team one",
            "widgets",
            "typed widget",
            widget,
            field_manager="apply-manager",
            force=True,
        )
        assert widget.spec.size == 8
        widgets = await client.custom_objects.list_namespaced_custom_object(
            group,
            "v1",
            "team one",
            "widgets",
            response_model=CustomResourceList[Widget],
            label_selector="owner=tests",
        )
        assert widgets.items == [widget]
        assert widgets.items[0].spec.features == ["typed", "strict"]
        assert (
            await client.custom_objects.delete_namespaced_custom_object(
                group,
                "v1",
                "team one",
                "widgets",
                "typed widget",
                response_model=Status,
            )
        ).status == "Success"

        await client.custom_objects.create_namespaced_custom_object(
            group,
            "v1",
            "team one",
            "widgets",
            Widget(
                metadata=ObjectMeta(
                    name="namespaced one",
                    labels={"collection": "namespaced"},
                ),
                spec=WidgetSpec(size=1),
            ),
        )
        await client.custom_objects.create_namespaced_custom_object(
            group,
            "v1",
            "team one",
            "widgets",
            Widget(
                metadata=ObjectMeta(
                    name="namespaced two",
                    labels={"collection": "namespaced"},
                ),
                spec=WidgetSpec(size=1),
            ),
        )
        deleted_widgets = await client.custom_objects.delete_collection_namespaced_custom_object(
            group,
            "v1",
            "team one",
            "widgets",
            DeleteOptions(propagation_policy="Foreground"),
            response_model=CustomResourceList[Widget],
            label_selector="collection=namespaced",
            field_selector="metadata.namespace=team one",
            limit=2,
            continue_token="namespaced-next",
        )
        assert {item.metadata.name for item in deleted_widgets.items} == {
            "namespaced one",
            "namespaced two",
        }
        assert ("team one", "namespaced one") not in api_server.resources
        assert ("team one", "namespaced two") not in api_server.resources

    assert api_server.delete_collection_calls == [
        (
            "",
            {
                "labelSelector": "collection=cluster",
                "fieldSelector": "metadata.namespace=",
                "limit": "2",
                "continue": "cluster-next",
            },
            {"apiVersion": "v1", "kind": "DeleteOptions", "dryRun": ["All"]},
        ),
        (
            "",
            {"labelSelector": "collection=cluster"},
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "dryRun": [],
                "gracePeriodSeconds": 0,
                "propagationPolicy": "Background",
            },
        ),
        (
            "team one",
            {
                "labelSelector": "collection=namespaced",
                "fieldSelector": "metadata.namespace=team one",
                "limit": "2",
                "continue": "namespaced-next",
            },
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "dryRun": [],
                "propagationPolicy": "Foreground",
            },
        ),
    ]
