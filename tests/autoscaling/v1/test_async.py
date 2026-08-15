from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    HorizontalPodAutoscalerSpecV1,
    HorizontalPodAutoscalerV1,
    MergePatch,
    ObjectMeta,
)
from tests.autoscaling._fake import FakeAutoscalingAPI, target


@pytest.mark.anyio
async def test_async_autoscaling_v1_lifecycle_through_httpx2() -> None:
    api_server = FakeAutoscalingAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.autoscaling_v1 is client.autoscaling_v1

        hpa_v1 = await client.autoscaling_v1.create_namespaced_horizontal_pod_autoscaler(
            "async team",
            HorizontalPodAutoscalerV1(
                metadata=ObjectMeta(name="async v1", labels={"owner": "async"}),
                spec=HorizontalPodAutoscalerSpecV1(
                    max_replicas=3,
                    min_replicas=1,
                    scale_target_ref=target(),
                ),
            ),
        )
        assert (
            await client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler(
                "async v1", "async team"
            )
            == hpa_v1
        )
        hpa_v1 = await client.autoscaling_v1.apply_namespaced_horizontal_pod_autoscaler(
            "async v1", "async team", hpa_v1, field_manager="async-tests", force=True
        )
        hpa_v1 = await client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler(
            "async v1",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"async": "v1"}}}),
            dry_run="All",
        )
        hpa_v1 = await client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler(
            "async v1", "async team", hpa_v1, field_manager="async-replace", dry_run="All"
        )
        status = await client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler_status(
            "async v1", "async team"
        )
        status = await client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler_status(
            "async v1",
            "async team",
            MergePatch(document={"status": {"desiredReplicas": 4}}),
            dry_run="All",
        )
        hpa_v1 = await client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler_status(
            "async v1", "async team", status, field_manager="async-status"
        )
        assert hpa_v1.status is not None
        assert hpa_v1.status.desired_replicas == 4
        assert (
            await client.autoscaling_v1.list_namespaced_horizontal_pod_autoscaler(
                "async team", label_selector="owner=async"
            )
        ).items == [hpa_v1]
        assert (
            await client.autoscaling_v1.list_horizontal_pod_autoscaler_for_all_namespaces(
                label_selector="owner=async",
                field_selector="metadata.name=async v1",
                limit=1,
                continue_token="all-next",
            )
        ).items == [hpa_v1]
        assert (
            await client.autoscaling_v1.delete_collection_namespaced_horizontal_pod_autoscaler(
                "async team", DeleteOptions(dry_run=["All"]), label_selector="owner=async"
            )
        ).items == [hpa_v1]
        assert (
            await client.autoscaling_v1.delete_namespaced_horizontal_pod_autoscaler(
                "async v1", "async team"
            )
        ).status == "Success"

    assert [version for version, _, _ in api_server.patch_calls] == ["v1", "v1", "v1"]
    assert [subresource for _, subresource, _ in api_server.put_calls] == [None, "status"]
    assert [version for version, _, _ in api_server.delete_collection_calls] == ["v1"]
