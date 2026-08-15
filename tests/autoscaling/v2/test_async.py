from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    HorizontalPodAutoscalerSpecV2,
    HorizontalPodAutoscalerV2,
    MergePatch,
    ObjectMeta,
)
from tests.autoscaling._fake import FakeAutoscalingAPI, target


@pytest.mark.anyio
async def test_async_autoscaling_v2_lifecycle_through_httpx2() -> None:
    api_server = FakeAutoscalingAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.autoscaling_v2 is client.autoscaling_v2

        hpa_v2 = await client.autoscaling_v2.create_namespaced_horizontal_pod_autoscaler(
            "async team",
            HorizontalPodAutoscalerV2(
                metadata=ObjectMeta(name="async v2", labels={"owner": "async"}),
                spec=HorizontalPodAutoscalerSpecV2(
                    max_replicas=4,
                    min_replicas=1,
                    scale_target_ref=target(),
                ),
            ),
        )
        assert (
            await client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler(
                "async v2", "async team"
            )
            == hpa_v2
        )
        hpa_v2 = await client.autoscaling_v2.apply_namespaced_horizontal_pod_autoscaler(
            "async v2", "async team", hpa_v2, field_manager="async-tests", force=False
        )
        hpa_v2 = await client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler(
            "async v2",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"async": "v2"}}}),
        )
        hpa_v2 = await client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler(
            "async v2", "async team", hpa_v2, field_manager="async-replace", dry_run="All"
        )
        status = await client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler_status(
            "async v2", "async team"
        )
        status = await client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler_status(
            "async v2",
            "async team",
            MergePatch(document={"status": {"desiredReplicas": 5}}),
            dry_run="All",
        )
        hpa_v2 = await client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler_status(
            "async v2", "async team", status, field_manager="async-status"
        )
        assert hpa_v2.status is not None
        assert hpa_v2.status.desired_replicas == 5
        assert (
            await client.autoscaling_v2.list_namespaced_horizontal_pod_autoscaler("async team")
        ).items == [hpa_v2]
        assert (
            await client.autoscaling_v2.list_horizontal_pod_autoscaler_for_all_namespaces(
                label_selector="owner=async",
                field_selector="metadata.name=async v2",
                limit=1,
                continue_token="all-next",
            )
        ).items == [hpa_v2]
        assert (
            await client.autoscaling_v2.delete_collection_namespaced_horizontal_pod_autoscaler(
                "async team", DeleteOptions(dry_run=["All"])
            )
        ).items == [hpa_v2]
        assert (
            await client.autoscaling_v2.delete_namespaced_horizontal_pod_autoscaler(
                "async v2", "async team"
            )
        ).status == "Success"

    assert [version for version, _, _ in api_server.patch_calls] == ["v2", "v2", "v2"]
    assert [subresource for _, subresource, _ in api_server.put_calls] == [None, "status"]
    assert [version for version, _, _ in api_server.delete_collection_calls] == ["v2"]
