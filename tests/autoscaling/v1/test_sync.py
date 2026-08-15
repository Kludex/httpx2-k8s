from __future__ import annotations

import httpx2

from httpx2_k8s import (
    DeleteOptions,
    HorizontalPodAutoscalerSpecV1,
    HorizontalPodAutoscalerV1,
    KubeClient,
    MergePatch,
    ObjectMeta,
)
from tests.autoscaling._fake import FakeAutoscalingAPI, target


def test_autoscaling_v1_lifecycle_through_httpx2() -> None:
    api_server = FakeAutoscalingAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.autoscaling_v1 is client.autoscaling_v1

        hpa_v1 = client.autoscaling_v1.create_namespaced_horizontal_pod_autoscaler(
            "team one",
            HorizontalPodAutoscalerV1(
                metadata=ObjectMeta(name="cpu scaler", labels={"owner": "tests"}),
                spec=HorizontalPodAutoscalerSpecV1(
                    max_replicas=10,
                    min_replicas=2,
                    scale_target_ref=target(),
                    target_cpu_utilization_percentage=60,
                ),
            ),
        )
        assert hpa_v1.status is not None
        assert hpa_v1.status.current_cpu_utilization_percentage == 40
        assert (
            client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler(
                "cpu scaler", "team one"
            )
            == hpa_v1
        )
        hpa_v1 = client.autoscaling_v1.apply_namespaced_horizontal_pod_autoscaler(
            "cpu scaler",
            "team one",
            hpa_v1,
            field_manager="autoscaling-tests",
            force=True,
        )
        hpa_v1 = client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler(
            "cpu scaler",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "v1"}}}),
            field_manager="autoscaling-tests",
            dry_run="All",
        )
        assert hpa_v1.metadata.annotations == {"patched": "v1"}
        hpa_v1 = client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler(
            "cpu scaler",
            "team one",
            hpa_v1,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert hpa_v1.metadata.resource_version == "4"
        status = client.autoscaling_v1.read_namespaced_horizontal_pod_autoscaler_status(
            "cpu scaler", "team one"
        )
        assert status.status is not None
        assert status.status.desired_replicas == 3
        status = client.autoscaling_v1.patch_namespaced_horizontal_pod_autoscaler_status(
            "cpu scaler",
            "team one",
            MergePatch(document={"status": {"desiredReplicas": 4}}),
            field_manager="status-tests",
            dry_run="All",
        )
        assert status.status is not None
        assert status.status.desired_replicas == 4
        hpa_v1 = client.autoscaling_v1.replace_namespaced_horizontal_pod_autoscaler_status(
            "cpu scaler",
            "team one",
            status,
            field_manager="status-replace-tests",
            dry_run="All",
        )
        assert hpa_v1.status is not None
        assert hpa_v1.status.desired_replicas == 4
        hpas_v1 = client.autoscaling_v1.list_namespaced_horizontal_pod_autoscaler(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=cpu scaler",
            limit=1,
            continue_token="next",
        )
        assert hpas_v1.items == [hpa_v1]
        assert hpas_v1.metadata.remaining_item_count == 0
        all_hpas_v1 = client.autoscaling_v1.list_horizontal_pod_autoscaler_for_all_namespaces(
            label_selector="owner=tests",
            field_selector="metadata.name=cpu scaler",
            limit=1,
            continue_token="all-next",
        )
        assert all_hpas_v1.items == [hpa_v1]
        deleted_hpas_v1 = (
            client.autoscaling_v1.delete_collection_namespaced_horizontal_pod_autoscaler(
                "team one",
                DeleteOptions(dry_run=["All"], propagation_policy="Background"),
                label_selector="owner=tests",
                field_selector="metadata.namespace=team one",
                limit=1,
                continue_token="hpa-next",
            )
        )
        assert [item.metadata.name for item in deleted_hpas_v1.items] == ["cpu scaler"]
        assert (
            client.autoscaling_v1.delete_namespaced_horizontal_pod_autoscaler(
                "cpu scaler", "team one"
            ).status
            == "Success"
        )

    assert [version for version, _, _ in api_server.patch_calls] == ["v1", "v1", "v1"]
    assert api_server.put_calls == [
        ("v1", None, {"fieldManager": "replace-tests", "dryRun": "All"}),
        (
            "v1",
            "status",
            {"fieldManager": "status-replace-tests", "dryRun": "All"},
        ),
    ]
    assert [version for version, _, _ in api_server.delete_collection_calls] == ["v1"]
    assert api_server.delete_collection_calls[0] == (
        "v1",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.namespace=team one",
            "limit": "1",
            "continue": "hpa-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
