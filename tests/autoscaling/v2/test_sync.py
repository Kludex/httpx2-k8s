from __future__ import annotations

import httpx2

from httpx2_k8s import (
    ContainerResourceMetricSource,
    CrossVersionObjectReference,
    DeleteOptions,
    ExternalMetricSource,
    HorizontalPodAutoscalerBehavior,
    HorizontalPodAutoscalerSpecV2,
    HorizontalPodAutoscalerV2,
    HPAScalingPolicy,
    HPAScalingRules,
    KubeClient,
    LabelSelector,
    MergePatch,
    MetricIdentifier,
    MetricSpec,
    MetricTarget,
    ObjectMeta,
    ObjectMetricSource,
    PodsMetricSource,
    ResourceMetricSource,
)
from tests.autoscaling._fake import FakeAutoscalingAPI, target


def test_autoscaling_v2_lifecycle_through_httpx2() -> None:
    api_server = FakeAutoscalingAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.autoscaling_v2 is client.autoscaling_v2

        hpa_v2 = client.autoscaling_v2.create_namespaced_horizontal_pod_autoscaler(
            "team one",
            HorizontalPodAutoscalerV2(
                metadata=ObjectMeta(name="metric scaler", labels={"owner": "tests"}),
                spec=HorizontalPodAutoscalerSpecV2(
                    max_replicas=20,
                    min_replicas=1,
                    scale_target_ref=target(),
                    behavior=HorizontalPodAutoscalerBehavior(
                        scale_up=HPAScalingRules(
                            policies=[
                                HPAScalingPolicy(period_seconds=60, type="Percent", value=100),
                                HPAScalingPolicy(period_seconds=60, type="Pods", value=4),
                            ],
                            select_policy="Max",
                            stabilization_window_seconds=0,
                            tolerance="0.1",
                        ),
                        scale_down=HPAScalingRules(
                            policies=[
                                HPAScalingPolicy(period_seconds=60, type="Percent", value=50)
                            ],
                            select_policy="Min",
                            stabilization_window_seconds=300,
                        ),
                    ),
                    metrics=[
                        MetricSpec(
                            type="ContainerResource",
                            container_resource=ContainerResourceMetricSource(
                                container="web",
                                name="cpu",
                                target=MetricTarget(type="Utilization", average_utilization=60),
                            ),
                        ),
                        MetricSpec(
                            type="External",
                            external=ExternalMetricSource(
                                metric=MetricIdentifier(
                                    name="queue", selector=LabelSelector(match_labels={"q": "a"})
                                ),
                                target=MetricTarget(type="Value", value="10"),
                            ),
                        ),
                        MetricSpec(
                            type="Object",
                            object=ObjectMetricSource(
                                described_object=CrossVersionObjectReference(
                                    api_version="v1", kind="Service", name="web"
                                ),
                                metric=MetricIdentifier(name="requests"),
                                target=MetricTarget(type="Value", value="20"),
                            ),
                        ),
                        MetricSpec(
                            type="Pods",
                            pods=PodsMetricSource(
                                metric=MetricIdentifier(name="packets"),
                                target=MetricTarget(type="AverageValue", average_value="5"),
                            ),
                        ),
                        MetricSpec(
                            type="Resource",
                            resource=ResourceMetricSource(
                                name="memory",
                                target=MetricTarget(type="Utilization", average_utilization=70),
                            ),
                        ),
                    ],
                ),
            ),
        )
        assert hpa_v2.status is not None
        assert hpa_v2.status.current_metrics is not None
        assert len(hpa_v2.status.current_metrics) == 5
        assert hpa_v2.status.conditions[0].reason == "SucceededGetScale"
        assert (
            client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler(
                "metric scaler", "team one"
            )
            == hpa_v2
        )
        hpa_v2 = client.autoscaling_v2.apply_namespaced_horizontal_pod_autoscaler(
            "metric scaler",
            "team one",
            hpa_v2,
            field_manager="autoscaling-tests",
            force=False,
            dry_run="All",
        )
        hpa_v2 = client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler(
            "metric scaler",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "v2"}}}),
        )
        assert hpa_v2.metadata.annotations == {"patched": "v2"}
        hpa_v2 = client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler(
            "metric scaler",
            "team one",
            hpa_v2,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert hpa_v2.metadata.resource_version == "4"
        status = client.autoscaling_v2.read_namespaced_horizontal_pod_autoscaler_status(
            "metric scaler", "team one"
        )
        assert status.status is not None
        assert status.status.desired_replicas == 3
        status = client.autoscaling_v2.patch_namespaced_horizontal_pod_autoscaler_status(
            "metric scaler",
            "team one",
            MergePatch(document={"status": {"desiredReplicas": 4}}),
            field_manager="status-tests",
            dry_run="All",
        )
        assert status.status is not None
        assert status.status.desired_replicas == 4
        hpa_v2 = client.autoscaling_v2.replace_namespaced_horizontal_pod_autoscaler_status(
            "metric scaler",
            "team one",
            status,
            field_manager="status-replace-tests",
            dry_run="All",
        )
        assert hpa_v2.status is not None
        assert hpa_v2.status.desired_replicas == 4
        assert client.autoscaling_v2.list_namespaced_horizontal_pod_autoscaler(
            "team one"
        ).items == [hpa_v2]
        assert client.autoscaling_v2.list_horizontal_pod_autoscaler_for_all_namespaces(
            label_selector="owner=tests",
            field_selector="metadata.name=metric scaler",
            limit=1,
            continue_token="all-next",
        ).items == [hpa_v2]
        deleted_hpas_v2 = (
            client.autoscaling_v2.delete_collection_namespaced_horizontal_pod_autoscaler(
                "team one",
                DeleteOptions(dry_run=["All"], grace_period_seconds=0),
                label_selector="owner=tests",
            )
        )
        assert [item.metadata.name for item in deleted_hpas_v2.items] == ["metric scaler"]
        assert (
            client.autoscaling_v2.delete_namespaced_horizontal_pod_autoscaler(
                "metric scaler", "team one"
            ).status
            == "Success"
        )

    assert [version for version, _, _ in api_server.patch_calls] == ["v2", "v2", "v2"]
    assert [subresource for _, subresource, _ in api_server.put_calls] == [None, "status"]
    assert [version for version, _, _ in api_server.delete_collection_calls] == ["v2"]
