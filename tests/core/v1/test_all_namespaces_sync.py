from __future__ import annotations

import httpx2

from httpx2_k8s import (
    ConfigMapList,
    EndpointsList,
    EventList,
    KubeClient,
    LimitRangeList,
    PersistentVolumeClaimList,
    PodList,
    PodTemplateList,
    ReplicationControllerList,
    ResourceQuotaList,
    SecretList,
    ServiceAccountList,
    ServiceList,
)
from tests.core.v1._all_namespaces_fake import AllNamespacesBoundary


def test_sync_core_lists_for_all_namespaces() -> None:
    boundary = AllNamespacesBoundary()
    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        api = client.core_v1
        assert isinstance(
            api.list_config_map_for_all_namespaces(
                label_selector="scope=all",
                field_selector="metadata.namespace!=default",
                limit=7,
                continue_token="next page",
            ),
            ConfigMapList,
        )
        assert isinstance(api.list_endpoints_for_all_namespaces(), EndpointsList)
        assert isinstance(api.list_event_for_all_namespaces(), EventList)
        assert isinstance(api.list_limit_range_for_all_namespaces(), LimitRangeList)
        assert isinstance(
            api.list_persistent_volume_claim_for_all_namespaces(),
            PersistentVolumeClaimList,
        )
        assert isinstance(api.list_pod_for_all_namespaces(), PodList)
        assert isinstance(api.list_pod_template_for_all_namespaces(), PodTemplateList)
        assert isinstance(
            api.list_replication_controller_for_all_namespaces(),
            ReplicationControllerList,
        )
        assert isinstance(api.list_resource_quota_for_all_namespaces(), ResourceQuotaList)
        assert isinstance(api.list_secret_for_all_namespaces(), SecretList)
        assert isinstance(api.list_service_account_for_all_namespaces(), ServiceAccountList)
        assert isinstance(api.list_service_for_all_namespaces(), ServiceList)
    assert len(boundary.requests) == 12
