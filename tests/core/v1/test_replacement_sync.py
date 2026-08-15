from __future__ import annotations

import httpx2

from httpx2_k8s import KubeClient
from tests.core.v1._replacement_fake import (
    CONFIG_MAP,
    ENDPOINTS,
    EVENT,
    LIMIT_RANGE,
    NAMESPACE,
    NODE,
    PERSISTENT_VOLUME,
    PERSISTENT_VOLUME_CLAIM,
    POD,
    POD_TEMPLATE,
    REPLICATION_CONTROLLER,
    RESOURCE_QUOTA,
    SECRET,
    SERVICE,
    SERVICE_ACCOUNT,
    ReplacementBoundary,
)


def test_sync_core_replacements_and_status_reads() -> None:
    boundary = ReplacementBoundary()
    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        api = client.core_v1
        assert (
            api.replace_namespace(
                "namespace one", NAMESPACE, field_manager="replacement-tests", dry_run="All"
            )
            == NAMESPACE
        )
        assert api.replace_node("node one", NODE) == NODE
        assert api.replace_namespaced_event("event one", "team one", EVENT) == EVENT
        assert (
            api.replace_namespaced_limit_range("limits one", "team one", LIMIT_RANGE) == LIMIT_RANGE
        )
        assert (
            api.replace_namespaced_resource_quota("quota one", "team one", RESOURCE_QUOTA)
            == RESOURCE_QUOTA
        )
        assert api.replace_persistent_volume("volume one", PERSISTENT_VOLUME) == PERSISTENT_VOLUME
        assert (
            api.replace_namespaced_persistent_volume_claim(
                "claim one", "team one", PERSISTENT_VOLUME_CLAIM
            )
            == PERSISTENT_VOLUME_CLAIM
        )
        assert api.replace_namespaced_config_map("config one", "team one", CONFIG_MAP) == CONFIG_MAP
        assert api.replace_namespaced_secret("secret one", "team one", SECRET) == SECRET
        assert (
            api.replace_namespaced_service_account("account one", "team one", SERVICE_ACCOUNT)
            == SERVICE_ACCOUNT
        )
        assert api.replace_namespaced_service("service one", "team one", SERVICE) == SERVICE
        assert api.replace_namespaced_endpoints("endpoints one", "team one", ENDPOINTS) == ENDPOINTS
        assert (
            api.replace_namespaced_pod_template("template one", "team one", POD_TEMPLATE)
            == POD_TEMPLATE
        )
        assert (
            api.replace_namespaced_replication_controller(
                "controller one", "team one", REPLICATION_CONTROLLER
            )
            == REPLICATION_CONTROLLER
        )
        assert api.replace_namespaced_pod("pod one", "team one", POD) == POD
        assert api.read_namespace_status("namespace one") == NAMESPACE
        assert api.read_node_status("node one") == NODE
        assert api.read_namespaced_resource_quota_status("quota one", "team one") == RESOURCE_QUOTA
        assert api.read_persistent_volume_status("volume one") == PERSISTENT_VOLUME
        assert (
            api.read_namespaced_persistent_volume_claim_status("claim one", "team one")
            == PERSISTENT_VOLUME_CLAIM
        )
        assert api.read_namespaced_service_status("service one", "team one") == SERVICE
        assert (
            api.read_namespaced_replication_controller_status("controller one", "team one")
            == REPLICATION_CONTROLLER
        )
        assert api.read_namespaced_pod_status("pod one", "team one") == POD

    assert len(boundary.requests) == 23
    assert boundary.requests[0].method == "PUT"
    assert dict(boundary.requests[0].url.params) == {
        "fieldManager": "replacement-tests",
        "dryRun": "All",
    }
    assert boundary.requests[-1].url.path.endswith("/pods/pod one/status")
