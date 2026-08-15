from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import AsyncKubeClient
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


@pytest.mark.anyio
async def test_async_core_replacements_and_status_reads() -> None:
    boundary = ReplacementBoundary()
    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        api = client.core_v1
        assert await api.replace_namespace("namespace one", NAMESPACE) == NAMESPACE
        assert await api.replace_node("node one", NODE) == NODE
        assert await api.replace_namespaced_event("event one", "team one", EVENT) == EVENT
        assert (
            await api.replace_namespaced_limit_range("limits one", "team one", LIMIT_RANGE)
            == LIMIT_RANGE
        )
        assert (
            await api.replace_namespaced_resource_quota("quota one", "team one", RESOURCE_QUOTA)
            == RESOURCE_QUOTA
        )
        assert (
            await api.replace_persistent_volume("volume one", PERSISTENT_VOLUME)
            == PERSISTENT_VOLUME
        )
        assert (
            await api.replace_namespaced_persistent_volume_claim(
                "claim one", "team one", PERSISTENT_VOLUME_CLAIM
            )
            == PERSISTENT_VOLUME_CLAIM
        )
        assert (
            await api.replace_namespaced_config_map("config one", "team one", CONFIG_MAP)
            == CONFIG_MAP
        )
        assert await api.replace_namespaced_secret("secret one", "team one", SECRET) == SECRET
        assert (
            await api.replace_namespaced_service_account("account one", "team one", SERVICE_ACCOUNT)
            == SERVICE_ACCOUNT
        )
        assert await api.replace_namespaced_service("service one", "team one", SERVICE) == SERVICE
        assert (
            await api.replace_namespaced_endpoints("endpoints one", "team one", ENDPOINTS)
            == ENDPOINTS
        )
        assert (
            await api.replace_namespaced_pod_template("template one", "team one", POD_TEMPLATE)
            == POD_TEMPLATE
        )
        assert (
            await api.replace_namespaced_replication_controller(
                "controller one", "team one", REPLICATION_CONTROLLER
            )
            == REPLICATION_CONTROLLER
        )
        assert await api.replace_namespaced_pod("pod one", "team one", POD) == POD
        assert await api.read_namespace_status("namespace one") == NAMESPACE
        assert await api.read_node_status("node one") == NODE
        assert (
            await api.read_namespaced_resource_quota_status("quota one", "team one")
            == RESOURCE_QUOTA
        )
        assert await api.read_persistent_volume_status("volume one") == PERSISTENT_VOLUME
        assert (
            await api.read_namespaced_persistent_volume_claim_status("claim one", "team one")
            == PERSISTENT_VOLUME_CLAIM
        )
        assert await api.read_namespaced_service_status("service one", "team one") == SERVICE
        assert (
            await api.read_namespaced_replication_controller_status("controller one", "team one")
            == REPLICATION_CONTROLLER
        )
        assert await api.read_namespaced_pod_status("pod one", "team one") == POD

    assert len(boundary.requests) == 23
    assert not boundary.requests[0].url.query
    assert boundary.requests[-1].url.path.endswith("/pods/pod one/status")
