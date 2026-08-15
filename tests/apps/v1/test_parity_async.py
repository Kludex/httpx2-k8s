from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import AsyncKubeClient, JsonPatch, JsonPatchOperation, MergePatch
from tests.apps.v1._parity_fake import (
    CONTROLLER_REVISION,
    DAEMON_SET,
    DEPLOYMENT,
    REPLICA_SET,
    SCALE,
    STATEFUL_SET,
    AppsParityBoundary,
)

pytestmark = pytest.mark.anyio

STATUS_PATCH = MergePatch(document={"status": {}})
SCALE_PATCH = JsonPatch(
    operations=[JsonPatchOperation(op="replace", path="/spec/replicas", value=2)]
)


async def test_async_apps_v1_catalog_parity_operations() -> None:
    boundary = AppsParityBoundary()
    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(boundary)
    ) as client:
        api = client.apps_v1
        assert (await api.list_deployment_for_all_namespaces()).items == [DEPLOYMENT]
        assert (await api.list_replica_set_for_all_namespaces()).items == [REPLICA_SET]
        assert (await api.list_stateful_set_for_all_namespaces()).items == [STATEFUL_SET]
        assert (await api.list_daemon_set_for_all_namespaces()).items == [DAEMON_SET]
        assert (await api.list_controller_revision_for_all_namespaces()).items == [
            CONTROLLER_REVISION
        ]

        assert (
            await api.replace_namespaced_deployment(
                "deployment one",
                "team one",
                DEPLOYMENT,
                field_manager="apps-parity",
                dry_run="All",
            )
            == DEPLOYMENT
        )
        assert (
            await api.read_namespaced_deployment_status("deployment one", "team one") == DEPLOYMENT
        )
        assert (
            await api.replace_namespaced_deployment_status("deployment one", "team one", DEPLOYMENT)
            == DEPLOYMENT
        )
        assert (
            await api.patch_namespaced_deployment_status("deployment one", "team one", STATUS_PATCH)
            == DEPLOYMENT
        )
        assert await api.read_namespaced_deployment_scale("deployment one", "team one") == SCALE
        assert (
            await api.replace_namespaced_deployment_scale("deployment one", "team one", SCALE)
            == SCALE
        )
        assert (
            await api.patch_namespaced_deployment_scale("deployment one", "team one", SCALE_PATCH)
            == SCALE
        )

        assert (
            await api.replace_namespaced_replica_set("replica one", "team one", REPLICA_SET)
            == REPLICA_SET
        )
        assert (
            await api.read_namespaced_replica_set_status("replica one", "team one") == REPLICA_SET
        )
        assert (
            await api.replace_namespaced_replica_set_status("replica one", "team one", REPLICA_SET)
            == REPLICA_SET
        )
        assert (
            await api.patch_namespaced_replica_set_status("replica one", "team one", STATUS_PATCH)
            == REPLICA_SET
        )
        assert await api.read_namespaced_replica_set_scale("replica one", "team one") == SCALE
        assert (
            await api.replace_namespaced_replica_set_scale("replica one", "team one", SCALE)
            == SCALE
        )
        assert (
            await api.patch_namespaced_replica_set_scale("replica one", "team one", SCALE_PATCH)
            == SCALE
        )

        assert (
            await api.replace_namespaced_stateful_set("stateful one", "team one", STATEFUL_SET)
            == STATEFUL_SET
        )
        assert (
            await api.read_namespaced_stateful_set_status("stateful one", "team one")
            == STATEFUL_SET
        )
        assert (
            await api.replace_namespaced_stateful_set_status(
                "stateful one", "team one", STATEFUL_SET
            )
            == STATEFUL_SET
        )
        assert (
            await api.patch_namespaced_stateful_set_status("stateful one", "team one", STATUS_PATCH)
            == STATEFUL_SET
        )
        assert await api.read_namespaced_stateful_set_scale("stateful one", "team one") == SCALE
        assert (
            await api.replace_namespaced_stateful_set_scale("stateful one", "team one", SCALE)
            == SCALE
        )
        assert (
            await api.patch_namespaced_stateful_set_scale("stateful one", "team one", SCALE_PATCH)
            == SCALE
        )

        assert (
            await api.replace_namespaced_daemon_set("daemon one", "team one", DAEMON_SET)
            == DAEMON_SET
        )
        assert await api.read_namespaced_daemon_set_status("daemon one", "team one") == DAEMON_SET
        assert (
            await api.replace_namespaced_daemon_set_status("daemon one", "team one", DAEMON_SET)
            == DAEMON_SET
        )
        assert (
            await api.patch_namespaced_daemon_set_status("daemon one", "team one", STATUS_PATCH)
            == DAEMON_SET
        )
        assert (
            await api.replace_namespaced_controller_revision(
                "revision one", "team one", CONTROLLER_REVISION
            )
            == CONTROLLER_REVISION
        )
    assert len(boundary.requests) == 31
