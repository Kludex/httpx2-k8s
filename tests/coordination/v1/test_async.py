from datetime import UTC, datetime

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    Lease,
    LeaseSpec,
    MergePatch,
    ObjectMeta,
)
from tests.coordination._fake import FakeCoordinationAPI


@pytest.mark.anyio
async def test_async_lease_lifecycle_through_httpx2() -> None:
    api_server = FakeCoordinationAPI()
    now = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.coordination_v1 is client.coordination_v1
        lease = await client.coordination_v1.create_namespaced_lease(
            "async team",
            Lease(
                metadata=ObjectMeta(name="async lease", labels={"owner": "async-tests"}),
                spec=LeaseSpec(holder_identity="async-controller", renew_time=now),
            ),
        )
        assert (
            await client.coordination_v1.read_namespaced_lease("async lease", "async team") == lease
        )

        lease = await client.coordination_v1.apply_namespaced_lease(
            "async lease",
            "async team",
            lease,
            field_manager="async-control-plane",
            force=False,
            dry_run="All",
        )
        lease = await client.coordination_v1.patch_namespaced_lease(
            "async lease",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-lease"}}}),
        )
        assert lease.metadata.annotations == {"patched": "async-lease"}
        lease = await client.coordination_v1.replace_namespaced_lease(
            "async lease",
            "async team",
            lease,
            field_manager="async-replace-tests",
            dry_run="All",
        )
        assert lease.metadata.resource_version == "4"

        leases = await client.coordination_v1.list_namespaced_lease(
            "async team",
            label_selector="owner=async-tests",
            field_selector="metadata.name=async lease",
            limit=1,
            continue_token="next",
        )
        assert leases.items == [lease]
        all_leases = await client.coordination_v1.list_lease_for_all_namespaces(
            label_selector="owner=async-tests",
            field_selector="metadata.name=async lease",
            limit=1,
            continue_token="all-next",
        )
        assert all_leases.items == [lease]
        deleted = await client.coordination_v1.delete_collection_namespaced_lease(
            "async team",
            DeleteOptions(dry_run=["All"]),
            label_selector="owner=async-tests",
        )
        assert [item.metadata.name for item in deleted.items] == ["async lease"]
        assert (
            await client.coordination_v1.delete_namespaced_lease("async lease", "async team")
        ).status == "Success"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "async-control-plane", "force": "false", "dryRun": "All"},
        ),
        ("application/merge-patch+json", {}),
    ]
    assert api_server.put_calls == [{"fieldManager": "async-replace-tests", "dryRun": "All"}]
    assert api_server.delete_collection_call == (
        {"labelSelector": "owner=async-tests"},
        {"apiVersion": "v1", "kind": "DeleteOptions", "dryRun": ["All"]},
    )
