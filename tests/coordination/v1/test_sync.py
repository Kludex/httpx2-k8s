from datetime import UTC, datetime

import httpx2

from httpx2_k8s import DeleteOptions, KubeClient, Lease, LeaseSpec, MergePatch, ObjectMeta
from tests.coordination._fake import FakeCoordinationAPI


def test_lease_lifecycle_through_httpx2() -> None:
    api_server = FakeCoordinationAPI()
    now = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.coordination_v1 is client.coordination_v1
        lease = client.coordination_v1.create_namespaced_lease(
            "team one",
            Lease(
                metadata=ObjectMeta(name="leader lease", labels={"owner": "tests"}),
                spec=LeaseSpec(
                    acquire_time=now,
                    holder_identity="controller-a",
                    lease_duration_seconds=30,
                    lease_transitions=2,
                    preferred_holder="controller-b",
                    renew_time=now,
                    strategy="OldestEmulationVersion",
                ),
            ),
        )
        assert lease.spec is not None
        assert lease.spec.renew_time == now
        assert lease.spec.preferred_holder == "controller-b"
        assert client.coordination_v1.read_namespaced_lease("leader lease", "team one") == lease

        lease = client.coordination_v1.apply_namespaced_lease(
            "leader lease",
            "team one",
            lease,
            field_manager="control-plane-tests",
            force=False,
            dry_run="All",
        )
        lease = client.coordination_v1.patch_namespaced_lease(
            "leader lease",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "lease"}}}),
        )
        assert lease.metadata.annotations == {"patched": "lease"}
        lease = client.coordination_v1.replace_namespaced_lease(
            "leader lease",
            "team one",
            lease,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert lease.metadata.resource_version == "4"

        leases = client.coordination_v1.list_namespaced_lease(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=leader lease",
            limit=1,
            continue_token="next",
        )
        assert leases.items == [lease]
        assert leases.metadata.continue_ == ""
        all_leases = client.coordination_v1.list_lease_for_all_namespaces(
            label_selector="owner=tests",
            field_selector="metadata.name=leader lease",
            limit=1,
            continue_token="all-next",
        )
        assert all_leases.items == [lease]
        deleted = client.coordination_v1.delete_collection_namespaced_lease(
            "team one",
            DeleteOptions(dry_run=["All"], grace_period_seconds=0),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted.items] == ["leader lease"]
        assert (
            client.coordination_v1.delete_namespaced_lease("leader lease", "team one").status
            == "Success"
        )

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "control-plane-tests", "force": "false", "dryRun": "All"},
        ),
        ("application/merge-patch+json", {}),
    ]
    assert api_server.put_calls == [{"fieldManager": "replace-tests", "dryRun": "All"}]
    assert api_server.delete_collection_call == (
        {"labelSelector": "owner=tests"},
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "gracePeriodSeconds": 0,
        },
    )
