import httpx2

from httpx2_k8s import DeleteOptions, KubeClient, MergePatch, ObjectMeta, PriorityClass
from tests.scheduling._fake import FakeSchedulingAPI


def test_priority_class_lifecycle_through_httpx2() -> None:
    api_server = FakeSchedulingAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.scheduling_v1 is client.scheduling_v1
        priority = client.scheduling_v1.create_priority_class(
            PriorityClass(
                metadata=ObjectMeta(name="background class", labels={"owner": "tests"}),
                value=-100,
                description="Background workloads",
                global_default=False,
                preemption_policy="Never",
            )
        )
        assert priority.preemption_policy == "Never"
        assert client.scheduling_v1.read_priority_class("background class") == priority

        priority = client.scheduling_v1.apply_priority_class(
            "background class",
            priority,
            field_manager="control-plane-tests",
            force=True,
        )
        priority = client.scheduling_v1.patch_priority_class(
            "background class",
            MergePatch(document={"metadata": {"annotations": {"patched": "priority"}}}),
            field_manager="control-plane-tests",
            dry_run="All",
        )
        assert priority.metadata.annotations == {"patched": "priority"}
        priority = client.scheduling_v1.replace_priority_class(
            "background class",
            priority,
            field_manager="replace-tests",
            dry_run="All",
        )
        assert priority.metadata.resource_version == "4"

        priorities = client.scheduling_v1.list_priority_class(
            label_selector="owner=tests",
            field_selector="metadata.name=background class",
            limit=1,
            continue_token="next",
        )
        assert priorities.items == [priority]
        assert priorities.metadata.remaining_item_count == 0
        deleted = client.scheduling_v1.delete_collection_priority_class(
            DeleteOptions(dry_run=["All"], propagation_policy="Background"),
            label_selector="owner=tests",
            field_selector="metadata.name=background class",
            limit=1,
            continue_token="priority-next",
        )
        assert [item.metadata.name for item in deleted.items] == ["background class"]
        assert client.scheduling_v1.delete_priority_class("background class").status == "Success"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "control-plane-tests", "force": "true"},
        ),
        (
            "application/merge-patch+json",
            {"fieldManager": "control-plane-tests", "dryRun": "All"},
        ),
    ]
    assert api_server.put_calls == [{"fieldManager": "replace-tests", "dryRun": "All"}]
    assert api_server.delete_collection_call == (
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.name=background class",
            "limit": "1",
            "continue": "priority-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
