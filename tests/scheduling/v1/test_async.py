import httpx2
import pytest

from httpx2_k8s import AsyncKubeClient, DeleteOptions, MergePatch, ObjectMeta, PriorityClass
from tests.scheduling._fake import FakeSchedulingAPI


@pytest.mark.anyio
async def test_async_priority_class_lifecycle_through_httpx2() -> None:
    api_server = FakeSchedulingAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.scheduling_v1 is client.scheduling_v1
        priority = await client.scheduling_v1.create_priority_class(
            PriorityClass(
                metadata=ObjectMeta(name="async background", labels={"owner": "async-tests"}),
                value=-200,
                global_default=False,
            )
        )
        assert await client.scheduling_v1.read_priority_class("async background") == priority

        priority = await client.scheduling_v1.apply_priority_class(
            "async background",
            priority,
            field_manager="async-control-plane",
            force=True,
        )
        priority = await client.scheduling_v1.patch_priority_class(
            "async background",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-priority"}}}),
            dry_run="All",
        )
        assert priority.metadata.annotations == {"patched": "async-priority"}
        priority = await client.scheduling_v1.replace_priority_class(
            "async background",
            priority,
            field_manager="async-replace-tests",
            dry_run="All",
        )
        assert priority.metadata.resource_version == "4"

        priorities = await client.scheduling_v1.list_priority_class(
            label_selector="owner=async-tests",
            field_selector="metadata.name=async background",
            limit=1,
            continue_token="next",
        )
        assert priorities.items == [priority]
        deleted = await client.scheduling_v1.delete_collection_priority_class(
            DeleteOptions(dry_run=["All"]),
            label_selector="owner=async-tests",
        )
        assert [item.metadata.name for item in deleted.items] == ["async background"]
        assert (
            await client.scheduling_v1.delete_priority_class("async background")
        ).status == "Success"

    assert api_server.patch_calls == [
        (
            "application/apply-patch+yaml",
            {"fieldManager": "async-control-plane", "force": "true"},
        ),
        ("application/merge-patch+json", {"dryRun": "All"}),
    ]
    assert api_server.put_calls == [{"fieldManager": "async-replace-tests", "dryRun": "All"}]
    assert api_server.delete_collection_call == (
        {"labelSelector": "owner=async-tests"},
        {"apiVersion": "v1", "kind": "DeleteOptions", "dryRun": ["All"]},
    )
