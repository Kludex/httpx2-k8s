import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    DeleteOptions,
    Eviction,
    LabelSelector,
    MergePatch,
    ObjectMeta,
    PodDisruptionBudget,
    PodDisruptionBudgetSpec,
)
from tests.policy._fake import FakePolicyAPI


@pytest.mark.anyio
async def test_async_budget_and_eviction_through_httpx2() -> None:
    api_server = FakePolicyAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.policy_v1 is client.policy_v1
        budget = await client.policy_v1.create_namespaced_pod_disruption_budget(
            "async team",
            PodDisruptionBudget(
                metadata=ObjectMeta(name="async budget", labels={"owner": "async-tests"}),
                spec=PodDisruptionBudgetSpec(
                    min_available=1,
                    selector=LabelSelector(match_labels={"app": "async-web"}),
                ),
            ),
        )
        assert (
            await client.policy_v1.read_namespaced_pod_disruption_budget(
                "async budget", "async team"
            )
            == budget
        )
        budget = await client.policy_v1.apply_namespaced_pod_disruption_budget(
            "async budget",
            "async team",
            budget,
            field_manager="async-policy",
            force=True,
        )
        budget = await client.policy_v1.patch_namespaced_pod_disruption_budget(
            "async budget",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-budget"}}}),
            field_manager="async-policy",
            dry_run="All",
        )
        assert budget.metadata.annotations == {"patched": "async-budget"}
        budget = await client.policy_v1.replace_namespaced_pod_disruption_budget(
            "async budget",
            "async team",
            budget,
            field_manager="async-policy",
            dry_run="All",
        )
        assert (
            await client.policy_v1.read_namespaced_pod_disruption_budget_status(
                "async budget", "async team"
            )
        ) == budget
        budget = await client.policy_v1.replace_namespaced_pod_disruption_budget_status(
            "async budget",
            "async team",
            budget,
            field_manager="async-policy-status",
            dry_run="All",
        )
        budget = await client.policy_v1.patch_namespaced_pod_disruption_budget_status(
            "async budget",
            "async team",
            MergePatch(
                document={
                    "status": {
                        "currentHealthy": 1,
                        "desiredHealthy": 1,
                        "disruptionsAllowed": 1,
                        "expectedPods": 1,
                    }
                }
            ),
            field_manager="async-policy-status",
            dry_run="All",
        )
        assert budget.status is not None
        assert budget.status.disruptions_allowed == 1
        budgets = await client.policy_v1.list_namespaced_pod_disruption_budget(
            "async team",
            label_selector="owner=async-tests",
            field_selector="metadata.name=async budget",
            limit=1,
            continue_token="next",
        )
        assert budgets.items == [budget]
        assert (
            await client.policy_v1.list_pod_disruption_budget_for_all_namespaces(
                label_selector="owner=async-tests",
                field_selector="metadata.name=async budget",
                limit=1,
                continue_token="all-budget-next",
            )
        ).items == [budget]
        deleted = await client.policy_v1.delete_collection_namespaced_pod_disruption_budget(
            "async team",
            DeleteOptions(dry_run=["All"]),
            label_selector="owner=async-tests",
        )
        assert [item.metadata.name for item in deleted.items] == ["async budget"]
        assert (
            await client.policy_v1.delete_namespaced_pod_disruption_budget(
                "async budget", "async team"
            )
        ).status == "Success"
        eviction = await client.policy_v1.create_namespaced_pod_eviction(
            "async pod",
            "async team",
            Eviction(
                metadata=ObjectMeta(name="async pod", namespace="async team"),
                delete_options=DeleteOptions(grace_period_seconds=0),
            ),
        )
        assert eviction.status == "Success"

    assert [content_type for content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/merge-patch+json",
    ]
    assert api_server.put_calls == [
        (None, {"fieldManager": "async-policy", "dryRun": "All"}),
        ("status", {"fieldManager": "async-policy-status", "dryRun": "All"}),
    ]
