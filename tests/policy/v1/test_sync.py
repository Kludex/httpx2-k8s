from typing import cast

import httpx2

from httpx2_k8s import (
    DeleteOptions,
    Eviction,
    KubeClient,
    LabelSelector,
    MergePatch,
    ObjectMeta,
    PodDisruptionBudget,
    PodDisruptionBudgetSpec,
    Preconditions,
)
from tests.policy._fake import FakePolicyAPI


def test_budget_and_eviction_through_httpx2() -> None:
    api_server = FakePolicyAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.policy_v1 is client.policy_v1
        budget = client.policy_v1.create_namespaced_pod_disruption_budget(
            "team one",
            PodDisruptionBudget(
                metadata=ObjectMeta(name="web budget", labels={"owner": "tests"}),
                spec=PodDisruptionBudgetSpec(
                    max_unavailable="25%",
                    min_available=1,
                    selector=LabelSelector(match_labels={"app": "web"}),
                    unhealthy_pod_eviction_policy="AlwaysAllow",
                ),
            ),
        )
        assert budget.status is not None
        assert budget.status.disrupted_pods["old-pod"].year == 2026
        assert budget.status.conditions[0].reason == "InsufficientPods"
        assert (
            client.policy_v1.read_namespaced_pod_disruption_budget("web budget", "team one")
            == budget
        )
        budget = client.policy_v1.apply_namespaced_pod_disruption_budget(
            "web budget",
            "team one",
            budget,
            field_manager="policy-tests",
            force=True,
        )
        budget = client.policy_v1.patch_namespaced_pod_disruption_budget(
            "web budget",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "budget"}}}),
            field_manager="policy-tests",
            dry_run="All",
        )
        assert budget.metadata.annotations == {"patched": "budget"}
        budget = client.policy_v1.replace_namespaced_pod_disruption_budget(
            "web budget",
            "team one",
            budget,
            field_manager="policy-tests",
            dry_run="All",
        )
        assert (
            client.policy_v1.read_namespaced_pod_disruption_budget_status("web budget", "team one")
            == budget
        )
        budget = client.policy_v1.replace_namespaced_pod_disruption_budget_status(
            "web budget",
            "team one",
            budget,
            field_manager="policy-status-tests",
            dry_run="All",
        )
        budget = client.policy_v1.patch_namespaced_pod_disruption_budget_status(
            "web budget",
            "team one",
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
            field_manager="policy-status-tests",
            dry_run="All",
        )
        assert budget.status is not None
        assert budget.status.disruptions_allowed == 1
        budgets = client.policy_v1.list_namespaced_pod_disruption_budget(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=web budget",
            limit=1,
            continue_token="next",
        )
        assert budgets.items == [budget]
        assert budgets.metadata.remaining_item_count == 0
        assert client.policy_v1.list_pod_disruption_budget_for_all_namespaces(
            label_selector="owner=tests",
            field_selector="metadata.name=web budget",
            limit=1,
            continue_token="all-budget-next",
        ).items == [budget]
        deleted = client.policy_v1.delete_collection_namespaced_pod_disruption_budget(
            "team one",
            DeleteOptions(dry_run=["All"], propagation_policy="Background"),
            label_selector="owner=tests",
            field_selector="metadata.namespace=team one",
            limit=1,
            continue_token="budget-next",
        )
        assert [item.metadata.name for item in deleted.items] == ["web budget"]
        assert (
            client.policy_v1.delete_namespaced_pod_disruption_budget(
                "web budget", "team one"
            ).status
            == "Success"
        )
        eviction = client.policy_v1.create_namespaced_pod_eviction(
            "pod one",
            "team one",
            Eviction(
                metadata=ObjectMeta(name="pod one", namespace="team one"),
                delete_options=DeleteOptions(
                    dry_run=["All"],
                    grace_period_seconds=5,
                    orphan_dependents=False,
                    preconditions=Preconditions(resource_version="3", uid="pod-uid"),
                    propagation_policy="Foreground",
                ),
            ),
        )
        assert eviction.status == "Success"
        assert api_server.last_eviction is not None
        options = cast(dict[str, object], api_server.last_eviction["deleteOptions"])
        assert options["gracePeriodSeconds"] == 5
        assert options["propagationPolicy"] == "Foreground"

    assert [content_type for content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/merge-patch+json",
    ]
    assert api_server.put_calls == [
        (None, {"fieldManager": "policy-tests", "dryRun": "All"}),
        ("status", {"fieldManager": "policy-status-tests", "dryRun": "All"}),
    ]
    assert api_server.delete_collection_calls == [
        (
            {
                "labelSelector": "owner=tests",
                "fieldSelector": "metadata.namespace=team one",
                "limit": "1",
                "continue": "budget-next",
            },
            {
                "apiVersion": "v1",
                "kind": "DeleteOptions",
                "dryRun": ["All"],
                "propagationPolicy": "Background",
            },
        )
    ]
