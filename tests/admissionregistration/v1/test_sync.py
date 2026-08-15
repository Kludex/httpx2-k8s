from __future__ import annotations

import httpx2

from httpx2_k8s import (
    AdmissionApplyConfiguration,
    AdmissionAuditAnnotation,
    AdmissionJSONPatch,
    AdmissionMutation,
    AdmissionRegistrationV1API,
    AdmissionServiceReference,
    AdmissionValidation,
    AdmissionVariable,
    DeleteOptions,
    KubeClient,
    LabelSelector,
    MatchCondition,
    MatchResources,
    MergePatch,
    MutatingAdmissionPolicy,
    MutatingAdmissionPolicyBinding,
    MutatingAdmissionPolicyBindingSpec,
    MutatingAdmissionPolicySpec,
    MutatingWebhook,
    MutatingWebhookConfiguration,
    NamedRuleWithOperations,
    ObjectMeta,
    ParamKind,
    ParamRef,
    ValidatingAdmissionPolicy,
    ValidatingAdmissionPolicyBinding,
    ValidatingAdmissionPolicyBindingSpec,
    ValidatingAdmissionPolicySpec,
    ValidatingWebhook,
    ValidatingWebhookConfiguration,
    WebhookClientConfig,
)
from tests.admissionregistration._fake import FakeAdmissionRegistrationAPI, admission_rule


def test_all_admissionregistration_v1_lifecycles_through_httpx2() -> None:
    api_server = FakeAdmissionRegistrationAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        api: AdmissionRegistrationV1API = client.admissionregistration_v1
        assert api is client.admissionregistration_v1

        mutating = api.create_mutating_webhook_configuration(
            MutatingWebhookConfiguration(
                metadata=ObjectMeta(name="mutator config", labels={"owner": "tests"}),
                webhooks=[
                    MutatingWebhook(
                        admission_review_versions=["v1"],
                        client_config=WebhookClientConfig(
                            ca_bundle="Y2E=",
                            service=AdmissionServiceReference(
                                name="admission", namespace="system", path="/mutate", port=443
                            ),
                        ),
                        name="mutator.example.test",
                        side_effects="NoneOnDryRun",
                        failure_policy="Ignore",
                        match_conditions=[
                            MatchCondition(
                                name="not-system", expression="object.metadata.name != 'system'"
                            )
                        ],
                        match_policy="Equivalent",
                        namespace_selector=LabelSelector(match_labels={"environment": "test"}),
                        object_selector=LabelSelector(match_labels={"managed": "true"}),
                        reinvocation_policy="IfNeeded",
                        rules=[admission_rule()],
                        timeout_seconds=5,
                    )
                ],
            )
        )
        assert mutating.webhooks[0].client_config.ca_bundle == "Y2E="
        assert mutating.webhooks[0].reinvocation_policy == "IfNeeded"
        assert api.read_mutating_webhook_configuration("mutator config") == mutating
        mutating = api.apply_mutating_webhook_configuration(
            "mutator config",
            mutating,
            field_manager="admission-tests",
            force=True,
        )
        mutating = api.replace_mutating_webhook_configuration(
            "mutator config",
            mutating,
            field_manager="admission-tests",
            dry_run="All",
        )
        mutating = api.patch_mutating_webhook_configuration(
            "mutator config",
            MergePatch(document={"metadata": {"annotations": {"patched": "mutating"}}}),
            dry_run="All",
        )
        assert mutating.metadata.annotations == {"patched": "mutating"}
        mutating_list = api.list_mutating_webhook_configuration(
            label_selector="owner=tests",
            field_selector="metadata.name=mutator config",
            limit=1,
            continue_token="next",
        )
        assert mutating_list.items == [mutating]
        assert mutating_list.metadata.remaining_item_count == 0
        deleted_mutating = api.delete_collection_mutating_webhook_configuration(
            DeleteOptions(dry_run=["All"], propagation_policy="Background"),
            label_selector="owner=tests",
            field_selector="metadata.name=mutator config",
            limit=1,
            continue_token="mutating-next",
        )
        assert [item.metadata.name for item in deleted_mutating.items] == ["mutator config"]
        assert api.delete_mutating_webhook_configuration("mutator config").status == "Success"

        validating = api.create_validating_webhook_configuration(
            ValidatingWebhookConfiguration(
                metadata=ObjectMeta(name="validator config", labels={"owner": "tests"}),
                webhooks=[
                    ValidatingWebhook(
                        admission_review_versions=["v1"],
                        client_config=WebhookClientConfig(
                            url="https://admission.example.test/validate"
                        ),
                        name="validator.example.test",
                        side_effects="None",
                        failure_policy="Fail",
                        match_conditions=[MatchCondition(name="all", expression="true")],
                        match_policy="Exact",
                        namespace_selector=LabelSelector(),
                        object_selector=LabelSelector(),
                        rules=[admission_rule()],
                        timeout_seconds=10,
                    )
                ],
            )
        )
        assert validating.webhooks[0].client_config.url is not None
        assert api.read_validating_webhook_configuration("validator config") == validating
        validating = api.apply_validating_webhook_configuration(
            "validator config",
            validating,
            field_manager="admission-tests",
            force=False,
            dry_run="All",
        )
        validating = api.replace_validating_webhook_configuration(
            "validator config",
            validating,
            field_manager="admission-tests",
        )
        validating = api.patch_validating_webhook_configuration(
            "validator config",
            MergePatch(document={"metadata": {"annotations": {"patched": "validating"}}}),
        )
        assert validating.metadata.annotations == {"patched": "validating"}
        assert api.list_validating_webhook_configuration().items == [validating]
        deleted_validating = api.delete_collection_validating_webhook_configuration(
            DeleteOptions(dry_run=["All"], grace_period_seconds=0),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_validating.items] == ["validator config"]
        assert api.delete_validating_webhook_configuration("validator config").status == "Success"

        matching = MatchResources(
            exclude_resource_rules=[
                NamedRuleWithOperations(
                    api_groups=[""],
                    api_versions=["v1"],
                    operations=["DELETE"],
                    resources=["configmaps"],
                    resource_names=["protected"],
                    scope="Namespaced",
                )
            ],
            match_policy="Equivalent",
            namespace_selector=LabelSelector(match_labels={"environment": "test"}),
            object_selector=LabelSelector(match_labels={"managed": "true"}),
            resource_rules=[
                NamedRuleWithOperations(
                    api_groups=[""],
                    api_versions=["v1"],
                    operations=["CREATE", "UPDATE"],
                    resources=["configmaps"],
                    scope="Namespaced",
                )
            ],
        )
        policy = api.create_validating_admission_policy(
            ValidatingAdmissionPolicy(
                metadata=ObjectMeta(name="config policy", labels={"owner": "tests"}),
                spec=ValidatingAdmissionPolicySpec(
                    audit_annotations=[
                        AdmissionAuditAnnotation(
                            key="decision", value_expression="'accepted-' + object.metadata.name"
                        )
                    ],
                    failure_policy="Fail",
                    match_conditions=[
                        MatchCondition(name="has-name", expression="has(object.metadata.name)")
                    ],
                    match_constraints=matching,
                    param_kind=ParamKind(api_version="v1", kind="ConfigMap"),
                    validations=[
                        AdmissionValidation(
                            expression="variables.allowed",
                            message="name is forbidden",
                            message_expression="'invalid: ' + object.metadata.name",
                            reason="Invalid",
                        )
                    ],
                    variables=[
                        AdmissionVariable(
                            name="allowed", expression="object.metadata.name != 'forbidden'"
                        )
                    ],
                ),
            )
        )
        assert policy.status is not None
        assert policy.status.conditions[0].reason == "TypeCheckingSucceeded"
        assert policy.status.type_checking is not None
        assert policy.status.type_checking.expression_warnings[0].field_ref.startswith("spec")
        assert api.read_validating_admission_policy("config policy") == policy
        policy = api.apply_validating_admission_policy(
            "config policy",
            policy,
            field_manager="admission-tests",
        )
        policy = api.replace_validating_admission_policy(
            "config policy",
            policy,
            field_manager="admission-tests",
            dry_run="All",
        )
        policy = api.patch_validating_admission_policy(
            "config policy",
            MergePatch(document={"metadata": {"annotations": {"patched": "policy"}}}),
            field_manager="admission-tests",
        )
        assert policy.metadata.annotations == {"patched": "policy"}
        assert api.list_validating_admission_policy().items == [policy]
        policy_status = api.read_validating_admission_policy_status("config policy")
        policy_status = api.patch_validating_admission_policy_status(
            "config policy",
            MergePatch(document={"status": {"observedGeneration": 2}}),
            field_manager="admission-tests",
            dry_run="All",
        )
        assert policy_status.status is not None
        assert policy_status.status.observed_generation == 2
        policy = api.replace_validating_admission_policy_status(
            "config policy",
            policy_status,
            field_manager="admission-tests",
        )
        assert policy.status is not None
        assert policy.status.observed_generation == 2

        binding = api.create_validating_admission_policy_binding(
            ValidatingAdmissionPolicyBinding(
                metadata=ObjectMeta(name="config binding", labels={"owner": "tests"}),
                spec=ValidatingAdmissionPolicyBindingSpec(
                    policy_name="config policy",
                    match_resources=matching,
                    param_ref=ParamRef(
                        name="parameters",
                        namespace="team one",
                        parameter_not_found_action="Allow",
                        selector=LabelSelector(match_labels={"active": "true"}),
                    ),
                    validation_actions=["Audit", "Warn"],
                ),
            )
        )
        assert binding.spec.param_ref is not None
        assert binding.spec.param_ref.parameter_not_found_action == "Allow"
        assert api.read_validating_admission_policy_binding("config binding") == binding
        binding = api.apply_validating_admission_policy_binding(
            "config binding",
            binding,
            field_manager="admission-tests",
            force=True,
            dry_run="All",
        )
        binding = api.replace_validating_admission_policy_binding(
            "config binding",
            binding,
            field_manager="admission-tests",
        )
        binding = api.patch_validating_admission_policy_binding(
            "config binding",
            MergePatch(document={"metadata": {"annotations": {"patched": "binding"}}}),
            field_manager="admission-tests",
            dry_run="All",
        )
        assert binding.metadata.annotations == {"patched": "binding"}
        assert api.list_validating_admission_policy_binding().items == [binding]
        deleted_bindings = api.delete_collection_validating_admission_policy_binding(
            DeleteOptions(dry_run=["All"], propagation_policy="Foreground"),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_bindings.items] == ["config binding"]
        assert api.delete_validating_admission_policy_binding("config binding").status == "Success"
        deleted_policies = api.delete_collection_validating_admission_policy(
            DeleteOptions(dry_run=["All"], orphan_dependents=False),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_policies.items] == ["config policy"]
        assert api.delete_validating_admission_policy("config policy").status == "Success"

        mutation_matching = MatchResources(
            object_selector=LabelSelector(match_labels={"admission-target": "never"}),
            resource_rules=[
                NamedRuleWithOperations(
                    api_groups=[""],
                    api_versions=["v1"],
                    operations=["CREATE"],
                    resources=["configmaps"],
                    scope="Namespaced",
                )
            ],
        )
        mutating_policy = api.create_mutating_admission_policy(
            MutatingAdmissionPolicy(
                metadata=ObjectMeta(name="label policy", labels={"owner": "tests"}),
                spec=MutatingAdmissionPolicySpec(
                    failure_policy="Fail",
                    match_conditions=[MatchCondition(name="enabled", expression="true")],
                    match_constraints=mutation_matching,
                    reinvocation_policy="Never",
                    mutations=[
                        AdmissionMutation(
                            patch_type="ApplyConfiguration",
                            apply_configuration=AdmissionApplyConfiguration(
                                expression=(
                                    "Object{metadata: Object.metadata{labels: "
                                    "{'managed-by': 'httpx2-k8s'}}}"
                                )
                            ),
                        ),
                        AdmissionMutation(
                            patch_type="JSONPatch",
                            json_patch=AdmissionJSONPatch(
                                expression=(
                                    "[JSONPatch{op: 'add', path: "
                                    "'/metadata/labels/test', value: 'true'}]"
                                )
                            ),
                        ),
                    ],
                    variables=[AdmissionVariable(name="enabled", expression="true")],
                ),
            )
        )
        assert mutating_policy.spec.mutations[0].apply_configuration is not None
        assert mutating_policy.spec.mutations[1].json_patch is not None
        assert api.read_mutating_admission_policy("label policy") == mutating_policy
        mutating_policy = api.apply_mutating_admission_policy(
            "label policy",
            mutating_policy,
            field_manager="admission-tests",
            force=True,
        )
        mutating_policy = api.replace_mutating_admission_policy(
            "label policy",
            mutating_policy,
            field_manager="admission-tests",
            dry_run="All",
        )
        mutating_policy = api.patch_mutating_admission_policy(
            "label policy",
            MergePatch(document={"metadata": {"annotations": {"patched": "mutation"}}}),
            field_manager="admission-tests",
        )
        assert mutating_policy.metadata.annotations == {"patched": "mutation"}
        assert api.list_mutating_admission_policy().items == [mutating_policy]

        mutating_binding = api.create_mutating_admission_policy_binding(
            MutatingAdmissionPolicyBinding(
                metadata=ObjectMeta(name="label binding", labels={"owner": "tests"}),
                spec=MutatingAdmissionPolicyBindingSpec(
                    policy_name="label policy",
                    match_resources=mutation_matching,
                    param_ref=ParamRef(
                        name="mutation parameters",
                        parameter_not_found_action="Deny",
                    ),
                ),
            )
        )
        assert mutating_binding.spec.param_ref is not None
        assert mutating_binding.spec.param_ref.parameter_not_found_action == "Deny"
        assert api.read_mutating_admission_policy_binding("label binding") == mutating_binding
        mutating_binding = api.apply_mutating_admission_policy_binding(
            "label binding",
            mutating_binding,
            field_manager="admission-tests",
            force=False,
            dry_run="All",
        )
        mutating_binding = api.replace_mutating_admission_policy_binding(
            "label binding",
            mutating_binding,
            field_manager="admission-tests",
        )
        mutating_binding = api.patch_mutating_admission_policy_binding(
            "label binding",
            MergePatch(document={"metadata": {"annotations": {"patched": "binding"}}}),
            dry_run="All",
        )
        assert mutating_binding.metadata.annotations == {"patched": "binding"}
        assert api.list_mutating_admission_policy_binding().items == [mutating_binding]
        deleted_mutating_bindings = api.delete_collection_mutating_admission_policy_binding(
            DeleteOptions(dry_run=["All"]),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_mutating_bindings.items] == ["label binding"]
        assert api.delete_mutating_admission_policy_binding("label binding").status == "Success"
        deleted_mutating_policies = api.delete_collection_mutating_admission_policy(
            DeleteOptions(dry_run=["All"]),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_mutating_policies.items] == ["label policy"]
        assert api.delete_mutating_admission_policy("label policy").status == "Success"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "mutatingwebhookconfigurations",
        "mutatingwebhookconfigurations",
        "validatingwebhookconfigurations",
        "validatingwebhookconfigurations",
        "validatingadmissionpolicies",
        "validatingadmissionpolicies",
        "validatingadmissionpolicies",
        "validatingadmissionpolicybindings",
        "validatingadmissionpolicybindings",
        "mutatingadmissionpolicies",
        "mutatingadmissionpolicies",
        "mutatingadmissionpolicybindings",
        "mutatingadmissionpolicybindings",
    ]
    assert [content_type for _, content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/merge-patch+json",
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/apply-patch+yaml",
        "application/merge-patch+json",
    ]
    assert api_server.patch_calls[0][2] == {
        "fieldManager": "admission-tests",
        "force": "true",
    }
    assert api_server.patch_calls[2][2] == {
        "fieldManager": "admission-tests",
        "force": "false",
        "dryRun": "All",
    }
    assert [resource for resource, _, _ in api_server.delete_collection_calls] == [
        "mutatingwebhookconfigurations",
        "validatingwebhookconfigurations",
        "validatingadmissionpolicybindings",
        "validatingadmissionpolicies",
        "mutatingadmissionpolicybindings",
        "mutatingadmissionpolicies",
    ]
    assert api_server.put_calls == [
        (
            "mutatingwebhookconfigurations",
            None,
            {"fieldManager": "admission-tests", "dryRun": "All"},
        ),
        ("validatingwebhookconfigurations", None, {"fieldManager": "admission-tests"}),
        (
            "validatingadmissionpolicies",
            None,
            {"fieldManager": "admission-tests", "dryRun": "All"},
        ),
        ("validatingadmissionpolicies", "status", {"fieldManager": "admission-tests"}),
        (
            "validatingadmissionpolicybindings",
            None,
            {"fieldManager": "admission-tests"},
        ),
        (
            "mutatingadmissionpolicies",
            None,
            {"fieldManager": "admission-tests", "dryRun": "All"},
        ),
        (
            "mutatingadmissionpolicybindings",
            None,
            {"fieldManager": "admission-tests"},
        ),
    ]
    assert api_server.delete_collection_calls[0] == (
        "mutatingwebhookconfigurations",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.name=mutator config",
            "limit": "1",
            "continue": "mutating-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
