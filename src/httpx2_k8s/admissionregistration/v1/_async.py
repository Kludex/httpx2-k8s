from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    DeleteOptions,
    JsonPatch,
    MergePatch,
    MutatingAdmissionPolicy,
    MutatingAdmissionPolicyBinding,
    MutatingAdmissionPolicyBindingList,
    MutatingAdmissionPolicyList,
    MutatingWebhookConfiguration,
    MutatingWebhookConfigurationList,
    Status,
    ValidatingAdmissionPolicy,
    ValidatingAdmissionPolicyBinding,
    ValidatingAdmissionPolicyBindingList,
    ValidatingAdmissionPolicyList,
    ValidatingWebhookConfiguration,
    ValidatingWebhookConfigurationList,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncAdmissionRegistrationV1API:
    """Typed AdmissionRegistration v1 webhook and CEL policy operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def create_mutating_webhook_configuration(
        self, body: MutatingWebhookConfiguration
    ) -> MutatingWebhookConfiguration:
        return await self._client.request(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            response_model=MutatingWebhookConfiguration,
            body=body,
        )

    async def read_mutating_webhook_configuration(self, name: str) -> MutatingWebhookConfiguration:
        return await self._client.request(
            "GET",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingWebhookConfiguration,
        )

    async def replace_mutating_webhook_configuration(
        self,
        name: str,
        body: MutatingWebhookConfiguration,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingWebhookConfiguration:
        return await self._client.request(
            "PUT",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingWebhookConfiguration,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_mutating_webhook_configuration(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingWebhookConfiguration:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingWebhookConfiguration,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_mutating_webhook_configuration(
        self,
        name: str,
        body: MutatingWebhookConfiguration,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingWebhookConfiguration:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingWebhookConfiguration,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_mutating_webhook_configuration(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> MutatingWebhookConfigurationList:
        return await self._client.request(
            "GET",
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            response_model=MutatingWebhookConfigurationList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_mutating_webhook_configuration(self, name: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_mutating_webhook_configuration(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> MutatingWebhookConfigurationList:
        return await self._client.request(
            "DELETE",
            "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations",
            response_model=MutatingWebhookConfigurationList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_validating_webhook_configuration(
        self, body: ValidatingWebhookConfiguration
    ) -> ValidatingWebhookConfiguration:
        return await self._client.request(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            response_model=ValidatingWebhookConfiguration,
            body=body,
        )

    async def read_validating_webhook_configuration(
        self, name: str
    ) -> ValidatingWebhookConfiguration:
        return await self._client.request(
            "GET",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingWebhookConfiguration,
        )

    async def replace_validating_webhook_configuration(
        self,
        name: str,
        body: ValidatingWebhookConfiguration,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingWebhookConfiguration:
        return await self._client.request(
            "PUT",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingWebhookConfiguration,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_validating_webhook_configuration(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingWebhookConfiguration:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingWebhookConfiguration,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_validating_webhook_configuration(
        self,
        name: str,
        body: ValidatingWebhookConfiguration,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingWebhookConfiguration:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingWebhookConfiguration,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_validating_webhook_configuration(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ValidatingWebhookConfigurationList:
        return await self._client.request(
            "GET",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            response_model=ValidatingWebhookConfigurationList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_validating_webhook_configuration(self, name: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/"
                f"{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_validating_webhook_configuration(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ValidatingWebhookConfigurationList:
        return await self._client.request(
            "DELETE",
            "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations",
            response_model=ValidatingWebhookConfigurationList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_validating_admission_policy(
        self, body: ValidatingAdmissionPolicy
    ) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies",
            response_model=ValidatingAdmissionPolicy,
            body=body,
        )

    async def read_validating_admission_policy(self, name: str) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "GET",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicy,
        )

    async def replace_validating_admission_policy(
        self,
        name: str,
        body: ValidatingAdmissionPolicy,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "PUT",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def read_validating_admission_policy_status(self, name: str) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "GET",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}/status"
            ),
            response_model=ValidatingAdmissionPolicy,
        )

    async def replace_validating_admission_policy_status(
        self,
        name: str,
        body: ValidatingAdmissionPolicy,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "PUT",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}/status"
            ),
            response_model=ValidatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_validating_admission_policy_status(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}/status"
            ),
            response_model=ValidatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def patch_validating_admission_policy(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_validating_admission_policy(
        self,
        name: str,
        body: ValidatingAdmissionPolicy,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicy:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_validating_admission_policy(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ValidatingAdmissionPolicyList:
        return await self._client.request(
            "GET",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies",
            response_model=ValidatingAdmissionPolicyList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_validating_admission_policy(self, name: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_validating_admission_policy(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ValidatingAdmissionPolicyList:
        return await self._client.request(
            "DELETE",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicies",
            response_model=ValidatingAdmissionPolicyList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_validating_admission_policy_binding(
        self, body: ValidatingAdmissionPolicyBinding
    ) -> ValidatingAdmissionPolicyBinding:
        return await self._client.request(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings",
            response_model=ValidatingAdmissionPolicyBinding,
            body=body,
        )

    async def read_validating_admission_policy_binding(
        self, name: str
    ) -> ValidatingAdmissionPolicyBinding:
        return await self._client.request(
            "GET",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicyBinding,
        )

    async def replace_validating_admission_policy_binding(
        self,
        name: str,
        body: ValidatingAdmissionPolicyBinding,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicyBinding:
        return await self._client.request(
            "PUT",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicyBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_validating_admission_policy_binding(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicyBinding:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicyBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_validating_admission_policy_binding(
        self,
        name: str,
        body: ValidatingAdmissionPolicyBinding,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ValidatingAdmissionPolicyBinding:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=ValidatingAdmissionPolicyBinding,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_validating_admission_policy_binding(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ValidatingAdmissionPolicyBindingList:
        return await self._client.request(
            "GET",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings",
            response_model=ValidatingAdmissionPolicyBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_validating_admission_policy_binding(self, name: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_validating_admission_policy_binding(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ValidatingAdmissionPolicyBindingList:
        return await self._client.request(
            "DELETE",
            "/apis/admissionregistration.k8s.io/v1/validatingadmissionpolicybindings",
            response_model=ValidatingAdmissionPolicyBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_mutating_admission_policy(
        self, body: MutatingAdmissionPolicy
    ) -> MutatingAdmissionPolicy:
        return await self._client.request(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies",
            response_model=MutatingAdmissionPolicy,
            body=body,
        )

    async def read_mutating_admission_policy(self, name: str) -> MutatingAdmissionPolicy:
        return await self._client.request(
            "GET",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicy,
        )

    async def replace_mutating_admission_policy(
        self,
        name: str,
        body: MutatingAdmissionPolicy,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingAdmissionPolicy:
        return await self._client.request(
            "PUT",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_mutating_admission_policy(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingAdmissionPolicy:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_mutating_admission_policy(
        self,
        name: str,
        body: MutatingAdmissionPolicy,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingAdmissionPolicy:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicy,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_mutating_admission_policy(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> MutatingAdmissionPolicyList:
        return await self._client.request(
            "GET",
            "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies",
            response_model=MutatingAdmissionPolicyList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_mutating_admission_policy(self, name: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies/"
                f"{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_mutating_admission_policy(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> MutatingAdmissionPolicyList:
        return await self._client.request(
            "DELETE",
            "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicies",
            response_model=MutatingAdmissionPolicyList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_mutating_admission_policy_binding(
        self, body: MutatingAdmissionPolicyBinding
    ) -> MutatingAdmissionPolicyBinding:
        return await self._client.request(
            "POST",
            "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings",
            response_model=MutatingAdmissionPolicyBinding,
            body=body,
        )

    async def read_mutating_admission_policy_binding(
        self, name: str
    ) -> MutatingAdmissionPolicyBinding:
        return await self._client.request(
            "GET",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicyBinding,
        )

    async def replace_mutating_admission_policy_binding(
        self,
        name: str,
        body: MutatingAdmissionPolicyBinding,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingAdmissionPolicyBinding:
        return await self._client.request(
            "PUT",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicyBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_mutating_admission_policy_binding(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingAdmissionPolicyBinding:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicyBinding,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_mutating_admission_policy_binding(
        self,
        name: str,
        body: MutatingAdmissionPolicyBinding,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> MutatingAdmissionPolicyBinding:
        return await self._client.request(
            "PATCH",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=MutatingAdmissionPolicyBinding,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_mutating_admission_policy_binding(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> MutatingAdmissionPolicyBindingList:
        return await self._client.request(
            "GET",
            "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings",
            response_model=MutatingAdmissionPolicyBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_mutating_admission_policy_binding(self, name: str) -> Status:
        return await self._client.request(
            "DELETE",
            (
                "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings/"
                f"{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_mutating_admission_policy_binding(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> MutatingAdmissionPolicyBindingList:
        return await self._client.request(
            "DELETE",
            "/apis/admissionregistration.k8s.io/v1/mutatingadmissionpolicybindings",
            response_model=MutatingAdmissionPolicyBindingList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
