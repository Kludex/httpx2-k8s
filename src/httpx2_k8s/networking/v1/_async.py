from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    DeleteOptions,
    DeleteResult,
    Ingress,
    IngressClass,
    IngressClassList,
    IngressList,
    IPAddress,
    IPAddressList,
    JsonPatch,
    MergePatch,
    NetworkPolicy,
    NetworkPolicyList,
    ServiceCIDR,
    ServiceCIDRList,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncNetworkingV1API:
    """Asynchronous typed Networking v1 API operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def list_ingress_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> IngressList:
        """List Ingresses across all Namespaces."""
        return await self._client.request(
            "GET",
            "/apis/networking.k8s.io/v1/ingresses",
            response_model=IngressList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def list_network_policy_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> NetworkPolicyList:
        """List NetworkPolicies across all Namespaces."""
        return await self._client.request(
            "GET",
            "/apis/networking.k8s.io/v1/networkpolicies",
            response_model=NetworkPolicyList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def replace_namespaced_ingress(
        self,
        name: str,
        namespace: str,
        body: Ingress,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Ingress:
        """Replace a namespaced Ingress."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}"
            ),
            response_model=Ingress,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def read_namespaced_ingress_status(self, name: str, namespace: str) -> Ingress:
        """Read an Ingress through its status subresource."""
        return await self._client.request(
            "GET",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}/status"
            ),
            response_model=Ingress,
        )

    async def replace_namespaced_ingress_status(
        self,
        name: str,
        namespace: str,
        body: Ingress,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Ingress:
        """Replace an Ingress's status subresource."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}/status"
            ),
            response_model=Ingress,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_namespaced_ingress_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Ingress:
        """Patch an Ingress's status subresource."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}/status"
            ),
            response_model=Ingress,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def replace_ingress_class(
        self,
        name: str,
        body: IngressClass,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> IngressClass:
        """Replace a cluster-scoped IngressClass."""
        return await self._client.request(
            "PUT",
            f"/apis/networking.k8s.io/v1/ingressclasses/{resource_name(name)}",
            response_model=IngressClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def replace_namespaced_network_policy(
        self,
        name: str,
        namespace: str,
        body: NetworkPolicy,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> NetworkPolicy:
        """Replace a namespaced NetworkPolicy."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/networkpolicies/{resource_name(name)}"
            ),
            response_model=NetworkPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def create_ip_address(self, body: IPAddress) -> IPAddress:
        """Create a cluster-scoped IPAddress allocation."""
        return await self._client.request(
            "POST",
            "/apis/networking.k8s.io/v1/ipaddresses",
            response_model=IPAddress,
            body=body,
        )

    async def read_ip_address(self, name: str) -> IPAddress:
        """Read an IPAddress by its canonical IP name."""
        return await self._client.request(
            "GET",
            f"/apis/networking.k8s.io/v1/ipaddresses/{resource_name(name)}",
            response_model=IPAddress,
        )

    async def replace_ip_address(
        self,
        name: str,
        body: IPAddress,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> IPAddress:
        """Replace a cluster-scoped IPAddress allocation."""
        return await self._client.request(
            "PUT",
            f"/apis/networking.k8s.io/v1/ipaddresses/{resource_name(name)}",
            response_model=IPAddress,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_ip_address(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> IPAddress:
        """Patch a cluster-scoped IPAddress allocation."""
        return await self._client.request(
            "PATCH",
            f"/apis/networking.k8s.io/v1/ipaddresses/{resource_name(name)}",
            response_model=IPAddress,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_ip_address(
        self,
        name: str,
        body: IPAddress,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> IPAddress:
        """Server-side apply a cluster-scoped IPAddress allocation."""
        return await self._client.request(
            "PATCH",
            f"/apis/networking.k8s.io/v1/ipaddresses/{resource_name(name)}",
            response_model=IPAddress,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_ip_address(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> IPAddressList:
        """List cluster-scoped IPAddress allocations."""
        return await self._client.request(
            "GET",
            "/apis/networking.k8s.io/v1/ipaddresses",
            response_model=IPAddressList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_ip_address(self, name: str) -> Status:
        """Delete a cluster-scoped IPAddress allocation."""
        return await self._client.request(
            "DELETE",
            f"/apis/networking.k8s.io/v1/ipaddresses/{resource_name(name)}",
            response_model=Status,
        )

    async def delete_collection_ip_address(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> IPAddressList:
        """Delete selected cluster-scoped IPAddress allocations."""
        return await self._client.request(
            "DELETE",
            "/apis/networking.k8s.io/v1/ipaddresses",
            response_model=IPAddressList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_service_cidr(self, body: ServiceCIDR) -> ServiceCIDR:
        """Create a cluster-scoped ServiceCIDR."""
        return await self._client.request(
            "POST",
            "/apis/networking.k8s.io/v1/servicecidrs",
            response_model=ServiceCIDR,
            body=body,
        )

    async def read_service_cidr(self, name: str) -> ServiceCIDR:
        """Read a ServiceCIDR by name."""
        return await self._client.request(
            "GET",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}",
            response_model=ServiceCIDR,
        )

    async def replace_service_cidr(
        self,
        name: str,
        body: ServiceCIDR,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceCIDR:
        """Replace a cluster-scoped ServiceCIDR."""
        return await self._client.request(
            "PUT",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}",
            response_model=ServiceCIDR,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_service_cidr(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceCIDR:
        """Patch a cluster-scoped ServiceCIDR."""
        return await self._client.request(
            "PATCH",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}",
            response_model=ServiceCIDR,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_service_cidr(
        self,
        name: str,
        body: ServiceCIDR,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceCIDR:
        """Server-side apply a cluster-scoped ServiceCIDR."""
        return await self._client.request(
            "PATCH",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}",
            response_model=ServiceCIDR,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_service_cidr(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceCIDRList:
        """List cluster-scoped ServiceCIDRs."""
        return await self._client.request(
            "GET",
            "/apis/networking.k8s.io/v1/servicecidrs",
            response_model=ServiceCIDRList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_service_cidr(self, name: str) -> DeleteResult[ServiceCIDR]:
        """Delete a cluster-scoped ServiceCIDR."""
        return await self._client.request(
            "DELETE",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}",
            response_model=DeleteResult[ServiceCIDR],
        )

    async def delete_collection_service_cidr(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> ServiceCIDRList:
        """Delete selected cluster-scoped ServiceCIDRs."""
        return await self._client.request(
            "DELETE",
            "/apis/networking.k8s.io/v1/servicecidrs",
            response_model=ServiceCIDRList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def read_service_cidr_status(self, name: str) -> ServiceCIDR:
        """Read a ServiceCIDR through its status subresource."""
        return await self._client.request(
            "GET",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}/status",
            response_model=ServiceCIDR,
        )

    async def replace_service_cidr_status(
        self,
        name: str,
        body: ServiceCIDR,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceCIDR:
        """Replace a ServiceCIDR's status subresource."""
        return await self._client.request(
            "PUT",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}/status",
            response_model=ServiceCIDR,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_service_cidr_status(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> ServiceCIDR:
        """Patch a ServiceCIDR's status subresource."""
        return await self._client.request(
            "PATCH",
            f"/apis/networking.k8s.io/v1/servicecidrs/{resource_name(name)}/status",
            response_model=ServiceCIDR,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def create_namespaced_ingress(self, namespace: str, body: Ingress) -> Ingress:
        """Create an Ingress in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}/ingresses",
            response_model=Ingress,
            body=body,
        )

    async def read_namespaced_ingress(self, name: str, namespace: str) -> Ingress:
        """Read a namespaced Ingress by name."""
        return await self._client.request(
            "GET",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}"
            ),
            response_model=Ingress,
        )

    async def patch_namespaced_ingress(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Ingress:
        """Patch a namespaced Ingress."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}"
            ),
            response_model=Ingress,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_ingress(
        self,
        name: str,
        namespace: str,
        body: Ingress,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Ingress:
        """Server-side apply a namespaced Ingress."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}"
            ),
            response_model=Ingress,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_ingress(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> IngressList:
        """List Ingresses in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}/ingresses",
            response_model=IngressList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_ingress(self, name: str, namespace: str) -> Status:
        """Delete a namespaced Ingress."""
        return await self._client.request(
            "DELETE",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/ingresses/{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_namespaced_ingress(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> IngressList:
        """Delete selected Ingresses in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}/ingresses",
            response_model=IngressList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_ingress_class(self, body: IngressClass) -> IngressClass:
        """Create a cluster-scoped IngressClass."""
        return await self._client.request(
            "POST",
            "/apis/networking.k8s.io/v1/ingressclasses",
            response_model=IngressClass,
            body=body,
        )

    async def read_ingress_class(self, name: str) -> IngressClass:
        """Read an IngressClass by name."""
        return await self._client.request(
            "GET",
            f"/apis/networking.k8s.io/v1/ingressclasses/{resource_name(name)}",
            response_model=IngressClass,
        )

    async def patch_ingress_class(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> IngressClass:
        """Patch a cluster-scoped IngressClass."""
        return await self._client.request(
            "PATCH",
            f"/apis/networking.k8s.io/v1/ingressclasses/{resource_name(name)}",
            response_model=IngressClass,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_ingress_class(
        self,
        name: str,
        body: IngressClass,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> IngressClass:
        """Server-side apply a cluster-scoped IngressClass."""
        return await self._client.request(
            "PATCH",
            f"/apis/networking.k8s.io/v1/ingressclasses/{resource_name(name)}",
            response_model=IngressClass,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_ingress_class(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> IngressClassList:
        """List cluster-scoped IngressClasses."""
        return await self._client.request(
            "GET",
            "/apis/networking.k8s.io/v1/ingressclasses",
            response_model=IngressClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_ingress_class(self, name: str) -> Status:
        """Delete an IngressClass."""
        return await self._client.request(
            "DELETE",
            f"/apis/networking.k8s.io/v1/ingressclasses/{resource_name(name)}",
            response_model=Status,
        )

    async def delete_collection_ingress_class(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> IngressClassList:
        """Delete selected cluster-scoped IngressClasses."""
        return await self._client.request(
            "DELETE",
            "/apis/networking.k8s.io/v1/ingressclasses",
            response_model=IngressClassList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_namespaced_network_policy(
        self, namespace: str, body: NetworkPolicy
    ) -> NetworkPolicy:
        """Create a NetworkPolicy in a Namespace."""
        return await self._client.request(
            "POST",
            (f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}/networkpolicies"),
            response_model=NetworkPolicy,
            body=body,
        )

    async def read_namespaced_network_policy(self, name: str, namespace: str) -> NetworkPolicy:
        """Read a namespaced NetworkPolicy by name."""
        return await self._client.request(
            "GET",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/networkpolicies/{resource_name(name)}"
            ),
            response_model=NetworkPolicy,
        )

    async def patch_namespaced_network_policy(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> NetworkPolicy:
        """Patch a namespaced NetworkPolicy."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/networkpolicies/{resource_name(name)}"
            ),
            response_model=NetworkPolicy,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_network_policy(
        self,
        name: str,
        namespace: str,
        body: NetworkPolicy,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> NetworkPolicy:
        """Server-side apply a namespaced NetworkPolicy."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/networkpolicies/{resource_name(name)}"
            ),
            response_model=NetworkPolicy,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_network_policy(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> NetworkPolicyList:
        """List NetworkPolicies in a Namespace."""
        return await self._client.request(
            "GET",
            (f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}/networkpolicies"),
            response_model=NetworkPolicyList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_network_policy(self, name: str, namespace: str) -> Status:
        """Delete a namespaced NetworkPolicy."""
        return await self._client.request(
            "DELETE",
            (
                f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}"
                f"/networkpolicies/{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_namespaced_network_policy(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> NetworkPolicyList:
        """Delete selected NetworkPolicies in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/networking.k8s.io/v1/namespaces/{resource_name(namespace)}/networkpolicies",
            response_model=NetworkPolicyList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
