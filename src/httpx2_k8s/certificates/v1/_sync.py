from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    CertificateSigningRequest,
    CertificateSigningRequestList,
    DeleteOptions,
    JsonPatch,
    MergePatch,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import SyncKubeClientProtocol


class CertificatesV1API:
    """Typed Certificates v1 CertificateSigningRequest operations."""

    _collection_path = "/apis/certificates.k8s.io/v1/certificatesigningrequests"

    def __init__(self, client: SyncKubeClientProtocol) -> None:
        self._client = client

    def create_certificate_signing_request(
        self, body: CertificateSigningRequest
    ) -> CertificateSigningRequest:
        return self._client.request(
            "POST", self._collection_path, response_model=CertificateSigningRequest, body=body
        )

    def read_certificate_signing_request(self, name: str) -> CertificateSigningRequest:
        return self._client.request(
            "GET",
            f"{self._collection_path}/{resource_name(name)}",
            response_model=CertificateSigningRequest,
        )

    def patch_certificate_signing_request(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CertificateSigningRequest:
        return self._client.request(
            "PATCH",
            f"{self._collection_path}/{resource_name(name)}",
            response_model=CertificateSigningRequest,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def apply_certificate_signing_request(
        self,
        name: str,
        body: CertificateSigningRequest,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> CertificateSigningRequest:
        return self._client.request(
            "PATCH",
            f"{self._collection_path}/{resource_name(name)}",
            response_model=CertificateSigningRequest,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    def replace_certificate_signing_request(
        self, name: str, body: CertificateSigningRequest
    ) -> CertificateSigningRequest:
        return self._client.request(
            "PUT",
            f"{self._collection_path}/{resource_name(name)}",
            response_model=CertificateSigningRequest,
            body=body,
        )

    def replace_certificate_signing_request_approval(
        self, name: str, body: CertificateSigningRequest
    ) -> CertificateSigningRequest:
        return self._client.request(
            "PUT",
            f"{self._collection_path}/{resource_name(name)}/approval",
            response_model=CertificateSigningRequest,
            body=body,
        )

    def read_certificate_signing_request_approval(self, name: str) -> CertificateSigningRequest:
        """Read the approval subresource of a CertificateSigningRequest."""
        return self._client.request(
            "GET",
            f"{self._collection_path}/{resource_name(name)}/approval",
            response_model=CertificateSigningRequest,
        )

    def patch_certificate_signing_request_approval(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CertificateSigningRequest:
        return self._client.request(
            "PATCH",
            f"{self._collection_path}/{resource_name(name)}/approval",
            response_model=CertificateSigningRequest,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def replace_certificate_signing_request_status(
        self, name: str, body: CertificateSigningRequest
    ) -> CertificateSigningRequest:
        return self._client.request(
            "PUT",
            f"{self._collection_path}/{resource_name(name)}/status",
            response_model=CertificateSigningRequest,
            body=body,
        )

    def read_certificate_signing_request_status(self, name: str) -> CertificateSigningRequest:
        """Read the status subresource of a CertificateSigningRequest."""
        return self._client.request(
            "GET",
            f"{self._collection_path}/{resource_name(name)}/status",
            response_model=CertificateSigningRequest,
        )

    def patch_certificate_signing_request_status(
        self,
        name: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CertificateSigningRequest:
        return self._client.request(
            "PATCH",
            f"{self._collection_path}/{resource_name(name)}/status",
            response_model=CertificateSigningRequest,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    def list_certificate_signing_request(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CertificateSigningRequestList:
        return self._client.request(
            "GET",
            self._collection_path,
            response_model=CertificateSigningRequestList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    def delete_certificate_signing_request(self, name: str) -> Status:
        return self._client.request(
            "DELETE",
            f"{self._collection_path}/{resource_name(name)}",
            response_model=Status,
        )

    def delete_collection_certificate_signing_request(
        self,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CertificateSigningRequestList:
        return self._client.request(
            "DELETE",
            self._collection_path,
            response_model=CertificateSigningRequestList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
