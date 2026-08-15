from __future__ import annotations

import base64
from datetime import UTC, datetime

import httpx2
import pytest
from pydantic import ValidationError

from httpx2_k8s import (
    CertificateSigningRequest,
    CertificateSigningRequestCondition,
    CertificateSigningRequestSpec,
    CertificateSigningRequestStatus,
    DeleteOptions,
    KubeClient,
    MergePatch,
    ObjectMeta,
)
from tests.certificates._fake import FakeCertificatesAPI


def test_certificate_signing_request_lifecycle_through_httpx2() -> None:
    api_server = FakeCertificatesAPI()
    now = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.certificates_v1 is client.certificates_v1
        created = client.certificates_v1.create_certificate_signing_request(
            CertificateSigningRequest(
                metadata=ObjectMeta(name="client request", labels={"owner": "tests"}),
                spec=CertificateSigningRequestSpec(
                    request=b"pem certificate request",
                    signer_name="example.com/client",
                    expiration_seconds=3600,
                    usages=["digital signature", "key encipherment", "client auth"],
                ),
            )
        )
        assert created.spec.request == b"pem certificate request"
        assert created.spec.username == "alice"
        assert created.spec.groups == ["system:authenticated"]
        assert created.spec.uid == "requester-uid"
        assert created.spec.extra == {"authentication.kubernetes.io/pod-name": ["issuer"]}
        assert client.certificates_v1.read_certificate_signing_request("client request") == created
        created = client.certificates_v1.apply_certificate_signing_request(
            "client request",
            created,
            field_manager="certificate-tests",
            force=True,
        )
        created = client.certificates_v1.patch_certificate_signing_request(
            "client request",
            MergePatch(document={"metadata": {"annotations": {"patched": "true"}}}),
            field_manager="certificate-tests",
            dry_run="All",
        )
        assert created.metadata.annotations == {"patched": "true"}

        created.metadata.annotations["reviewed"] = "true"
        replaced = client.certificates_v1.replace_certificate_signing_request(
            "client request", created
        )
        assert replaced.metadata.annotations == {"patched": "true", "reviewed": "true"}

        replaced.status = CertificateSigningRequestStatus(
            conditions=[
                CertificateSigningRequestCondition(
                    type="Approved",
                    status="True",
                    last_transition_time=now,
                    last_update_time=now,
                    message="approved by tests",
                    reason="IntegrationApproved",
                )
            ]
        )
        approval_preview = client.certificates_v1.patch_certificate_signing_request_approval(
            "client request",
            MergePatch(
                document={
                    "status": {
                        "conditions": [
                            {
                                "type": "Approved",
                                "status": "True",
                                "lastTransitionTime": "2026-08-14T12:00:00Z",
                                "lastUpdateTime": "2026-08-14T12:00:00Z",
                                "message": "approved by tests",
                                "reason": "IntegrationApproved",
                            }
                        ]
                    }
                }
            ),
            field_manager="certificate-tests",
            dry_run="All",
        )
        assert approval_preview.status is not None
        assert approval_preview.status.conditions[0].reason == "IntegrationApproved"
        approved = client.certificates_v1.replace_certificate_signing_request_approval(
            "client request", approval_preview
        )
        assert approved.status is not None
        assert approved.status.conditions[0].reason == "IntegrationApproved"
        assert (
            client.certificates_v1.read_certificate_signing_request_approval("client request")
            == approved
        )

        approved.status.certificate = b"issued certificate"
        status_preview = client.certificates_v1.patch_certificate_signing_request_status(
            "client request",
            MergePatch(
                document={
                    "status": {"certificate": base64.b64encode(b"issued certificate").decode()}
                }
            ),
            dry_run="All",
        )
        assert status_preview.status is not None
        assert status_preview.status.certificate == b"issued certificate"
        issued = client.certificates_v1.replace_certificate_signing_request_status(
            "client request", status_preview
        )
        assert issued.status is not None
        assert issued.status.certificate == b"issued certificate"
        assert (
            client.certificates_v1.read_certificate_signing_request_status("client request")
            == issued
        )

        requests = client.certificates_v1.list_certificate_signing_request(
            label_selector="owner=tests",
            field_selector="metadata.name=client request",
            limit=1,
            continue_token="next",
        )
        assert requests.items == [issued]
        assert requests.metadata.remaining_item_count == 0
        deleted_requests = client.certificates_v1.delete_collection_certificate_signing_request(
            DeleteOptions(dry_run=["All"], propagation_policy="Background"),
            label_selector="owner=tests",
            field_selector="metadata.name=client request",
            limit=1,
            continue_token="delete-next",
        )
        assert [item.metadata.name for item in deleted_requests.items] == ["client request"]
        assert client.certificates_v1.delete_certificate_signing_request(
            "client request"
        ).status == ("Success")

    assert [subresource for subresource, _, _ in api_server.patch_calls] == [
        None,
        None,
        "approval",
        "status",
    ]
    assert [content_type for _, content_type, _ in api_server.patch_calls] == [
        "application/apply-patch+yaml",
        "application/merge-patch+json",
        "application/merge-patch+json",
        "application/merge-patch+json",
    ]
    assert api_server.patch_calls[0][2] == {
        "fieldManager": "certificate-tests",
        "force": "true",
    }
    assert api_server.patch_calls[2][2] == {
        "fieldManager": "certificate-tests",
        "dryRun": "All",
    }
    assert api_server.delete_collection_call == (
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.name=client request",
            "limit": "1",
            "continue": "delete-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )


def test_certificate_signing_request_rejects_invalid_base64_responses() -> None:
    with pytest.raises(ValidationError, match="Value is not valid base64"):
        CertificateSigningRequest.model_validate(
            {
                "metadata": {"name": "invalid"},
                "spec": {"request": "not base64!", "signerName": "example.com/client"},
            }
        )
