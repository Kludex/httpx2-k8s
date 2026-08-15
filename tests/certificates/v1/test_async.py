from __future__ import annotations

import base64
from datetime import UTC, datetime

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    CertificateSigningRequest,
    CertificateSigningRequestCondition,
    CertificateSigningRequestSpec,
    CertificateSigningRequestStatus,
    DeleteOptions,
    MergePatch,
    ObjectMeta,
)
from tests.certificates._fake import FakeCertificatesAPI


@pytest.mark.anyio
async def test_async_certificate_signing_request_lifecycle_through_httpx2() -> None:
    api_server = FakeCertificatesAPI()
    now = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.certificates_v1 is client.certificates_v1
        request = await client.certificates_v1.create_certificate_signing_request(
            CertificateSigningRequest(
                metadata=ObjectMeta(name="client request", labels={"owner": "tests"}),
                spec=CertificateSigningRequestSpec(
                    request=b"pem certificate request",
                    signer_name="example.com/async",
                ),
            )
        )
        assert (
            await client.certificates_v1.read_certificate_signing_request("client request")
            == request
        )
        request = await client.certificates_v1.apply_certificate_signing_request(
            "client request", request, field_manager="async-tests", force=True
        )
        request = await client.certificates_v1.patch_certificate_signing_request(
            "client request",
            MergePatch(document={"metadata": {"annotations": {"async": "true"}}}),
        )
        request = await client.certificates_v1.replace_certificate_signing_request(
            "client request", request
        )
        request.status = CertificateSigningRequestStatus(
            conditions=[
                CertificateSigningRequestCondition(
                    type="Approved",
                    status="True",
                    last_transition_time=now,
                    reason="AsyncApproved",
                )
            ]
        )
        request = await client.certificates_v1.patch_certificate_signing_request_approval(
            "client request",
            MergePatch(
                document={
                    "status": {
                        "conditions": [
                            {
                                "type": "Approved",
                                "status": "True",
                                "lastTransitionTime": "2026-08-14T12:00:00Z",
                                "reason": "AsyncApproved",
                            }
                        ]
                    }
                }
            ),
        )
        request = await client.certificates_v1.replace_certificate_signing_request_approval(
            "client request", request
        )
        assert (
            await client.certificates_v1.read_certificate_signing_request_approval("client request")
            == request
        )
        request = await client.certificates_v1.patch_certificate_signing_request_status(
            "client request",
            MergePatch(
                document={"status": {"certificate": base64.b64encode(b"async cert").decode()}}
            ),
        )
        request = await client.certificates_v1.replace_certificate_signing_request_status(
            "client request", request
        )
        assert (
            await client.certificates_v1.read_certificate_signing_request_status("client request")
            == request
        )
        assert (
            await client.certificates_v1.list_certificate_signing_request(
                label_selector="owner=tests",
                field_selector="metadata.name=client request",
                limit=1,
                continue_token="next",
            )
        ).items == [request]
        assert (
            await client.certificates_v1.delete_collection_certificate_signing_request(
                DeleteOptions(dry_run=["All"], propagation_policy="Background"),
                label_selector="owner=tests",
                field_selector="metadata.name=client request",
                limit=1,
                continue_token="delete-next",
            )
        ).items == [request]
        assert (
            await client.certificates_v1.delete_certificate_signing_request("client request")
        ).status == "Success"
