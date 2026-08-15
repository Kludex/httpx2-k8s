from __future__ import annotations

from httpx2_k8s._api import list_params, resource_name
from httpx2_k8s._models import (
    CronJob,
    CronJobList,
    DeleteOptions,
    Job,
    JobList,
    JsonPatch,
    MergePatch,
    Status,
)
from httpx2_k8s._patch import DryRun, patch_content_type, patch_params
from httpx2_k8s._protocols import AsyncKubeClientProtocol


class AsyncBatchV1API:
    """Asynchronous typed Batch v1 Job and CronJob operations."""

    def __init__(self, client: AsyncKubeClientProtocol) -> None:
        self._client = client

    async def list_job_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> JobList:
        """List Jobs across all Namespaces."""
        return await self._client.request(
            "GET",
            "/apis/batch/v1/jobs",
            response_model=JobList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def list_cron_job_for_all_namespaces(
        self,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CronJobList:
        """List CronJobs across all Namespaces."""
        return await self._client.request(
            "GET",
            "/apis/batch/v1/cronjobs",
            response_model=CronJobList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def replace_namespaced_job(
        self,
        name: str,
        namespace: str,
        body: Job,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Job:
        """Replace a namespaced Job."""
        return await self._client.request(
            "PUT",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs/{resource_name(name)}",
            response_model=Job,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def read_namespaced_job_status(self, name: str, namespace: str) -> Job:
        """Read a Job through its status subresource."""
        return await self._client.request(
            "GET",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/jobs/{resource_name(name)}/status"
            ),
            response_model=Job,
        )

    async def replace_namespaced_job_status(
        self,
        name: str,
        namespace: str,
        body: Job,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Job:
        """Replace a Job's status subresource."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/jobs/{resource_name(name)}/status"
            ),
            response_model=Job,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_namespaced_job_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Job:
        """Patch a Job's status subresource."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/jobs/{resource_name(name)}/status"
            ),
            response_model=Job,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def replace_namespaced_cron_job(
        self,
        name: str,
        namespace: str,
        body: CronJob,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CronJob:
        """Replace a namespaced CronJob."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}"
            ),
            response_model=CronJob,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def read_namespaced_cron_job_status(self, name: str, namespace: str) -> CronJob:
        """Read a CronJob through its status subresource."""
        return await self._client.request(
            "GET",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}/status"
            ),
            response_model=CronJob,
        )

    async def replace_namespaced_cron_job_status(
        self,
        name: str,
        namespace: str,
        body: CronJob,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CronJob:
        """Replace a CronJob's status subresource."""
        return await self._client.request(
            "PUT",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}/status"
            ),
            response_model=CronJob,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
        )

    async def patch_namespaced_cron_job_status(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CronJob:
        """Patch a CronJob's status subresource."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}/status"
            ),
            response_model=CronJob,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def create_namespaced_job(self, namespace: str, body: Job) -> Job:
        """Create a Job in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs",
            response_model=Job,
            body=body,
        )

    async def read_namespaced_job(self, name: str, namespace: str) -> Job:
        """Read a namespaced Job by name."""
        return await self._client.request(
            "GET",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs/{resource_name(name)}",
            response_model=Job,
        )

    async def patch_namespaced_job(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> Job:
        """Patch a namespaced Job."""
        return await self._client.request(
            "PATCH",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs/{resource_name(name)}",
            response_model=Job,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_job(
        self,
        name: str,
        namespace: str,
        body: Job,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> Job:
        """Server-side apply a namespaced Job."""
        return await self._client.request(
            "PATCH",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs/{resource_name(name)}",
            response_model=Job,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_job(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> JobList:
        """List Jobs in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs",
            response_model=JobList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_job(self, name: str, namespace: str) -> Job:
        """Delete a namespaced Job."""
        return await self._client.request(
            "DELETE",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs/{resource_name(name)}",
            response_model=Job,
        )

    async def delete_collection_namespaced_job(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> JobList:
        """Delete selected Jobs in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/jobs",
            response_model=JobList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )

    async def create_namespaced_cron_job(self, namespace: str, body: CronJob) -> CronJob:
        """Create a CronJob in a Namespace."""
        return await self._client.request(
            "POST",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/cronjobs",
            response_model=CronJob,
            body=body,
        )

    async def read_namespaced_cron_job(self, name: str, namespace: str) -> CronJob:
        """Read a namespaced CronJob by name."""
        return await self._client.request(
            "GET",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}"
            ),
            response_model=CronJob,
        )

    async def patch_namespaced_cron_job(
        self,
        name: str,
        namespace: str,
        body: JsonPatch | MergePatch,
        *,
        field_manager: str | None = None,
        dry_run: DryRun | None = None,
    ) -> CronJob:
        """Patch a namespaced CronJob."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}"
            ),
            response_model=CronJob,
            params=patch_params(field_manager=field_manager, dry_run=dry_run),
            body=body,
            content_type=patch_content_type(body),
        )

    async def apply_namespaced_cron_job(
        self,
        name: str,
        namespace: str,
        body: CronJob,
        *,
        field_manager: str,
        force: bool | None = None,
        dry_run: DryRun | None = None,
    ) -> CronJob:
        """Server-side apply a namespaced CronJob."""
        return await self._client.request(
            "PATCH",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}"
            ),
            response_model=CronJob,
            params=patch_params(field_manager=field_manager, force=force, dry_run=dry_run),
            body=body,
            content_type="application/apply-patch+yaml",
        )

    async def list_namespaced_cron_job(
        self,
        namespace: str,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CronJobList:
        """List CronJobs in a Namespace."""
        return await self._client.request(
            "GET",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/cronjobs",
            response_model=CronJobList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
        )

    async def delete_namespaced_cron_job(self, name: str, namespace: str) -> Status:
        """Delete a namespaced CronJob."""
        return await self._client.request(
            "DELETE",
            (
                f"/apis/batch/v1/namespaces/{resource_name(namespace)}"
                f"/cronjobs/{resource_name(name)}"
            ),
            response_model=Status,
        )

    async def delete_collection_namespaced_cron_job(
        self,
        namespace: str,
        body: DeleteOptions | None = None,
        *,
        label_selector: str | None = None,
        field_selector: str | None = None,
        limit: int | None = None,
        continue_token: str | None = None,
    ) -> CronJobList:
        """Delete selected CronJobs in a Namespace."""
        return await self._client.request(
            "DELETE",
            f"/apis/batch/v1/namespaces/{resource_name(namespace)}/cronjobs",
            response_model=CronJobList,
            params=list_params(
                label_selector=label_selector,
                field_selector=field_selector,
                limit=limit,
                continue_token=continue_token,
            ),
            body=body,
        )
