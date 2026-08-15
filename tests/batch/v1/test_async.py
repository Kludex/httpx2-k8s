from __future__ import annotations

import httpx2
import pytest

from httpx2_k8s import (
    AsyncKubeClient,
    CronJob,
    CronJobSpec,
    DeleteOptions,
    Job,
    JobSpec,
    JobTemplateSpec,
    MergePatch,
    ObjectMeta,
)
from tests.batch._fake import FakeBatchAPI, job_template


@pytest.mark.anyio
async def test_async_batch_v1_lifecycles_through_httpx2() -> None:
    api_server = FakeBatchAPI()

    async with AsyncKubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.batch_v1 is client.batch_v1

        job = await client.batch_v1.create_namespaced_job(
            "async team",
            Job(
                metadata=ObjectMeta(name="async job", labels={"owner": "async-tests"}),
                spec=JobSpec(
                    template=job_template("async-job"),
                    completion_mode="Indexed",
                    completions=3,
                    parallelism=2,
                    suspend=True,
                ),
            ),
        )
        assert job.status is not None
        assert job.status.completed_indexes == "0-1"
        assert (await client.batch_v1.read_namespaced_job("async job", "async team")) == job
        job = await client.batch_v1.replace_namespaced_job(
            "async job",
            "async team",
            job,
            field_manager="async-batch-tests",
            dry_run="All",
        )
        job = await client.batch_v1.apply_namespaced_job(
            "async job",
            "async team",
            job,
            field_manager="async-batch-tests",
            force=True,
        )
        job = await client.batch_v1.patch_namespaced_job(
            "async job",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-job"}}}),
            field_manager="async-batch-tests",
            dry_run="All",
        )
        assert job.metadata.annotations == {"patched": "async-job"}
        assert (await client.batch_v1.read_namespaced_job_status("async job", "async team")) == job
        job = await client.batch_v1.replace_namespaced_job_status("async job", "async team", job)
        job = await client.batch_v1.patch_namespaced_job_status(
            "async job",
            "async team",
            MergePatch(document={"status": {}}),
            field_manager="async-batch-tests",
            dry_run="All",
        )
        assert (
            await client.batch_v1.list_namespaced_job(
                "async team",
                label_selector="owner=async-tests",
                field_selector="metadata.name=async job",
                limit=1,
                continue_token="async-next",
            )
        ).items == [job]
        assert (await client.batch_v1.list_job_for_all_namespaces()).items == [job]
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.delete_collection_namespaced_job(
                    "async team",
                    DeleteOptions(dry_run=["All"]),
                    label_selector="owner=async-tests",
                )
            ).items
        ] == ["async job"]
        assert (
            await client.batch_v1.delete_namespaced_job("async job", "async team")
        ).metadata.name == "async job"

        cron_job = await client.batch_v1.create_namespaced_cron_job(
            "async team",
            CronJob(
                metadata=ObjectMeta(name="async nightly", labels={"owner": "async-tests"}),
                spec=CronJobSpec(
                    schedule="0 0 * * *",
                    job_template=JobTemplateSpec(
                        metadata=ObjectMeta(labels={"job": "async-nightly"}),
                        spec=JobSpec(template=job_template("async-nightly")),
                    ),
                    suspend=True,
                    time_zone="Etc/UTC",
                ),
            ),
        )
        assert cron_job.status is not None
        assert cron_job.status.last_successful_time is not None
        assert (
            await client.batch_v1.read_namespaced_cron_job("async nightly", "async team")
        ) == cron_job
        cron_job = await client.batch_v1.replace_namespaced_cron_job(
            "async nightly", "async team", cron_job
        )
        cron_job = await client.batch_v1.apply_namespaced_cron_job(
            "async nightly",
            "async team",
            cron_job,
            field_manager="async-batch-tests",
            force=False,
            dry_run="All",
        )
        cron_job = await client.batch_v1.patch_namespaced_cron_job(
            "async nightly",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "async-cron"}}}),
        )
        assert cron_job.metadata.annotations == {"patched": "async-cron"}
        assert (
            await client.batch_v1.read_namespaced_cron_job_status("async nightly", "async team")
        ) == cron_job
        cron_job = await client.batch_v1.replace_namespaced_cron_job_status(
            "async nightly", "async team", cron_job
        )
        cron_job = await client.batch_v1.patch_namespaced_cron_job_status(
            "async nightly", "async team", MergePatch(document={"status": {}})
        )
        assert (await client.batch_v1.list_namespaced_cron_job("async team")).items == [cron_job]
        assert (await client.batch_v1.list_cron_job_for_all_namespaces()).items == [cron_job]
        assert [
            item.metadata.name
            for item in (
                await client.batch_v1.delete_collection_namespaced_cron_job(
                    "async team",
                    DeleteOptions(dry_run=["All"]),
                    label_selector="owner=async-tests",
                )
            ).items
        ] == ["async nightly"]
        assert (
            await client.batch_v1.delete_namespaced_cron_job("async nightly", "async team")
        ).status == "Success"

    assert [resource for resource, _, _ in api_server.patch_calls] == [
        "jobs",
        "jobs",
        "jobs",
        "cronjobs",
        "cronjobs",
        "cronjobs",
    ]
    assert [resource for resource, _, _ in api_server.delete_collection_calls] == [
        "jobs",
        "cronjobs",
    ]
