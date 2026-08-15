from __future__ import annotations

import httpx2

from httpx2_k8s import (
    CronJob,
    CronJobSpec,
    DeleteOptions,
    Job,
    JobSpec,
    JobTemplateSpec,
    KubeClient,
    LabelSelector,
    MergePatch,
    ObjectMeta,
)
from tests.batch._fake import FakeBatchAPI, job_template


def test_batch_v1_lifecycles_through_httpx2() -> None:
    api_server = FakeBatchAPI()

    with KubeClient(
        "https://kubernetes.invalid", transport=httpx2.MockTransport(api_server)
    ) as client:
        assert client.batch_v1 is client.batch_v1

        job = client.batch_v1.create_namespaced_job(
            "team one",
            Job(
                metadata=ObjectMeta(name="indexed job", labels={"owner": "tests"}),
                spec=JobSpec(
                    template=job_template("indexed"),
                    active_deadline_seconds=300,
                    backoff_limit=6,
                    backoff_limit_per_index=2,
                    completion_mode="Indexed",
                    completions=3,
                    manual_selector=True,
                    max_failed_indexes=1,
                    parallelism=2,
                    pod_replacement_policy="Failed",
                    selector=LabelSelector(match_labels={"app": "indexed"}),
                    suspend=True,
                    ttl_seconds_after_finished=60,
                ),
            ),
        )
        assert job.status is not None
        assert job.status.completed_indexes == "0-1"
        assert job.status.conditions[0].reason == "JobSuspended"
        assert client.batch_v1.read_namespaced_job("indexed job", "team one") == job
        job = client.batch_v1.replace_namespaced_job(
            "indexed job",
            "team one",
            job,
            field_manager="batch-tests",
            dry_run="All",
        )
        job = client.batch_v1.apply_namespaced_job(
            "indexed job",
            "team one",
            job,
            field_manager="batch-tests",
            force=True,
        )
        job = client.batch_v1.patch_namespaced_job(
            "indexed job",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "job"}}}),
            field_manager="batch-tests",
            dry_run="All",
        )
        assert job.metadata.annotations == {"patched": "job"}
        assert client.batch_v1.read_namespaced_job_status("indexed job", "team one") == job
        job = client.batch_v1.replace_namespaced_job_status("indexed job", "team one", job)
        job = client.batch_v1.patch_namespaced_job_status(
            "indexed job",
            "team one",
            MergePatch(document={"status": {}}),
            field_manager="batch-tests",
            dry_run="All",
        )
        jobs = client.batch_v1.list_namespaced_job(
            "team one",
            label_selector="owner=tests",
            field_selector="metadata.name=indexed job",
            limit=1,
            continue_token="next",
        )
        assert jobs.items == [job]
        assert jobs.metadata.continue_ == ""
        assert jobs.metadata.remaining_item_count == 0
        assert jobs.metadata.self_link is not None
        assert client.batch_v1.list_job_for_all_namespaces().items == [job]
        assert [
            item.metadata.name
            for item in client.batch_v1.delete_collection_namespaced_job(
                "team one",
                DeleteOptions(dry_run=["All"], propagation_policy="Background"),
                label_selector="owner=tests",
                field_selector="metadata.namespace=team one",
                limit=1,
                continue_token="job-next",
            ).items
        ] == ["indexed job"]
        assert (
            client.batch_v1.delete_namespaced_job("indexed job", "team one").metadata.name
            == "indexed job"
        )

        cron_job = client.batch_v1.create_namespaced_cron_job(
            "team one",
            CronJob(
                metadata=ObjectMeta(name="nightly job", labels={"owner": "tests"}),
                spec=CronJobSpec(
                    schedule="0 0 * * *",
                    job_template=JobTemplateSpec(
                        metadata=ObjectMeta(labels={"job": "nightly"}),
                        spec=JobSpec(
                            template=job_template("nightly"),
                            backoff_limit=3,
                            completion_mode="NonIndexed",
                        ),
                    ),
                    concurrency_policy="Forbid",
                    failed_jobs_history_limit=2,
                    starting_deadline_seconds=30,
                    successful_jobs_history_limit=1,
                    suspend=True,
                    time_zone="Etc/UTC",
                ),
            ),
        )
        assert cron_job.status is not None
        assert cron_job.status.active[0].field_path == "spec.template"
        assert cron_job.status.last_successful_time is not None
        assert client.batch_v1.read_namespaced_cron_job("nightly job", "team one") == cron_job
        cron_job = client.batch_v1.replace_namespaced_cron_job("nightly job", "team one", cron_job)
        cron_job = client.batch_v1.apply_namespaced_cron_job(
            "nightly job",
            "team one",
            cron_job,
            field_manager="batch-tests",
            force=False,
            dry_run="All",
        )
        cron_job = client.batch_v1.patch_namespaced_cron_job(
            "nightly job",
            "team one",
            MergePatch(document={"metadata": {"annotations": {"patched": "cron-job"}}}),
        )
        assert cron_job.metadata.annotations == {"patched": "cron-job"}
        assert (
            client.batch_v1.read_namespaced_cron_job_status("nightly job", "team one") == cron_job
        )
        cron_job = client.batch_v1.replace_namespaced_cron_job_status(
            "nightly job", "team one", cron_job
        )
        cron_job = client.batch_v1.patch_namespaced_cron_job_status(
            "nightly job", "team one", MergePatch(document={"status": {}})
        )
        assert client.batch_v1.list_namespaced_cron_job("team one").items == [cron_job]
        assert client.batch_v1.list_cron_job_for_all_namespaces().items == [cron_job]
        assert [
            item.metadata.name
            for item in client.batch_v1.delete_collection_namespaced_cron_job(
                "team one",
                DeleteOptions(dry_run=["All"], grace_period_seconds=0),
                label_selector="owner=tests",
            ).items
        ] == ["nightly job"]
        assert (
            client.batch_v1.delete_namespaced_cron_job("nightly job", "team one").status
            == "Success"
        )

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
    assert api_server.delete_collection_calls[0] == (
        "jobs",
        {
            "labelSelector": "owner=tests",
            "fieldSelector": "metadata.namespace=team one",
            "limit": "1",
            "continue": "job-next",
        },
        {
            "apiVersion": "v1",
            "kind": "DeleteOptions",
            "dryRun": ["All"],
            "propagationPolicy": "Background",
        },
    )
