from __future__ import annotations

import json
import ssl
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import cast

import httpx2
import pytest
from typing_extensions import override

from httpx2_k8s import (
    APIError,
    AsyncKubeClient,
    Binding,
    ClientConfig,
    Container,
    DeleteOptions,
    MergePatch,
    Namespace,
    NamespaceStatus,
    ObjectMeta,
    ObjectReference,
    Pod,
    PodSpec,
    PodStatus,
    RetryPolicy,
)

VERSION = {"major": "1", "minor": "33", "gitVersion": "v1.33.0", "platform": "linux/arm64"}


class FakeAsyncKubernetes:
    """Stateful public HTTP boundary for the initial asynchronous Core v1 surface."""

    def __init__(self) -> None:
        self.namespaces: dict[str, dict[str, object]] = {}
        self.pods: dict[tuple[str, str], dict[str, object]] = {}

    @staticmethod
    def _json(request: httpx2.Request) -> dict[str, object]:
        return cast(dict[str, object], json.loads(request.content))

    @staticmethod
    def _response(status_code: int, body: Mapping[str, object]) -> httpx2.Response:
        return httpx2.Response(status_code, json=body)

    def __call__(self, request: httpx2.Request) -> httpx2.Response:
        assert request.headers["authorization"] == "Bearer async-secret"
        path = request.url.path
        if path == "/version":
            return self._response(
                200,
                {
                    "major": "1",
                    "minor": "33",
                    "gitVersion": "v1.33.0",
                    "platform": "linux/arm64",
                },
            )
        if path == "/unsupported":
            return httpx2.Response(418, text="async teapot")

        if path == "/api/v1/namespaces":
            if request.method == "POST":
                body = self._json(request)
                metadata = cast(dict[str, object], body["metadata"])
                metadata["resourceVersion"] = "1"
                self.namespaces[cast(str, metadata["name"])] = body
                return self._response(201, body)
            assert dict(request.url.params) == {
                "continue": "next page",
                "fieldSelector": "metadata.name=async team",
                "labelSelector": "owner=tests",
                "limit": "10",
            }
            return self._response(
                200,
                {
                    "apiVersion": "v1",
                    "kind": "NamespaceList",
                    "metadata": {"resourceVersion": "2"},
                    "items": list(self.namespaces.values()),
                },
            )

        namespace_prefix = "/api/v1/namespaces/"
        if path.startswith(namespace_prefix) and "/pods" not in path:
            namespace_path = path.removeprefix(namespace_prefix)
            name = namespace_path.removesuffix("/status")
            current = self.namespaces[name]
            if request.method == "GET":
                return self._response(200, current)
            if request.method == "DELETE":
                deleted = self.namespaces.pop(name)
                deleted["status"] = {"phase": "Terminating"}
                return self._response(200, deleted)
            if (
                request.headers["content-type"] == "application/apply-patch+yaml"
                or request.method == "PUT"
            ):
                current = self._json(request)
                self.namespaces[name] = current
            else:
                metadata = cast(dict[str, object], current["metadata"])
                metadata["annotations"] = {"patched": "true"}
            return self._response(200, current)

        rest = path.removeprefix(namespace_prefix)
        namespace, pod_path = rest.split("/pods", maxsplit=1)
        if pod_path.endswith("/log"):
            assert dict(request.url.params)["container"] == "worker"
            return httpx2.Response(200, text="first\nsecond\n")
        if pod_path == "":
            if request.method == "POST":
                body = self._json(request)
                metadata = cast(dict[str, object], body["metadata"])
                name = cast(str, metadata["name"])
                metadata["namespace"] = namespace
                self.pods[(namespace, name)] = body
                return self._response(201, body)
            items = [
                body
                for (stored_namespace, _), body in self.pods.items()
                if stored_namespace == namespace
            ]
            if request.method == "DELETE":
                self.pods = {key: body for key, body in self.pods.items() if key[0] != namespace}
            return self._response(
                200,
                {"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": items},
            )

        name, _, subresource = pod_path.removeprefix("/").partition("/")
        key = (namespace, name)
        current = self.pods[key]
        if subresource == "binding":
            return self._response(
                201,
                {"apiVersion": "v1", "kind": "Status", "status": "Success", "code": 201},
            )
        if subresource == "ephemeralcontainers" and request.method == "PUT":
            assert dict(request.url.params) == {
                "dryRun": "All",
                "fieldManager": "async-ephemeral-replace",
            }
        if subresource == "ephemeralcontainers" and request.method == "PATCH":
            assert request.headers["content-type"] == "application/merge-patch+json"
            assert dict(request.url.params) == {"fieldManager": "async-ephemeral-patch"}
        if request.method == "GET":
            return self._response(200, current)
        if request.method == "DELETE":
            deleted = self.pods.pop(key)
            deleted["status"] = {"phase": "Succeeded"}
            return self._response(200, deleted)
        if (
            request.headers["content-type"] == "application/apply-patch+yaml"
            or request.method == "PUT"
        ):
            current = self._json(request)
            self.pods[key] = current
        else:
            metadata = cast(dict[str, object], current["metadata"])
            metadata["annotations"] = {"patched": "true"}
        return self._response(200, current)


@pytest.mark.anyio
async def test_async_core_v1_lifecycle_through_httpx2() -> None:
    api_server = FakeAsyncKubernetes()
    async with AsyncKubeClient(
        "https://kubernetes.invalid/",
        token="async-secret",
        verify=False,
        cert=("client.crt", "client.key"),
        timeout=5,
        transport=httpx2.MockTransport(api_server),
    ) as client:
        assert (await client.version()).git_version == "v1.33.0"
        assert client.core_v1 is client.core_v1

        namespace = await client.core_v1.create_namespace(
            Namespace(metadata=ObjectMeta(name="async team", labels={"owner": "tests"}))
        )
        assert namespace.metadata.resource_version == "1"
        assert await client.core_v1.read_namespace("async team") == namespace
        namespaces = await client.core_v1.list_namespace(
            label_selector="owner=tests",
            field_selector="metadata.name=async team",
            limit=10,
            continue_token="next page",
        )
        assert namespaces.items == [namespace]
        namespace = await client.core_v1.patch_namespace(
            "async team", MergePatch(document={"metadata": {"annotations": {"patched": "true"}}})
        )
        assert namespace.metadata.annotations == {"patched": "true"}
        namespace = await client.core_v1.apply_namespace(
            "async team", namespace, field_manager="async-tests", force=True, dry_run="All"
        )
        namespace.status = NamespaceStatus(phase="Active")
        namespace = await client.core_v1.replace_namespace_status("async team", namespace)
        namespace = await client.core_v1.patch_namespace_status(
            "async team", MergePatch(document={"status": {"phase": "Active"}}), dry_run="All"
        )
        assert namespace.status == NamespaceStatus(phase="Active")

        pod = Pod(
            metadata=ObjectMeta(name="async pod"),
            spec=PodSpec(
                containers=[Container(name="worker", image="registry.invalid/worker:1")],
                restart_policy="Never",
            ),
        )
        pod = await client.core_v1.create_namespaced_pod("async team", pod)
        assert await client.core_v1.read_namespaced_pod("async pod", "async team") == pod
        pod = await client.core_v1.patch_namespaced_pod(
            "async pod", "async team", MergePatch(document={"metadata": {"annotations": {}}})
        )
        pod = await client.core_v1.apply_namespaced_pod(
            "async pod", "async team", pod, field_manager="async-tests"
        )
        assert (
            await client.core_v1.read_namespaced_pod_log(
                "async pod",
                "async team",
                container="worker",
                previous=True,
                since_seconds=60,
                tail_lines=2,
                timestamps=True,
                limit_bytes=1024,
            )
            == "first\nsecond\n"
        )
        lines = [
            line
            async for line in client.core_v1.stream_namespaced_pod_log(
                "async pod", "async team", container="worker", timeout=2
            )
        ]
        assert lines == ["first", "second"]
        pod.status = PodStatus(phase="Running")
        pod = await client.core_v1.replace_namespaced_pod_status("async pod", "async team", pod)
        pod = await client.core_v1.patch_namespaced_pod_status(
            "async pod",
            "async team",
            MergePatch(document={"status": {"phase": "Running"}}),
            field_manager="async-tests",
        )
        assert pod.status == PodStatus(phase="Running")
        binding = Binding(
            metadata=ObjectMeta(name="async pod"),
            target=ObjectReference(kind="Node", name="worker-node"),
        )
        assert (
            await client.core_v1.create_namespaced_pod_binding("async pod", "async team", binding)
        ).status == "Success"
        pod = await client.core_v1.replace_namespaced_pod_ephemeral_containers(
            "async pod",
            "async team",
            pod,
            field_manager="async-ephemeral-replace",
            dry_run="All",
        )
        assert (
            await client.core_v1.read_namespaced_pod_ephemeral_containers("async pod", "async team")
            == pod
        )
        pod = await client.core_v1.patch_namespaced_pod_ephemeral_containers(
            "async pod",
            "async team",
            MergePatch(document={"metadata": {"annotations": {"patched": "true"}}}),
            field_manager="async-ephemeral-patch",
        )
        assert pod.metadata.annotations == {"patched": "true"}
        assert (await client.core_v1.list_namespaced_pod("async team")).items == [pod]

        second = Pod(
            metadata=ObjectMeta(name="second pod"),
            spec=PodSpec(containers=[Container(name="worker", image="busybox")]),
        )
        await client.core_v1.create_namespaced_pod("async team", second)
        deleted = await client.core_v1.delete_namespaced_pod("second pod", "async team")
        assert deleted.status == PodStatus(phase="Succeeded")
        deleted_pods = await client.core_v1.delete_collection_namespaced_pod(
            "async team",
            DeleteOptions(propagation_policy="Foreground"),
            label_selector="owner=tests",
        )
        assert [item.metadata.name for item in deleted_pods.items] == ["async pod"]
        deleted_namespace = await client.core_v1.delete_namespace("async team")
        assert deleted_namespace.status == NamespaceStatus(phase="Terminating")

        with pytest.raises(APIError, match="async teapot"):
            await client.request("GET", "/unsupported", response_model=Namespace)


@pytest.mark.anyio
async def test_async_public_configuration_constructors(tmp_path: Path) -> None:
    requests = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal requests
        requests += 1
        assert request.headers["authorization"] in {
            "Bearer configured-token",
            "Bearer dynamic-token",
            "Bearer mounted-token",
        }
        if request.method == "POST":
            return httpx2.Response(201, content=request.content)
        return httpx2.Response(200, json=VERSION)

    transport = httpx2.MockTransport(handler)
    kubeconfig = (
        "apiVersion: v1\nkind: Config\ncurrent-context: selected\n"
        "clusters:\n- name: cluster\n  cluster:\n"
        "    server: https://kubernetes.invalid\n    insecure-skip-tls-verify: true\n"
        "contexts:\n- name: selected\n  context:\n    cluster: cluster\n    user: user\n"
        "users:\n- name: user\n  user:\n    token: configured-token\n"
    )
    kubeconfig_path = tmp_path / "config"
    kubeconfig_path.write_text(kubeconfig)
    service_account = tmp_path / "serviceaccount"
    service_account.mkdir()
    (service_account / "token").write_text("mounted-token\n")
    certificate = Path(__file__).parent / "fixtures" / "localhost.crt"
    (service_account / "ca.crt").write_bytes(certificate.read_bytes())
    (service_account / "namespace").write_text("default\n")

    dynamic = AsyncKubeClient.from_config(
        ClientConfig(
            server="https://kubernetes.invalid",
            token=None,
            verify=True,
            token_provider=lambda: "dynamic-token",
        ),
        transport=transport,
    )
    assert (await dynamic.version()).major == "1"
    created = await dynamic.core_v1.create_namespace(Namespace(metadata=ObjectMeta(name="dynamic")))
    assert created.metadata.name == "dynamic"
    await dynamic.close()

    yaml_client = AsyncKubeClient.from_kubeconfig_yaml(kubeconfig, transport=transport)
    assert (await yaml_client.version()).minor == "33"
    await yaml_client.close()

    file_client = AsyncKubeClient.from_kubeconfig(kubeconfig_path, transport=transport)
    assert (await file_client.version()).git_version == "v1.33.0"
    await file_client.close()

    cluster_client = AsyncKubeClient.from_in_cluster(
        service_account_path=service_account,
        environ={
            "KUBERNETES_SERVICE_HOST": "kubernetes.invalid",
            "KUBERNETES_SERVICE_PORT": "443",
        },
        transport=transport,
    )
    assert (await cluster_client.version()).platform == "linux/arm64"
    await cluster_client.close()

    assert requests == 5


class DisconnectingAsyncStream(httpx2.AsyncByteStream):
    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"delivered\n"
        raise httpx2.ReadError("async mid-stream disconnect")


@pytest.mark.anyio
async def test_async_retries_and_stream_failures() -> None:
    request_calls = 0
    stream_calls = 0

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal request_calls, stream_calls
        if request.url.path == "/version":
            request_calls += 1
            if request_calls == 1:
                raise httpx2.ConnectError("connect once", request=request)
            if request_calls == 2:
                return httpx2.Response(503, headers={"Retry-After": "0"})
            return httpx2.Response(200, json=VERSION)
        stream_calls += 1
        if stream_calls == 1:
            return httpx2.Response(503, headers={"Retry-After": "0"})
        if stream_calls == 2:
            raise httpx2.ConnectError("stream connect once", request=request)
        if stream_calls == 3:
            return httpx2.Response(200, text="ready\n")
        if stream_calls == 4:
            return httpx2.Response(404, text="missing stream")
        return httpx2.Response(200, stream=DisconnectingAsyncStream())

    policy = RetryPolicy(max_attempts=3, initial_backoff=0, max_backoff=0)
    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        retry_policy=policy,
        transport=httpx2.MockTransport(handler),
    ) as client:
        assert (await client.version()).major == "1"
        assert [line async for line in client.stream_lines("/logs")] == ["ready"]
        with pytest.raises(APIError, match="missing stream"):
            await anext(client.stream_lines("/logs"))
        disconnected = client.stream_lines("/logs")
        assert await anext(disconnected) == "delivered"
        with pytest.raises(httpx2.ReadError, match="async mid-stream disconnect"):
            await anext(disconnected)

    def terminal(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("terminal async disconnect", request=request)

    async with AsyncKubeClient(
        "https://kubernetes.invalid",
        retry_policy=None,
        transport=httpx2.MockTransport(terminal),
    ) as terminal_client:
        with pytest.raises(httpx2.ConnectError, match="terminal async disconnect"):
            await terminal_client.version()
        with pytest.raises(httpx2.ConnectError, match="terminal async disconnect"):
            await anext(terminal_client.stream_lines("/logs"))

    assert request_calls == 3
    assert stream_calls == 5


@pytest.mark.anyio
async def test_async_client_rotates_exec_certificate_connection_pool() -> None:
    initial = ssl.create_default_context()
    rotated = ssl.create_default_context()
    provider_calls = 0
    requests = 0

    def certificate_provider() -> ssl.SSLContext:
        nonlocal provider_calls
        provider_calls += 1
        return initial if provider_calls == 1 else rotated

    def handler(request: httpx2.Request) -> httpx2.Response:
        nonlocal requests
        requests += 1
        return httpx2.Response(200, json=VERSION)

    config = ClientConfig(
        server="https://kubernetes.invalid",
        token=None,
        verify=initial,
        certificate_provider=certificate_provider,
    )
    async with AsyncKubeClient.from_config(
        config, transport=httpx2.MockTransport(handler)
    ) as client:
        assert (await client.version()).major == "1"
        assert (await client.version()).minor == "33"
        assert (await client.version()).platform == "linux/arm64"

    assert provider_calls == 3
    assert requests == 3
