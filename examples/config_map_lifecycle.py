"""Create, patch, and delete one ConfigMap using the public synchronous API."""

from __future__ import annotations

import argparse

from httpx2_k8s import ConfigMap, KubeClient, MergePatch, ObjectMeta


def run(client: KubeClient, namespace: str, name: str) -> ConfigMap:
    """Run a complete ConfigMap lifecycle and return the patched object."""
    created = client.core_v1.create_namespaced_config_map(
        namespace,
        ConfigMap(
            metadata=ObjectMeta(name=name, labels={"app.kubernetes.io/managed-by": "httpx2-k8s"}),
            data={"mode": "development"},
        ),
    )
    updated = client.core_v1.patch_namespaced_config_map(
        name,
        namespace,
        MergePatch(document={"data": {"mode": "production"}}),
        field_manager="httpx2-k8s-example",
    )
    deleted = client.core_v1.delete_namespaced_config_map(name, namespace)
    if deleted.status != "Success":
        raise RuntimeError(f"Kubernetes did not confirm deletion of {created.metadata.name!r}")
    return updated


def main() -> None:
    """Parse connection options and run the example against the selected cluster."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", help="kubeconfig context; defaults to current-context")
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--name", default="httpx2-k8s-example")
    args = parser.parse_args()

    with KubeClient.from_kubeconfig(context=args.context) as client:
        updated = run(client, args.namespace, args.name)
    print(f"patched and deleted {updated.metadata.namespace}/{updated.metadata.name}")


if __name__ == "__main__":
    main()
