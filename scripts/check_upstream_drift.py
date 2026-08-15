from __future__ import annotations

import argparse
import json
import os
import re
import urllib.request
from pathlib import Path
from typing import TypedDict, cast

PROJECT_ROOT = Path(__file__).parents[1]
SOURCES_PATH = PROJECT_ROOT / "compliance" / "openapi" / "sources.json"
USER_AGENT = "httpx2-k8s-upstream-drift"
STABLE_TAG = re.compile(r"^v(?P<major>\d+)\.(?P<minor>\d+)\.0$")
CLIENT_VERSION = re.compile(r"^(?P<major>\d+)\.(?P<patch>\d+)\.(?P<fix>\d+)$")


class Source(TypedDict):
    commit: str
    kubernetes_tag: str
    python_client: str


class SourcesDocument(TypedDict):
    versions: list[Source]


class DriftResult(TypedDict):
    changed: bool
    commit: str
    kubernetes_tag: str
    python_client: str


def _source_tag(source: Source) -> str:
    return source["kubernetes_tag"]


def _request_json(url: str) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)


def _latest_kubernetes_release() -> tuple[str, str]:
    releases = _request_json("https://api.github.com/repos/kubernetes/kubernetes/releases")
    if not isinstance(releases, list):
        raise TypeError("Expected the GitHub releases API to return a list")
    candidates = tuple(
        cast(dict[str, object], release)
        for release in releases
        if isinstance(release, dict)
        and isinstance(release.get("tag_name"), str)
        and STABLE_TAG.fullmatch(cast(str, release["tag_name"]))
        and not release.get("draft")
        and not release.get("prerelease")
    )
    if not candidates:
        raise RuntimeError("No stable Kubernetes minor release found")
    tag = cast(str, candidates[0]["tag_name"])
    commit_document = _request_json(
        f"https://api.github.com/repos/kubernetes/kubernetes/commits/{tag}"
    )
    if not isinstance(commit_document, dict) or not isinstance(commit_document.get("sha"), str):
        raise TypeError("Expected the GitHub commit API to return a SHA")
    return tag, cast(str, commit_document["sha"])


def _matching_python_client(kubernetes_tag: str) -> str:
    match = STABLE_TAG.fullmatch(kubernetes_tag)
    if match is None:
        raise ValueError(f"Not a stable Kubernetes minor tag: {kubernetes_tag}")
    minor = int(match.group("minor"))
    document = _request_json("https://pypi.org/pypi/kubernetes/json")
    if not isinstance(document, dict) or not isinstance(document.get("releases"), dict):
        raise TypeError("Expected the PyPI project API to return releases")
    versions = tuple(
        (int(version_match.group("patch")), int(version_match.group("fix")), version)
        for version in cast(dict[str, object], document["releases"])
        if (version_match := CLIENT_VERSION.fullmatch(version)) is not None
        and int(version_match.group("major")) == minor
    )
    if not versions:
        raise RuntimeError(f"No official Python client release found for {kubernetes_tag}")
    return max(versions)[2]


def _write_github_output(result: DriftResult) -> None:
    output_path = os.getenv("GITHUB_OUTPUT")
    if output_path is None:
        return
    with Path(output_path).open("a") as output:
        output.write(f"changed={str(result['changed']).lower()}\n")
        output.write(f"kubernetes_tag={result['kubernetes_tag']}\n")
        output.write(f"commit={result['commit']}\n")
        output.write(f"python_client={result['python_client']}\n")


def check_upstream(*, update: bool) -> DriftResult:
    sources = cast(SourcesDocument, json.loads(SOURCES_PATH.read_text()))
    tag, commit = _latest_kubernetes_release()
    python_client = _matching_python_client(tag)
    changed = tag not in {source["kubernetes_tag"] for source in sources["versions"]}
    if changed and update:
        sources["versions"].append(
            {"commit": commit, "kubernetes_tag": tag, "python_client": python_client}
        )
        sources["versions"].sort(key=_source_tag)
        SOURCES_PATH.write_text(json.dumps(sources, indent=2, sort_keys=True) + "\n")
    result: DriftResult = {
        "changed": changed,
        "commit": commit,
        "kubernetes_tag": tag,
        "python_client": python_client,
    }
    _write_github_output(result)
    return result


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Detect a new stable Kubernetes API baseline")
    parser.add_argument("--update", action="store_true", help="append a newly discovered baseline")
    return parser.parse_args()


def main() -> None:
    result = check_upstream(update=_arguments().update)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
