# Kubernetes API compliance

The official, tagged Kubernetes OpenAPI documents are the source of truth for typed API coverage.
The matching `kubernetes` Python package is a secondary naming and surface-area cross-check; its
generated module layout is not an API design requirement for `httpx2-k8s`.

## Support policy

- The newest released Kubernetes minor must have zero operation or schema gaps before a release.
- Every older minor advertised as supported must retain the operations and schemas present in its
  pinned specification.
- Consolidated Pythonic methods are allowed when they expose the same typed HTTP operation.
- `Any`, `object`, untyped mappings, `Unstructured`, and extra-model storage do not count as typed
  coverage when the official schema defines a structure.
- Temporary exceptions require an exact schema/operation path, rationale, upstream link, owner,
  and expiry release. A complete-replacement claim requires an empty exception file.

[`openapi/sources.json`](openapi/sources.json) pins each Kubernetes tag to an immutable commit and
records the matching official Python client release. [`openapi/locks.json`](openapi/locks.json)
records the SHA-256 hash and inventory counts for the v2 document and every split v3 document.
The compressed manifests are deterministic, offline fixtures containing normalized operations and
schemas with prose removed but all typing, wire-format, and Kubernetes extension metadata intact.

[`python-client-inventory.json.gz`](python-client-inventory.json.gz) records every public API
method and HTTP operation found in each matching official Python wheel. Its wheel hashes and
reviewable counts are pinned in
[`python-client-locks.json`](python-client-locks.json). Differences are retained in the inventory:
the official client consolidates watches and has generic custom-object paths, while the OpenAPI
documents remain authoritative for the actual Kubernetes surface.

## Updating

```console
uv run python scripts/update_openapi.py --version v1.36.0
```

Omit `--version` to update every configured release. The updater:

1. verifies that the release tag still resolves to the pinned commit;
2. downloads the official v2 and split v3 specifications by immutable commit;
3. verifies that v2 and v3 expose identical operations and Kubernetes identities;
4. resolves split-document schema references into one normalized inventory;
5. writes reproducible gzip manifests and a human-reviewable lock file.

CI reads only the committed fixtures. Network access is not required for compliance tests.

Refresh the secondary Python-client inventory after changing a pin:

```console
uv run python scripts/update_python_client_inventory.py
```

Generate the deterministic machine-readable and documentation reports with:

```console
uv run python scripts/compliance_report.py
uv run python scripts/compliance_report.py --check
```

The release workflow requires a complete report, an empty exception manifest, and a wheel whose
public models and sync/async facades reproduce the pinned inventory.
