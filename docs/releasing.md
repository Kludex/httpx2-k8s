# Releasing

Releases are built from tags and published to PyPI through GitHub trusted publishing. No long-lived
PyPI token is stored in the repository.

## Prepare a release

1. Move the relevant entries in `CHANGELOG.md` from `Unreleased` to the target version and date.
2. Set the same version in `pyproject.toml`.
3. Run the fast suite, strict type checker, formatter, linter, distribution build, and publish
   dry-run.
4. Merge the release commit after CI and the K3s matrix pass.
5. Create and push an annotated `vX.Y.Z` tag matching the package version.

The release workflow rejects a tag whose value does not exactly match the version in
`pyproject.toml`. It rebuilds from the tagged source, uploads the artifacts to the workflow run,
then publishes them from the protected `pypi` environment.

PyPI must have a trusted publisher configured for this repository, the `publish.yml` workflow,
and the `pypi` environment before the first release.
