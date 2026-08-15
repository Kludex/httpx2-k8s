# Releasing

Releases are built from Git tags and published to PyPI through GitHub trusted publishing. The tag
is the package version, so there is no version field to update manually and no long-lived PyPI
token stored in the repository.

## Prepare a release

1. Move the relevant entries in `CHANGELOG.md` from `Unreleased` to the target version and date.
2. Run the fast suite, strict type checker, formatter, linter, distribution build, and publish
   dry-run.
3. Merge the release commit after CI and the K3s matrix pass.
4. Create a GitHub Release with a `vX.Y.Z` tag pointing to that commit, then publish the release.

Publishing the GitHub Release starts the workflow. `uv-dynamic-versioning` derives the package
version from its tag, the workflow verifies the resolved version, builds from the tagged source,
uploads the artifacts to the workflow run, and publishes them from the protected `pypi`
environment.

PyPI must have a trusted publisher configured for this repository, the `publish.yml` workflow,
and the `pypi` environment before the first release.
