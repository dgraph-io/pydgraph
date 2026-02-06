# Release Process

This document contains instructions to create a new pydgraph release and publish it to
[PyPI](https://pypi.org/).

1. Regenerate protobufs if [api.proto](pydgraph/proto/api.proto) was changed, see the
   [README](README.md#regenerating-protobufs)
1. Have a team member "at-the-ready" with repo `writer` access (you'll need them to approve PRs)
1. Create a new branch (prepare-for-release-vXX.X.X, for instance)
1. Update the VERSION in pydgraph/meta.py
1. Build pydgraph locally, see the [README](README.md#build-from-source)
1. Run the tests (`make test`) to ensure everything works
1. If you're concerned about incompatibilities with earlier Dgraph versions, invoke the test suite
   with earlier Dgraph versions

   ```sh
   DGRAPH_IMAGE_TAG=vX.X.X make test
   ```

1. If you happen to have the testpypi access token, try a test upload to testpypi:

   ```sh
   twine upload --repository testpypi dist/*
   ```

   - Verify readme and metadata look correct at testpypi

1. Regenerate protobufs if api.proto was changed: run `make protogen` (requires Python 3.13+ as
   specified in `.python-version` file)
1. Update the `CHANGELOG.md` file to reflect new changes. Sonnet 4.5 does a great job of doing this.
   Example prompt:

   ```text
   I'm releasing vXX.X.X off the main branch, add a new entry for this release. Conform to the
   "Keep a Changelog" format, use past entries as a formatting guide. Run the trunk linter on your changes.
   ```

1. Commit all this on your "prepare-for-release" branch and push up and create a PR
1. Once that's merged to the main branch, create a new draft release on GitHub from the
   [releases page](https://github.com/dgraph-io/pydgraph/releases). This step creates the new tag.
1. Run CD pipeline from
   [Github](https://github.com/dgraph-io/pydgraph/actions/workflows/cd-pydgraph.yml). The CD
   pipeline will build and publish to PyPI.
