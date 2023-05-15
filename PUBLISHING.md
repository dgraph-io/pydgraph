# Publishing to PyPI

This document contains instructions to publish pydgraph to [PyPI].

[PyPI]: https://www.pypi.org/

## Before deploying

- Build pydgraph locally (see readme)
- Push to testpypi (`twine upload --repository testpypi dist/*`)
- Verify readme and metadata look correct

## Deploying

- Regenerate protobufs if api.proto was changed
- Bump version by modifying the `VERSION` variable in `pydgraph/meta.py` file
- Update the `CHANGELOG.md` file to reflect new changes
- Tag pydgraph locally (`git tag x.y.z`) and push to origin (`git push x.y.z`)
- Run CD pipeline