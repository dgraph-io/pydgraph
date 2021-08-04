# Publishing to PyPI

This document contains instructions to publish pydgraph to [PyPI].

[PyPI]: https://www.pypi.org/

## Before deploying

- Get access to credentials for Dgraph's account on PyPI with username dgraph-io
- Setup you  `~/.pypirc` file like this: (remember to replace `${password}` with
  the actual password)

```
[distutils]
index-servers=
    pypi
    testpypi

[pypi]
username: dgraph-io
password: ${password}

[testpypi]
repository: https://test.pypi.org/legacy/
username: dgraph-io
password: ${password}
```

## Deploying

- Build and test the code that needs to be published
- Bump version by modifying the `VERSION` variable in `pydgraph/meta.py` file
- If necessary, update the `CHANGELOG.md` file to reflect new changes
- Commit the changes
- Make sure you have [setuptools], [wheel], [twine], and [pypandoc]
  installed. You can install them by running the following:

  ```
  pip install -r publishing-requirements.txt
  ```
- Run the following commands:

```sh
# Remove build and dist directories
rm -rf build
rm -rf dist

# Package you project: source distribution and wheel
python setup.py sdist
python setup.py bdist_wheel

# Upload it to PyPI
twine upload dist/*
# For testing, try uploading to testpypi:
# twine upload --repository testpypi dist/*
```

- If necessary, create a new release tag on the Github repository

[setuptools]: https://pypi.org/project/setuptools/
[wheel]: https://pypi.org/project/wheel/
[twine]: https://pypi.org/project/twine/
[pypandoc]: https://pypi.org/project/pypandoc/
