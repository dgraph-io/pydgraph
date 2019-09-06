# Copyright 2016 Dgraph Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

try:
    from pypandoc import convert_file
except ImportError as e:
    # NOTE: error is thrown only for package build steps
    if 'sdist' in sys.argv or 'bdist_wheel' in sys.argv:
        raise e

    def convert_file(f, _):
        return open(f, 'r').read()
except ModuleNotFoundError as e:
    # NOTE: error is thrown only for package build steps
    if 'sdist' in sys.argv or 'bdist_wheel' in sys.argv:
        raise e

    def convert_file(f, _):
        return open(f, 'r').read()

from pydgraph.meta import VERSION

README = os.path.join(os.path.dirname(__file__), 'README.md')

setup(
    name='pydgraph',
    version=VERSION,
    description='Official Dgraph client implementation for Python',
    long_description=convert_file(README, 'rst'),
    license='Apache License, Version 2.0',
    author='Dgraph Labs',
    author_email='contact@dgraph.io',
    url='https://github.com/dgraph-io/pydgraph',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Software Development',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    packages=['pydgraph', 'pydgraph.proto'],
    install_requires=open('requirements.txt').readlines(),
    test_suite='tests',
)
