# Copyright 2018 DGraph Labs, Inc.
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

import sys

from pydgraph.meta import VERSION

__author__ = 'Shailesh Kochhar <shailesh.kochhar@gmail.com>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'
__version__ = VERSION
__status__ = 'development'


def merge_lin_reads(target, src):
    if src is None:
        return target

    # cache for the loop
    target_ids = target.ids
    target_ids_get = target_ids.get

    for key, src_value in src.ids.items():
        if target_ids_get(key, 0) <= src_value:
            target_ids[key] = src_value

def is_string(s):
    if sys.version_info[0] < 3:
        return isinstance(s, basestring)
    else:
        return isinstance(s, str)
