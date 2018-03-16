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

__author__ = 'Garvit Pahal <garvit@dgraph.io>'
__maintainer__ = 'Garvit Pahal <garvit@dgraph.io>'

from pydgraph.proto import api_pb2 as api

def create_lin_read(ids):
    lr = api.LinRead()
    ids = lr.ids
    for key, value in ids:
        ids[key] = value
    
    return lr

def are_lin_reads_equal(a, b):
    aIds = a.ids
    bIds = b.ids

    if len(aIds) != len(bIds):
        return False
    
    for key in aIds.items():
        if key not in bIds:
            return False
    
    return True
