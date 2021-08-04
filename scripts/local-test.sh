#!/bin/bash
# Runs the 6-node test cluster (3 Zeros, 3 Alphas, replication level 3) whose
# first Alpha runs on port 9180. This script does not install Dgraph since it
# is intended for local development. Instead it assumes dgraph is already
# installed.

readonly SRCDIR=$(readlink -f ${BASH_SOURCE[0]%/*})

# Install dependencies
pip install -r requirements.txt
pip install coveralls

# Run cluster and tests
pushd $(dirname $SRCDIR)
source $GOPATH/src/github.com/dgraph-io/dgraph/contrib/scripts/functions.sh
restartCluster
pushd $GOPATH/src/github.com/dgraph-io/dgraph/dgraph
  alphaport=$(docker-compose -p dgraph port alpha1 9080 | awk -F: '{print $2}')
popd
export TEST_SERVER_ADDR="localhost:$alphaport"
coverage run --source=pydgraph --omit=pydgraph/proto/* setup.py test || stopCluster
stopCluster
popd
