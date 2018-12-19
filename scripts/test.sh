#!/bin/bash
# Installs the latest Dgraph from the master branch and runs the 6-node test
# cluster (3 Zeros, 3 Alphas, replication level 3) whose first Alpha runs on
# port 9180.

readonly SRCDIR=$(readlink -f ${BASH_SOURCE[0]%/*})

# Install Dgraph
if [ ! -n "$GOPATH" ]; then
    echo 'GOPATH not set'
    exit 1
fi
go get -d -v github.com/dgraph-io/dgo
go get -d -v github.com/dgraph-io/dgraph/dgraph

pushd $GOPATH/src/github.com/dgraph-io/dgraph
make install
dgraph version
popd

# Install dependencies
pip install -r requirements.txt
pip install -r coveralls

# Run cluster and tests
pushd $(dirname $SRCDIR)
source $GOPATH/src/github.com/dgraph-io/dgraph/contrib/scripts/functions.sh
restartCluster
coverage run --source=pydgraph --omit=pydgraph/proto/* setup.py test
stopCluster
popd
