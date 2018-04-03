#!/bin/bash

set -e
set -x

source scripts/functions.sh

init
startZero
start

coverage run --source=pydgraph setup.py test

quit 0
