#!/bin/bash
# Runs the 6-node test cluster (3 Zeros, 3 Alphas, replication level 3). This
# script does not install Dgraph since it is intended for local development.
# Instead it assumes dgraph is already installed.

function DockerCompose() {
	docker compose -p pydgraph "$@"
}

function wait-for-healthy() {
	printf 'wait-for-healthy: Waiting for %s to return 200 OK\n' "$1"
	tries=0
	until curl -sL -w '%{http_code}\n' "$1" -o /dev/null | grep -q 200; do
		tries=${tries}+1
		if [[ ${tries} -gt 300 ]]; then
			printf "wait-for-healthy: Took longer than 1 minute to be healthy.\n"
			printf "wait-for-healthy: Waiting stopped.\n"
			return 1
		fi
		sleep 0.2
	done
	printf "wait-for-healthy: Done.\n"
}

function restartCluster() {
	DockerCompose up --detach --force-recreate
	alphaHttpPort=$(DockerCompose port alpha1 8080 | awk -F: '{print $2}')
	wait-for-healthy localhost:"${alphaHttpPort}"/health
	sleep 5
}

function stopCluster() {
	DockerCompose down -t 5
}

SRCDIR=$(readlink -f "${BASH_SOURCE[0]%/*}")
readonly SRCDIR

# Run cluster and tests
pushd "$(dirname "${SRCDIR}")" || exit
pushd "${SRCDIR}"/../tests || exit
restartCluster
alphaGrpcPort=$(DockerCompose port alpha1 9080 | awk -F: '{print $2}')
popd || exit
export TEST_SERVER_ADDR="localhost:${alphaGrpcPort}"
echo "Using TEST_SERVER_ADDR=${TEST_SERVER_ADDR}"

# Use uv if available, otherwise run pytest directly
if command -v uv >/dev/null 2>&1; then
	PYTEST_CMD="uv run pytest"
else
	PYTEST_CMD="pytest"
fi

if [[ $# -eq 0 ]]; then
	# No arguments provided, run all tests
	${PYTEST_CMD}
else
	# Run specific tests passed as arguments
	${PYTEST_CMD} "$@"
fi
tests_failed="$?"
stopCluster
popd || exit
if [[ ${tests_failed} -ne 0 ]]; then
	exit 1
fi
