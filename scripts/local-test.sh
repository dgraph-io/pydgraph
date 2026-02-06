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
	# shellcheck disable=SC2312
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
	# shellcheck disable=SC2312
	alphaHttpPort=$(DockerCompose port alpha1 8080 | awk -F: '{print $2}')
	# shellcheck disable=SC2312
	alphaGrpcPort=$(DockerCompose port alpha1 9080 | awk -F: '{print $2}')
	# Wait for HTTP health endpoint
	wait-for-healthy 127.0.0.1:"${alphaHttpPort}"/health
}

function stopCluster() {
	DockerCompose down -t 5 -v
}

if [ -z "${SRCDIR}" ]; then
	SRCDIR=$(readlink -f "${BASH_SOURCE[0]%/*}")
fi
if [ ! -d "${SRCDIR}/../scripts" ]; then
	echo "No scripts directory found at \"${SRCDIR}/../scripts\""
	echo "Trying alternate locations for SRCDIR..."
	for dir in "./scripts"; do
		echo -n "Trying \"${dir}\"... "
		if [ -d "${dir}" ]; then
			echo "found: ${dir}"
			SRCDIR="${dir}"
			echo "Setting SRCDIR=\"${dir}\""
			break
		else
			echo "not found: ${dir}"
		fi
		if [ ! -d "${SRCDIR}"]; then
			echo "Unable to determine script SRCDIR."
			echo "Please re-run with SRCDIR set to correct project root."
			exit 1
		fi
	done
fi

readonly SRCDIR

SRCDIR_VENV="${SRCDIR}/../.venv"
VENV_ACTIVATE="${SRCDIR_VENV}/bin/activate"
if [ "${VIRTUAL_ENV}" != "${SRCDIR_VENV}" ]; then
	if [ -e "${VENV_ACTIVATE}" ]; then
		echo "Ensuring use of SRCDIR virtual env using \"${VENV_ACTIVATE}\""
		source "${VENV_ACTIVATE}"
	else
		echo "WARNING: Can't activate SRCDIR virtual env, no activate script found at \"${VENV_ACTIVATE}\""
	fi
fi

# Run cluster and tests
pushd "$(dirname "${SRCDIR}")" || exit
pushd "${SRCDIR}"/../tests || exit

restartCluster
# shellcheck disable=SC2312
alphaGrpcPort=$(DockerCompose port alpha1 9080 | awk -F: '{print $2}')
popd || exit
export TEST_SERVER_ADDR="127.0.0.1:${alphaGrpcPort}"
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
