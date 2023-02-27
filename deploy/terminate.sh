#!/usr/bin/env bash

SCRIPT_PATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# Load common functions
. "$SCRIPT_PATH/common/emr-helpers.sh"

# Find deploy directory
experiments_dir="$SCRIPT_PATH/experiments/rumbledb"
deploy_dir="$(discover_cluster "$experiments_dir")"

# Close SSH tunnel
kill $( cat "$deploy_dir/tunnel.pid" || echo -1 ) || echo "Could not find SSH tunnel..."

# Terminate
terminate_cluster "$deploy_dir"
