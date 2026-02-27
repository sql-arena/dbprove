#!/bin/bash

# Source environment variables for Databricks configuration
if [ -f "env.sh" ]; then
    source ./env.sh
else
    echo "Error: env.sh not found in current directory."
    exit 1
fi

# Determine workspace URL (ensuring it starts with https://)
WORKSPACE_URL="https://${DATABRICKS_HOSTNAME}"

# Call the dump_databricks_plan.mjs script in headful mode to allow for authentication.
node scripts/dump_databricks_plan.mjs \
    --workspace "${WORKSPACE_URL}" \
    --headless 0 \
    -v

echo "Browser session closed. If you logged in, your session should be saved in the playwright profile."
