#!/usr/bin/env bash
set -euo pipefail

# demo_control_plane.sh
#
# One-command demo driver for the Config & Rules Control Plane.
#
# Prereqs:
#   - python + pip
#   - AWS credentials configured
#   - Redshift DDL installed (control schema + procedures/views)
#   - config_publish_json.py and demo_control_plane.py in the same folder
#
# Usage:
#   export CP_BUCKET=...
#   export CP_PREFIX=config/demo
#   export CP_CLUSTER_ID=...
#   export CP_DATABASE=...
#   export CP_DB_USER=...
#   export CP_IAM_ROLE_ARN=...
#
#   bash demo_control_plane.sh
#
# Optional:
#   export DEMO_SCHEMA=silver
#   export DEMO_TABLE=members

: "${CP_BUCKET:?Set CP_BUCKET}"
: "${CP_PREFIX:?Set CP_PREFIX}"
: "${CP_CLUSTER_ID:?Set CP_CLUSTER_ID}"
: "${CP_DATABASE:?Set CP_DATABASE}"
: "${CP_DB_USER:?Set CP_DB_USER}"
: "${CP_IAM_ROLE_ARN:?Set CP_IAM_ROLE_ARN}"

DEMO_SCHEMA="${DEMO_SCHEMA:-silver}"
DEMO_TABLE="${DEMO_TABLE:-members}"

echo "==> Installing deps (boto3)..."
python -m pip install --quiet --upgrade boto3

echo "==> Writing demo JSON files..."
python demo_control_plane.py --write-only

echo "==> Publishing demo configs (upsert=true)..."
python demo_control_plane.py --publish --upsert

echo "==> (Optional) Run quality checks: ${DEMO_SCHEMA}.${DEMO_TABLE}"
python demo_control_plane.py --run-quality --schema "${DEMO_SCHEMA}" --table "${DEMO_TABLE}"

echo "==> (Optional) Print sample config rows..."
python demo_control_plane.py --print-config

python demo_control_plane.py publish-file --file path/to/whatever.json --upsert
echo "â Done."
