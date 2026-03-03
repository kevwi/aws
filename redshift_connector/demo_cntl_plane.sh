#!/usr/bin/env bash
set -euo pipefail

# demo_control_plane.sh
#
# End-to-end demo driver.
#
# Expects:
#   - config_publish_json.py and demo_control_plane.py in the same directory
#
# Required env:
#   CP_BUCKET, CP_PREFIX, CP_CLUSTER_ID, CP_DATABASE, CP_DB_USER, CP_IAM_ROLE_ARN
#
# Run:
#   bash demo_control_plane.sh
#
# Demo steps:
#   1) write sample JSON files
#   2) publish samples (upsert)
#   3) read config back
#   4) write a pipeline log event
#   5) read pipeline_log back
#   6) (optional) run quality checks if proc exists

: "${CP_BUCKET:?Set CP_BUCKET}"
: "${CP_PREFIX:?Set CP_PREFIX}"
: "${CP_CLUSTER_ID:?Set CP_CLUSTER_ID}"
: "${CP_DATABASE:?Set CP_DATABASE}"
: "${CP_DB_USER:?Set CP_DB_USER}"
: "${CP_IAM_ROLE_ARN:?Set CP_IAM_ROLE_ARN}"

DEMO_OUTDIR="${DEMO_OUTDIR:-demo_configs}"
DEMO_SCHEMA="${DEMO_SCHEMA:-silver}"
DEMO_TABLE="${DEMO_TABLE:-members}"

echo "==> Installing deps (boto3)..."
python -m pip install --quiet --upgrade boto3

echo "==> 1) Writing sample JSON files..."
python demo_control_plane.py write-samples --outdir "${DEMO_OUTDIR}"

echo "==> 2) Publishing samples (upsert=true)..."
python demo_control_plane.py publish-samples --outdir "${DEMO_OUTDIR}" --upsert

echo "==> 3) Reading config back (shared)..."
python demo_control_plane.py read-config --domain shared --limit 25

echo "==> 4) Writing a pipeline log event..."
python demo_control_plane.py log-event --domain demo_app --app demo_ingest --status started --metal bronze --location "s3://example/demo/path"

echo "==> 5) Reading pipeline_log back (latest 10)..."
python demo_control_plane.py read-pipeline-log --limit 10

echo "==> 6) (Optional) Running quality checks (if installed): ${DEMO_SCHEMA}.${DEMO_TABLE}"
set +e
python demo_control_plane.py run-quality --schema "${DEMO_SCHEMA}" --table "${DEMO_TABLE}" --fail-on-error
set -e

echo "â Done."
