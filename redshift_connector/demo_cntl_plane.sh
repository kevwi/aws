#!/usr/bin/env bash
set -euo pipefail

# demo_control_plane.sh
#
# This script supports:
#   1) Full demo (no args)
#   2) Uploading ANY local JSON file directly
#
# Required env:
#   CP_BUCKET, CP_PREFIX, CP_CLUSTER_ID, CP_DATABASE, CP_DB_USER, CP_IAM_ROLE_ARN
#
# ---------------------------------------------
# USAGE
# ---------------------------------------------
#
# Full demo:
#   bash demo_control_plane.sh
#
# Upload a JSON file directly:
#   bash demo_control_plane.sh --file path/to/config.json --upsert
#
# ---------------------------------------------

: "${CP_BUCKET:?Set CP_BUCKET}"
: "${CP_PREFIX:?Set CP_PREFIX}"
: "${CP_CLUSTER_ID:?Set CP_CLUSTER_ID}"
: "${CP_DATABASE:?Set CP_DATABASE}"
: "${CP_DB_USER:?Set CP_DB_USER}"
: "${CP_IAM_ROLE_ARN:?Set CP_IAM_ROLE_ARN}"

DEMO_OUTDIR="${DEMO_OUTDIR:-demo_configs}"
DEMO_SCHEMA="${DEMO_SCHEMA:-silver}"
DEMO_TABLE="${DEMO_TABLE:-members}"

JSON_FILE=""
UPSERT_FLAG=""

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --file)
      JSON_FILE="$2"
      shift 2
      ;;
    --upsert)
      UPSERT_FLAG="--upsert"
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

echo "==> Installing deps (boto3)..."
python -m pip install --quiet --upgrade boto3

# ---------------------------------------------
# Direct JSON upload mode
# ---------------------------------------------
if [[ -n "$JSON_FILE" ]]; then
  if [[ ! -f "$JSON_FILE" ]]; then
    echo "File not found: $JSON_FILE"
    exit 1
  fi

  echo "==> Publishing JSON file: $JSON_FILE"
  python demo_control_plane.py publish-file --file "$JSON_FILE" $UPSERT_FLAG
  echo "â Upload complete."
  exit 0
fi

# ---------------------------------------------
# Full demo mode
# ---------------------------------------------

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

echo "â Demo complete."
