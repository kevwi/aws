"""demo_control_plane.py

Demo + helper script for the Config & Rules Control Plane.

This script supports:
  - Publishing ANY local JSON config file from disk (dev-friendly).
  - Writing sample JSON files (shared seeds, quality rules, bindings).
  - Publishing those samples.
  - Reading back:
      * config rows (via control.v_config_active)
      * pipeline log rows (control.pipeline_log)
  - Writing to the pipeline log (via control.sp_log_pipeline_event_smart)

Prereqs:
  - Your DDL applied in Redshift (control schema, config_lookup, pipeline_log, procedures, views).
  - config_publish_json.py in the same folder (or on PYTHONPATH).
  - AWS creds configured (env vars, profile, or instance role).
  - Redshift Data API enabled for your cluster + db user.

Required env vars (recommended):
  CP_BUCKET
  CP_PREFIX
  CP_CLUSTER_ID
  CP_DATABASE
  CP_DB_USER
  CP_IAM_ROLE_ARN

Usage patterns:

  # 1) Publish any file from disk (the main dev workflow)
  python demo_control_plane.py publish-file --file path/to/config.json --upsert

  # 2) Generate demo JSON files
  python demo_control_plane.py write-samples --outdir demo_configs

  # 3) Publish demo JSON files
  python demo_control_plane.py publish-samples --outdir demo_configs --upsert

  # 4) Read config back
  python demo_control_plane.py read-config --domain shared --limit 50
  python demo_control_plane.py read-config --domain quality_rules --limit 50

  # 5) Write a pipeline log event (smart allocator)
  python demo_control_plane.py log-event --domain demo_app --app ingest_claims --status started --metal bronze --location s3://bucket/path

  # 6) Read pipeline log rows back
  python demo_control_plane.py read-pipeline-log --jobid 123 --limit 50
  python demo_control_plane.py read-pipeline-log --limit 50
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import boto3

# Import your publisher + read helper
try:
    from config_publish_json import publish_local_json, get_config  # type: ignore
except Exception as e:
    raise ImportError(
        "Could not import config_publish_json.py. Put this demo script in the same folder "
        "as config_publish_json.py (or install it on PYTHONPATH)."
    ) from e


@dataclass(frozen=True)
class CPEnv:
    bucket: str
    prefix: str
    cluster_id: str
    database: str
    db_user: str
    iam_role_arn: str


def _require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def load_env() -> CPEnv:
    return CPEnv(
        bucket=_require_env("CP_BUCKET"),
        prefix=_require_env("CP_PREFIX"),
        cluster_id=_require_env("CP_CLUSTER_ID"),
        database=_require_env("CP_DATABASE"),
        db_user=_require_env("CP_DB_USER"),
        iam_role_arn=_require_env("CP_IAM_ROLE_ARN"),
    )


def write_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
        f.write("\n")


def build_shared_seed() -> Dict[str, Any]:
    return {
        "domain": "shared",
        "entries": [
            # metal
            {"key1": "metal_code", "key2": "metal_name", "value1": "1", "value2": "bronze", "value3": ""},
            {"key1": "metal_code", "key2": "metal_name", "value1": "2", "value2": "silver", "value3": ""},
            {"key1": "metal_code", "key2": "metal_name", "value1": "3", "value2": "gold", "value3": ""},

            # status
            {"key1": "status_code", "key2": "status_name", "value1": "1", "value2": "started", "value3": ""},
            {"key1": "status_code", "key2": "status_name", "value1": "2", "value2": "success", "value3": ""},
            {"key1": "status_code", "key2": "status_name", "value1": "3", "value2": "failure", "value3": ""},
            {"key1": "status_code", "key2": "status_name", "value1": "4", "value2": "retry",   "value3": ""},

            # severity
            {"key1": "severity_code", "key2": "severity_name", "value1": "1", "value2": "info",     "value3": ""},
            {"key1": "severity_code", "key2": "severity_name", "value1": "2", "value2": "warn",     "value3": ""},
            {"key1": "severity_code", "key2": "severity_name", "value1": "3", "value2": "error",    "value3": ""},
            {"key1": "severity_code", "key2": "severity_name", "value1": "4", "value2": "critical", "value3": ""},

            # environment
            {"key1": "environment_code", "key2": "environment_name", "value1": "1", "value2": "dev",  "value3": ""},
            {"key1": "environment_code", "key2": "environment_name", "value1": "2", "value2": "qa",   "value3": ""},
            {"key1": "environment_code", "key2": "environment_name", "value1": "3", "value2": "uat",  "value3": ""},
            {"key1": "environment_code", "key2": "environment_name", "value1": "4", "value2": "prod", "value3": ""},
        ],
    }


def build_quality_rules() -> Dict[str, Any]:
    return {
        "domain": "quality_rules",
        "entries": [
            {"key1": "quality", "key2": "not_null",
             "value1": "{\"severity\":\"error\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NULL;",
             "value3": ""},

            {"key1": "quality", "key2": "unique",
             "value1": "{\"severity\":\"error\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM (SELECT [column_name], COUNT(*) c FROM [schema].[table] WHERE [where] GROUP BY [column_name] HAVING COUNT(*) > 1) d;",
             "value3": ""},

            {"key1": "quality", "key2": "between_numeric",
             "value1": "{\"severity\":\"error\",\"min\":\"0\",\"max\":\"120\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND ([column_name] < [min] OR [column_name] > [max]);",
             "value3": ""},

            {"key1": "quality", "key2": "regex_match",
             "value1": "{\"severity\":\"warn\",\"pattern\":\"^[A-Z0-9]{10}$\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND [column_name] !~ '[pattern]';",
             "value3": ""},

            {"key1": "quality", "key2": "referential_exists",
             "value1": "{\"severity\":\"error\",\"ref_schema\":\"silver\",\"ref_table\":\"members\",\"ref_column\":\"member_id\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] t LEFT JOIN [ref_schema].[ref_table] r ON t.[column_name] = r.[ref_column] WHERE [where] AND t.[column_name] IS NOT NULL AND r.[ref_column] IS NULL;",
             "value3": ""},
        ],
    }


def build_quality_apply() -> Dict[str, Any]:
    return {
        "domain": "quality_apply",
        "entries": [
            {"key1": "quality", "key2": "members:member_id:unique",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"severity\":\"error\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "members:member_id:not_null",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"severity\":\"error\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "members:email:regex_match",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"pattern\":\"^[^@]+@[^@]+\\\\.[^@]+$\",\"severity\":\"warn\"}",
             "value2": "", "value3": ""},
        ],
    }


def publish_file(env: CPEnv, path: str, *, upsert: bool, include_disabled: bool = False) -> Dict[str, Any]:
    # This is the "devs can simply upload a json file from disk" path.
    return publish_local_json(
        path,
        bucket=env.bucket,
        s3_prefix=env.prefix,
        cluster_id=env.cluster_id,
        database=env.database,
        db_user=env.db_user,
        iam_role_arn=env.iam_role_arn,
        upsert=upsert,
        include_disabled=include_disabled,
        apply_soft_delete=True,
    )


def execute_sql(env: CPEnv, sql: str) -> Dict[str, Any]:
    rsd = boto3.client("redshift-data")
    resp = rsd.execute_statement(
        ClusterIdentifier=env.cluster_id,
        Database=env.database,
        DbUser=env.db_user,
        Sql=sql,
    )
    statement_id = resp["Id"]
    while True:
        desc = rsd.describe_statement(Id=statement_id)
        if desc.get("Status") in ("FINISHED", "FAILED", "ABORTED"):
            if desc.get("Status") != "FINISHED":
                raise RuntimeError(f"SQL failed: {desc}")
            return {"statement_id": statement_id}
        time.sleep(0.5)


def query_sql(env: CPEnv, sql: str) -> List[Dict[str, Any]]:
    rsd = boto3.client("redshift-data")
    resp = rsd.execute_statement(
        ClusterIdentifier=env.cluster_id,
        Database=env.database,
        DbUser=env.db_user,
        Sql=sql,
    )
    statement_id = resp["Id"]
    while True:
        desc = rsd.describe_statement(Id=statement_id)
        if desc.get("Status") in ("FINISHED", "FAILED", "ABORTED"):
            if desc.get("Status") != "FINISHED":
                raise RuntimeError(f"Query failed: {desc}")
            break
        time.sleep(0.5)

    result = rsd.get_statement_result(Id=statement_id)
    cols = [c["name"] for c in result.get("ColumnMetadata", [])]
    rows: List[Dict[str, Any]] = []
    for rec in result.get("Records", []):
        row = {}
        for col, val in zip(cols, rec):
            row[col] = (list(val.values())[0] if val else None)
        rows.append(row)
    return rows


def cmd_write_samples(args: argparse.Namespace) -> None:
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)
    write_json(os.path.join(outdir, "shared_seed.json"), build_shared_seed())
    write_json(os.path.join(outdir, "quality_rules.json"), build_quality_rules())
    write_json(os.path.join(outdir, "quality_apply.json"), build_quality_apply())
    print(f"Wrote sample JSON files to: {os.path.abspath(outdir)}")


def cmd_publish_samples(args: argparse.Namespace) -> None:
    env = load_env()
    outdir = args.outdir
    paths = [
        os.path.join(outdir, "shared_seed.json"),
        os.path.join(outdir, "quality_rules.json"),
        os.path.join(outdir, "quality_apply.json"),
    ]
    receipts = {}
    for p in paths:
        receipts[os.path.basename(p)] = publish_file(env, p, upsert=args.upsert, include_disabled=args.include_disabled)
    print(json.dumps(receipts, indent=2))


def cmd_publish_file(args: argparse.Namespace) -> None:
    env = load_env()
    receipt = publish_file(env, args.file, upsert=args.upsert, include_disabled=args.include_disabled)
    print(json.dumps(receipt, indent=2))


def cmd_read_config(args: argparse.Namespace) -> None:
    env = load_env()
    where = f"domain = '{args.domain.lower()}'"
    rows = get_config(
        cluster_id=env.cluster_id,
        database=env.database,
        db_user=env.db_user,
        schema="control",
        view="v_config_active" if not args.include_disabled else "v_config",
        where_sql=where,
        limit=args.limit,
    )
    print(json.dumps(rows, indent=2))


def cmd_log_event(args: argparse.Namespace) -> None:
    env = load_env()
    # Uses the DDL proc: control.sp_log_pipeline_event_smart(...)
    sql = f"""CALL control.sp_log_pipeline_event_smart(
      '{args.domain}',
      '{args.app}',
      '{args.status}',
      '{args.metal}',
      '{args.location}',
      {args.jobid if args.jobid is not None else 'NULL'},
      NULL,
      NULL
    );"""
    # The OUT params aren't surfaced via Data API easily; we demo by querying latest log rows after.
    execute_sql(env, sql)
    print("Logged pipeline event via control.sp_log_pipeline_event_smart.")
    rows = query_sql(env, "SELECT * FROM control.pipeline_log ORDER BY created DESC, id DESC LIMIT 5;")
    print(json.dumps(rows, indent=2))


def cmd_read_pipeline_log(args: argparse.Namespace) -> None:
    env = load_env()
    where = "1=1"
    if args.jobid is not None:
        where = f"jobid = {int(args.jobid)}"
    sql = f"SELECT * FROM control.pipeline_log WHERE {where} ORDER BY created DESC, id DESC LIMIT {int(args.limit)};"
    rows = query_sql(env, sql)
    print(json.dumps(rows, indent=2))


def cmd_run_quality(args: argparse.Namespace) -> None:
    env = load_env()
    run_id = args.run_id or f"demo_{int(time.time())}"
    sql = f"""CALL control.run_quality_checks_for_table(
      '{run_id}',
      '{args.source}',
      '{args.schema}',
      '{args.table}',
      {str(bool(args.fail_on_error)).lower()}
    );"""
    execute_sql(env, sql)
    print(f"Quality checks executed. run_id={run_id}")
    rows = query_sql(env, f"SELECT * FROM control.quality_results WHERE run_id = '{run_id}' ORDER BY executed_at;")
    print(json.dumps(rows, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("write-samples", help="Write sample JSON files to an output directory")
    p.add_argument("--outdir", default="demo_configs")
    p.set_defaults(fn=cmd_write_samples)

    p = sub.add_parser("publish-samples", help="Publish the sample JSON files from an output directory")
    p.add_argument("--outdir", default="demo_configs")
    p.add_argument("--upsert", action="store_true", help="Upsert (recommended for repeatable demos)")
    p.add_argument("--include-disabled", action="store_true", help="Include disabled field if present")
    p.set_defaults(fn=cmd_publish_samples)

    p = sub.add_parser("publish-file", help="Publish ANY local JSON file from disk (dev workflow)")
    p.add_argument("--file", required=True)
    p.add_argument("--upsert", action="store_true")
    p.add_argument("--include-disabled", action="store_true")
    p.set_defaults(fn=cmd_publish_file)

    p = sub.add_parser("read-config", help="Read config rows back from control.v_config(_active)")
    p.add_argument("--domain", required=True)
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--include-disabled", action="store_true")
    p.set_defaults(fn=cmd_read_config)

    p = sub.add_parser("log-event", help="Write a pipeline log event via stored proc")
    p.add_argument("--domain", required=True, help="Namespace domain for code allocation (e.g., demo_app)")
    p.add_argument("--app", required=True, help="Process/app name (process_name)")
    p.add_argument("--status", required=True, help="Status string (e.g., started|success|failure)")
    p.add_argument("--metal", required=True, help="Metal string (bronze|silver|gold)")
    p.add_argument("--location", required=True, help="Location text (S3 path, topic, etc.)")
    p.add_argument("--jobid", type=int, default=None, help="Jobid (required for non-bronze; bronze allocates)")
    p.set_defaults(fn=cmd_log_event)

    p = sub.add_parser("read-pipeline-log", help="Read pipeline log rows back from control.pipeline_log")
    p.add_argument("--jobid", type=int, default=None)
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(fn=cmd_read_pipeline_log)

    p = sub.add_parser("run-quality", help="Execute quality checks for one table and print results")
    p.add_argument("--schema", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--run-id", default=None)
    p.add_argument("--source", default="demo", help="unit|pipeline|demo")
    p.add_argument("--fail-on-error", action="store_true", help="Raise exception if any error severity fails")
    p.set_defaults(fn=cmd_run_quality)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nâ Demo failed: {e}\n", file=sys.stderr)
        raise
