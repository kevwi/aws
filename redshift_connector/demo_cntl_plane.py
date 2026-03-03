"""demo_control_plane.py

Quick demo script for the Config & Rules Control Plane.

What this does:
  1) Writes sample JSON config files locally (shared seeds, quality rules, quality bindings).
  2) Publishes them using the publisher module (config_publish_json.py).
  3) Optionally runs a quality check stored procedure (requires your DDL to be installed).
  4) Optionally reads config back via control.v_config_active.

Prereqs:
  - Your DDL applied in Redshift (control schema, config_lookup, procedures, views).
  - The publisher module available on PYTHONPATH (same folder is fine):
      config_publish_json.py
  - AWS creds configured (env vars, profile, or instance role).
  - Redshift Data API enabled for your cluster + db user.

Environment variables (recommended):
  CP_BUCKET        (S3 bucket)
  CP_PREFIX        (S3 key prefix, e.g. config/demo)
  CP_CLUSTER_ID    (Redshift cluster identifier)
  CP_DATABASE      (Redshift database)
  CP_DB_USER       (Redshift db user)
  CP_IAM_ROLE_ARN  (IAM role ARN used by COPY)
  CP_REGION        (optional; boto3 uses default if omitted)

Usage:
  python demo_control_plane.py --write-only
  python demo_control_plane.py --publish
  python demo_control_plane.py --publish --run-quality --schema silver --table members

Notes:
  - This script is safe to run repeatedly if you publish with --upsert.
  - If you publish without upsert and rows already exist, the stored proc may error (by design).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Import your publisher (same file we built earlier)
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
    # domain=shared: canonical dimensions (codes + names)
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
    # Templates: key2 = rule_id, value1 = small params JSON string, value2 = SQL template
    # Contract: SQL must return exactly one row with integer fail_count.
    return {
        "domain": "quality_rules",
        "entries": [
            {"key1": "quality", "key2": "not_null",
             "value1": "{\"severity\":\"error\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NULL;",
             "value3": ""},

            {"key1": "quality", "key2": "not_blank",
             "value1": "{\"severity\":\"error\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND ([column_name] IS NULL OR TRIM([column_name]) = '');",
             "value3": ""},

            {"key1": "quality", "key2": "unique",
             "value1": "{\"severity\":\"error\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM (SELECT [column_name], COUNT(*) c FROM [schema].[table] WHERE [where] GROUP BY [column_name] HAVING COUNT(*) > 1) d;",
             "value3": ""},

            {"key1": "quality", "key2": "in_allowed_set",
             "value1": "{\"severity\":\"error\",\"allowed_list\":\"'A','B','C'\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND [column_name] NOT IN ([allowed_list]);",
             "value3": ""},

            {"key1": "quality", "key2": "between_numeric",
             "value1": "{\"severity\":\"error\",\"min\":\"0\",\"max\":\"120\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND ([column_name] < [min] OR [column_name] > [max]);",
             "value3": ""},

            {"key1": "quality", "key2": "regex_match",
             "value1": "{\"severity\":\"warn\",\"pattern\":\"^[A-Z0-9]{10}$\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND [column_name] !~ '[pattern]';",
             "value3": ""},

            {"key1": "quality", "key2": "min_length",
             "value1": "{\"severity\":\"warn\",\"min_len\":\"1\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND LEN([column_name]) < [min_len];",
             "value3": ""},

            {"key1": "quality", "key2": "max_length",
             "value1": "{\"severity\":\"warn\",\"max_len\":\"255\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND LEN([column_name]) > [max_len];",
             "value3": ""},

            {"key1": "quality", "key2": "non_negative",
             "value1": "{\"severity\":\"error\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] WHERE [where] AND [column_name] IS NOT NULL AND [column_name] < 0;",
             "value3": ""},

            {"key1": "quality", "key2": "referential_exists",
             "value1": "{\"severity\":\"error\",\"ref_schema\":\"silver\",\"ref_table\":\"members\",\"ref_column\":\"member_id\"}",
             "value2": "SELECT COUNT(*) AS fail_count FROM [schema].[table] t LEFT JOIN [ref_schema].[ref_table] r ON t.[column_name] = r.[ref_column] WHERE [where] AND t.[column_name] IS NOT NULL AND r.[ref_column] IS NULL;",
             "value3": ""},
        ],
    }


def build_quality_apply() -> Dict[str, Any]:
    # Bind templates to real columns. key2 format: <table>:<column>:<rule_id>
    # value1 contains params JSON string; override defaults as needed.
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

            {"key1": "quality", "key2": "members:email:not_blank",
             "value1": "{\"schema\":\"silver\",\"where\":\"status = 'active'\",\"severity\":\"warn\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "claims:claim_id:unique",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"severity\":\"error\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "claims:member_id:referential_exists",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"ref_schema\":\"silver\",\"ref_table\":\"members\",\"ref_column\":\"member_id\",\"severity\":\"error\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "claims:status:in_allowed_set",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"allowed_list\":\"'OPEN','CLOSED','PENDING'\",\"severity\":\"error\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "claims:amount:non_negative",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"severity\":\"error\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "claims:age:between_numeric",
             "value1": "{\"schema\":\"silver\",\"where\":\"age IS NOT NULL\",\"min\":\"0\",\"max\":\"120\",\"severity\":\"error\"}",
             "value2": "", "value3": ""},

            {"key1": "quality", "key2": "claims:diagnosis_code:max_length",
             "value1": "{\"schema\":\"silver\",\"where\":\"1=1\",\"max_len\":\"10\",\"severity\":\"warn\"}",
             "value2": "", "value3": ""},
        ],
    }


def publish_one(env: CPEnv, path: str, *, upsert: bool, include_disabled: bool = False) -> Dict[str, Any]:
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="demo_configs", help="Directory to write demo JSON files")
    ap.add_argument("--write-only", action="store_true", help="Only write demo JSON files")
    ap.add_argument("--publish", action="store_true", help="Publish demo JSON files to Redshift")
    ap.add_argument("--upsert", action="store_true", help="Publish using upsert mode (recommended for demos)")
    ap.add_argument("--run-quality", action="store_true", help="Run quality checks stored proc after publish")
    ap.add_argument("--schema", default="silver", help="Schema for quality check execution")
    ap.add_argument("--table", default="members", help="Table for quality check execution")
    ap.add_argument("--print-config", action="store_true", help="Print a few rows from control.v_config_active")

    args = ap.parse_args()

    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    paths = {
        "shared_seed": os.path.join(outdir, "shared_seed.json"),
        "quality_rules": os.path.join(outdir, "quality_rules.json"),
        "quality_apply": os.path.join(outdir, "quality_apply.json"),
    }

    write_json(paths["shared_seed"], build_shared_seed())
    write_json(paths["quality_rules"], build_quality_rules())
    write_json(paths["quality_apply"], build_quality_apply())

    print(f"Wrote demo configs to: {os.path.abspath(outdir)}")
    for k, p in paths.items():
        print(f"  - {k}: {p}")

    if args.write_only and not args.publish and not args.run_quality and not args.print_config:
        return

    if args.publish or args.run_quality or args.print_config:
        env = load_env()

    if args.publish:
        print("\nPublishing demo configs... (upsert=%s)\n" % args.upsert)
        r1 = publish_one(env, paths["shared_seed"], upsert=args.upsert)
        r2 = publish_one(env, paths["quality_rules"], upsert=args.upsert)
        r3 = publish_one(env, paths["quality_apply"], upsert=args.upsert)

        print("Publish receipts:")
        print(json.dumps({"shared_seed": r1, "quality_rules": r2, "quality_apply": r3}, indent=2))

    if args.run_quality:
        # Stored proc must exist: control.run_quality_checks_for_table(run_id, run_source, schema, table, fail_on_error)
        run_id = f"demo_{int(time.time())}"
        sql = f"""CALL control.run_quality_checks_for_table(
          '{run_id}',
          'demo',
          '{args.schema}',
          '{args.table}',
          TRUE
        );"""

        import boto3  # local import to keep module import clean
        rsd = boto3.client("redshift-data")
        resp = rsd.execute_statement(
            ClusterIdentifier=env.cluster_id,
            Database=env.database,
            DbUser=env.db_user,
            Sql=sql,
        )
        desc = _wait_for_statement(rsd, resp["Id"], poll_seconds=0.5)
        if desc["Status"] != "FINISHED":
            raise RuntimeError(f"Quality checks failed: {desc}")

        print(f"Quality checks executed. run_id={run_id}")
        print("Query results with:")
        print(f"""  SELECT * FROM control.quality_results WHERE run_id = '{run_id}' ORDER BY executed_at;""")

    if args.print_config:
        rows = get_config(
            cluster_id=env.cluster_id,
            database=env.database,
            db_user=env.db_user,
            schema="control",
            view="v_config_active",
            where_sql="domain IN ('shared','quality_rules','quality_apply')",
            limit=25,
        )
        print("\nSample config rows (control.v_config_active):")
        print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nâ Demo failed: {e}\n", file=sys.stderr)
        raise
