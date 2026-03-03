"""
config_publish_json.py

JSON-only config publishing to Redshift via JSON Lines (NDJSON) + stored procedure.

Why NDJSON?
- Easy for humans + machines
- Redshift COPY supports JSON 'auto'
- One object per line keeps COPY simple and scalable

Input formats supported:
1) JSON file containing either:
   A) {"domain": "pipeline", "entries": [ {...}, {...} ]}                       (single-domain)
   B) {"domains": [ {"domain": "pipeline", "entries":[...]}, {...} ]}          (multi-domain list)
   C) {"domains": { "pipeline": [ {...}, {...} ], "aca":[ {...} ] }}           (multi-domain mapping)
   D) [ {"domain":"pipeline","entries":[...]}, {"domain":"aca","entries":[...]} ] (top-level list)

Each entry supports:
  key1 (required), key2 (optional), value1 (required), value2 (optional), disabled (optional)

Normalization (in Python):
  - domain/key1/key2: trim + lowercase
  - value1/value2: trim (case preserved)
Guardrails:
  - required: domain, key1, value1 must be present after normalization
  - reject '|' in any string field (optional sanity; not required for JSON COPY, but keeps data clean)

Redshift-side required primitive:
  Stored procedure:
    CALL control.set_config_from_s3_json(
      p_s3_path,
      p_iam_role_arn,
      p_upsert,
      p_input_has_disabled,
      p_apply_soft_delete
    );

This module provides:
  - publish_config_json(...)  (main entry point)
  - render_json_to_jsonl(...) (transform JSON -> NDJSON)
  - call_set_config_from_s3_json(...)
  - get_config(...) (optional read helper via normalized views)

Dependencies:
  pip install boto3
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import boto3


# ----------------------------
# Redshift Data API helpers
# ----------------------------

def _wait_for_statement(rsd, statement_id: str, poll_seconds: float = 0.5) -> Dict[str, Any]:
    while True:
        desc = rsd.describe_statement(Id=statement_id)
        status = desc.get("Status")
        if status in ("FINISHED", "FAILED", "ABORTED"):
            return desc
        time.sleep(poll_seconds)


def _records_to_dicts(result: Dict[str, Any]) -> List[Dict[str, Any]]:
    cols = [c["name"] for c in result.get("ColumnMetadata", [])]
    rows: List[Dict[str, Any]] = []
    for rec in result.get("Records", []):
        row = {}
        for col, val in zip(cols, rec):
            row[col] = (list(val.values())[0] if val else None)
        rows.append(row)
    return rows


def get_config(
    *,
    cluster_id: str,
    database: str,
    db_user: str,
    schema: str = "control",
    view: str = "v_config_active",  # or "v_config"
    where_sql: Optional[str] = None,
    limit: Optional[int] = None,
    poll_seconds: float = 0.5,
    rsd_client=None,
) -> List[Dict[str, Any]]:
    """
    Optional read helper: SELECT from normalized view (e.g. control.v_config_active).
    """
    rsd = rsd_client or boto3.client("redshift-data")

    sql = f"SELECT * FROM {schema}.{view}"
    if where_sql:
        sql += f" WHERE {where_sql}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    sql += ";"

    resp = rsd.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=sql,
    )
    desc = _wait_for_statement(rsd, resp["Id"], poll_seconds=poll_seconds)
    if desc["Status"] != "FINISHED":
        raise RuntimeError(f"get_config failed: {desc}")

    result = rsd.get_statement_result(Id=resp["Id"])
    return _records_to_dicts(result)


def call_set_config_from_s3_json(
    *,
    cluster_id: str,
    database: str,
    db_user: str,
    s3_path: str,
    iam_role_arn: str,
    upsert: bool = False,
    input_has_disabled: bool = False,
    apply_soft_delete: bool = True,
    proc_schema: str = "control",
    proc_name: str = "set_config_from_s3_json",
    poll_seconds: float = 0.5,
    rsd_client=None,
) -> Dict[str, Any]:
    """
    Calls the Redshift stored proc that performs:
      COPY JSON -> stage -> normalize -> validate -> insert/upsert (+ soft-delete)
    """
    rsd = rsd_client or boto3.client("redshift-data")

    sql = f"""
    CALL {proc_schema}.{proc_name}(
      '{s3_path}',
      '{iam_role_arn}',
      {str(bool(upsert)).lower()},
      {str(bool(input_has_disabled)).lower()},
      {str(bool(apply_soft_delete)).lower()}
    );
    """

    resp = rsd.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=sql,
    )
    statement_id = resp["Id"]
    desc = _wait_for_statement(rsd, statement_id, poll_seconds=poll_seconds)
    if desc["Status"] != "FINISHED":
        raise RuntimeError(f"Stored proc failed: {desc}")

    return {"status": "success", "statement_id": statement_id}


# ----------------------------
# JSON parsing (multi-domain)
# ----------------------------

@dataclass(frozen=True)
class DomainDoc:
    domain: str
    entries: List[Dict[str, Any]]


def _normalize_domain(s: Any) -> str:
    d = "" if s is None else str(s).strip()
    if not d:
        raise ValueError("domain cannot be empty")
    return d


def _parse_json_multi_domain(obj: Any) -> List[DomainDoc]:
    """
    Accepted JSON shapes:

    A) Single domain:
      {"domain":"pipeline","entries":[{...},{...}]}

    B) Multi domain list:
      {"domains":[{"domain":"pipeline","entries":[...]},{"domain":"aca","entries":[...]}]}

    C) Multi domain mapping:
      {"domains":{"pipeline":[{...},{...}],"aca":[{...}]}}

    D) Top-level list:
      [{"domain":"pipeline","entries":[...]},{"domain":"aca","entries":[...]}]
    """
    docs: List[DomainDoc] = []

    if isinstance(obj, dict):
        if "domains" in obj:
            doms = obj["domains"]

            if isinstance(doms, list):
                for item in doms:
                    if not isinstance(item, dict):
                        raise ValueError("Each item in domains list must be an object with domain + entries")
                    domain = _normalize_domain(item.get("domain"))
                    entries = item.get("entries")
                    if not isinstance(entries, list):
                        raise ValueError(f"'entries' must be a list for domain={domain}")
                    docs.append(DomainDoc(domain=domain, entries=entries))
                return docs

            if isinstance(doms, dict):
                for domain_key, entries in doms.items():
                    domain = _normalize_domain(domain_key)
                    if not isinstance(entries, list):
                        raise ValueError(f"'domains.{domain}' must be a list of entries")
                    docs.append(DomainDoc(domain=domain, entries=entries))
                return docs

            raise ValueError("'domains' must be a list or mapping")

        if "domain" in obj:
            domain = _normalize_domain(obj.get("domain"))
            entries = obj.get("entries")
            if not isinstance(entries, list):
                raise ValueError("'entries' must be a list for single-domain JSON")
            docs.append(DomainDoc(domain=domain, entries=entries))
            return docs

        raise ValueError("JSON object must include either 'domain' or 'domains'.")

    if isinstance(obj, list):
        for item in obj:
            if not isinstance(item, dict):
                raise ValueError("Top-level JSON list items must be objects with domain + entries")
            domain = _normalize_domain(item.get("domain"))
            entries = item.get("entries")
            if not isinstance(entries, list):
                raise ValueError(f"'entries' must be a list for domain={domain}")
            docs.append(DomainDoc(domain=domain, entries=entries))
        return docs

    raise ValueError("Unsupported JSON structure")


# ----------------------------
# Guardrails + NDJSON rendering
# ----------------------------

def _norm_lower_trim(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


def _norm_trim(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def render_json_to_jsonl(
    json_path: str,
    *,
    include_disabled: bool = False,
    enforce_required: bool = True,
    normalize_in_python: bool = True,
    forbid_pipe_chars: bool = True,
) -> Tuple[str, List[str]]:
    """
    JSON -> NDJSON (JSON Lines), one object per line, COPY JSON 'auto' friendly.

    Each line includes:
      domain, key1, key2, value1, value2
    Optional:
      disabled

    Required field guardrail:
      domain, key1, value1 must be present after normalization.

    Returns: (jsonl_text, domains)
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    docs = _parse_json_multi_domain(data)

    domains: List[str] = []
    out_lines: List[str] = []

    for doc in docs:
        domains.append(doc.domain)

        for idx, entry in enumerate(doc.entries, start=1):
            if not isinstance(entry, dict):
                raise ValueError(f"Entry {idx} in domain '{doc.domain}' must be an object")

            if normalize_in_python:
                domain = _norm_lower_trim(doc.domain)
                key1 = _norm_lower_trim(entry.get("key1"))
                key2 = _norm_lower_trim(entry.get("key2"))
                value1 = _norm_trim(entry.get("value1"))
                value2 = _norm_trim(entry.get("value2"))
            else:
                domain = str(doc.domain)
                key1 = entry.get("key1")
                key2 = entry.get("key2")
                value1 = entry.get("value1")
                value2 = entry.get("value2")

            if enforce_required:
                if not domain:
                    raise ValueError(f"Domain is empty after normalization for doc: {doc.domain!r}")
                if not key1:
                    raise ValueError(f"Entry {idx} in domain '{doc.domain}': key1 is required")
                if not value1:
                    raise ValueError(f"Entry {idx} in domain '{doc.domain}': value1 is required")

            obj: Dict[str, Any] = {
                "domain": domain,
                "key1": key1,
                "key2": key2,
                "value1": value1,
                "value2": value2,
            }

            if include_disabled:
                # can be bool, int, or string; stored proc parses as text
                obj["disabled"] = entry.get("disabled")

            if forbid_pipe_chars:
                for k, v in obj.items():
                    if isinstance(v, str) and "|" in v:
                        raise ValueError(
                            f"Entry {idx} in domain '{doc.domain}': field '{k}' contains '|', "
                            "which is disallowed in this config system."
                        )

            out_lines.append(json.dumps(obj, ensure_ascii=False))

    return "\n".join(out_lines) + "\n", domains


# ----------------------------
# S3 upload + publish
# ----------------------------

def upload_text_to_s3(
    *,
    bucket: str,
    key: str,
    text: str,
    content_type: str = "application/x-ndjson",
    s3_client=None,
) -> str:
    s3 = s3_client or boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )
    return f"s3://{bucket}/{key}"


def publish_config_json(
    *,
    json_path: str,
    bucket: str,
    s3_prefix: str,  # e.g. "config/dev/my_service"
    cluster_id: str,
    database: str,
    db_user: str,
    iam_role_arn: str,
    upsert: bool = False,
    include_disabled: bool = False,
    apply_soft_delete: bool = True,
    proc_schema: str = "control",
    proc_name: str = "set_config_from_s3_json",
    poll_seconds: float = 0.5,
    s3_client=None,
    rsd_client=None,
) -> Dict[str, Any]:
    """
    Main entry point.

    - Reads JSON file (single/multi-domain shapes supported)
    - Normalizes + validates
    - Renders NDJSON
    - Uploads to S3
    - Calls Redshift stored proc to apply

    Returns a receipt with S3 path + statement id + included domains.
    """
    jsonl_text, domains = render_json_to_jsonl(
        json_path,
        include_disabled=include_disabled,
        enforce_required=True,
        normalize_in_python=True,
        forbid_pipe_chars=True,
    )

    base = os.path.basename(json_path)
    if base.endswith(".json"):
        base = base.rsplit(".", 1)[0] + ".jsonl"
    else:
        base = base + ".jsonl"

    key = f"{s3_prefix.rstrip('/')}/{base}"
    s3_path = upload_text_to_s3(bucket=bucket, key=key, text=jsonl_text, s3_client=s3_client)

    call_result = call_set_config_from_s3_json(
        cluster_id=cluster_id,
        database=database,
        db_user=db_user,
        s3_path=s3_path,
        iam_role_arn=iam_role_arn,
        upsert=upsert,
        input_has_disabled=include_disabled,
        apply_soft_delete=apply_soft_delete,
        proc_schema=proc_schema,
        proc_name=proc_name,
        poll_seconds=poll_seconds,
        rsd_client=rsd_client,
    )

    return {
        "status": "success",
        "json_path": json_path,
        "domains": domains,
        "s3_path": s3_path,
        "statement_id": call_result["statement_id"],
        "upsert": upsert,
        "include_disabled": include_disabled,
        "apply_soft_delete": apply_soft_delete,
        "s3_bucket": bucket,
        "s3_key": key,
    }


# ----------------------------
# Examples (for devs)
# ----------------------------

JSON_EXAMPLE_SINGLE_DOMAIN = {
    "domain": "pipeline",
    "entries": [
        {"key1": "jobid", "key2": "alloc", "value1": "next", "value2": "12345"},
        {"key1": "retry", "key2": "policy", "value1": "exponential", "value2": "true"},
    ],
}

JSON_EXAMPLE_MULTI_DOMAIN_MAPPING = {
    "domains": {
        "pipeline": [
            {"key1": "jobid", "key2": "alloc", "value1": "next", "value2": "12345"},
        ],
        "aca": [
            {"key1": "rule", "key2": "eligibility", "value1": "min_age", "value2": "18"},
        ],
    }
}

JSON_EXAMPLE_WITH_DISABLED = {
    "domains": {
        "pipeline": [
            {"key1": "old_feature", "key2": "flag", "value1": "deprecated", "value2": "true", "disabled": True},
        ]
    }
}

if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Publish JSON config to Redshift via stored procedure."
    )

    subparsers = parser.add_subparsers(dest="command")

    publish_parser = subparsers.add_parser("publish", help="Publish a local JSON config file")

    publish_parser.add_argument("--file", required=True, help="Path to local JSON config file")
    publish_parser.add_argument("--bucket", required=True, help="S3 bucket")
    publish_parser.add_argument("--prefix", required=True, help="S3 key prefix (e.g. config/dev/service)")
    publish_parser.add_argument("--cluster", required=True, help="Redshift cluster identifier")
    publish_parser.add_argument("--database", required=True, help="Redshift database")
    publish_parser.add_argument("--user", required=True, help="Redshift database user")
    publish_parser.add_argument("--iam-role", required=True, help="IAM role ARN for COPY")

    publish_parser.add_argument("--upsert", action="store_true", help="Enable upsert mode")
    publish_parser.add_argument("--include-disabled", action="store_true", help="Expect 'disabled' field in JSON")
    publish_parser.add_argument(
        "--no-soft-delete",
        action="store_true",
        help="Ignore disabled field even if present"
    )

    args = parser.parse_args()

    if args.command != "publish":
        parser.print_help()
        sys.exit(1)

    try:
        receipt = publish_local_json(
            json_path=args.file,
            bucket=args.bucket,
            s3_prefix=args.prefix,
            cluster_id=args.cluster,
            database=args.database,
            db_user=args.user,
            iam_role_arn=args.iam_role,
            upsert=args.upsert,
            include_disabled=args.include_disabled,
            apply_soft_delete=not args.no_soft_delete,
        )

        print("\n✅ Publish successful\n")
        print(json.dumps(receipt, indent=2))

    except Exception as e:
        print("\n❌ Publish failed\n")
        print(str(e))
        sys.exit(2)

def _wait_for_result(rsd, statement_id):
    while True:
        desc = rsd.describe_statement(Id=statement_id)
        if desc["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            if desc["Status"] != "FINISHED":
                raise RuntimeError(desc)
            return
        time.sleep(0.3)


def alloc_jobid(cluster_id, database, db_user):
    rsd = boto3.client("redshift-data")

    resp = rsd.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql="SELECT control.alloc_jobid_pipeline();"
    )

    _wait_for_result(rsd, resp["Id"])
    result = rsd.get_statement_result(Id=resp["Id"])

    return int(list(result["Records"][0][0].values())[0])


def log_pipeline_event(cluster_id, database, db_user,
                       domain, app, status, metal, location,
                       jobid=None):

    rsd = boto3.client("redshift-data")

    jobid_sql = "NULL" if jobid is None else str(jobid)

    sql = f"""
    CALL control.sp_log_pipeline_event_smart(
      '{domain}',
      '{app}',
      '{status}',
      '{metal}',
      '{location}',
      {jobid_sql},
      NULL,
      NULL
    );
    """

    rsd.execute_statement(
        ClusterIdentifier=cluster_id,
        Database=database,
        DbUser=db_user,
        Sql=sql,
    )
"""
bash:
python config_publish_json.py publish \
  --file config.json \
  --bucket my-bucket \
  --prefix config/dev/my_service \
  --cluster my-redshift-cluster \
  --database dev \
  --user andie \
  --iam-role arn:aws:iam::123:role/RedshiftCopyRole
"""
