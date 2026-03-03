import time
from typing import Any, Dict, List, Optional

import boto3


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
            # Data API returns each cell as {"stringValue": "..."} / {"longValue": 1} etc.
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
    Read config via a normalized view created in DDL (e.g., control.v_config_active).
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


def set_config_from_s3(
    *,
    cluster_id: str,
    database: str,
    db_user: str,
    s3_path: str,
    iam_role_arn: str,
    upsert: bool = False,
    ignoreheader: int = 1,
    input_has_disabled: bool = False,
    apply_soft_delete: bool = True,
    proc_schema: str = "control",
    proc_name: str = "set_config_from_s3",
    poll_seconds: float = 0.5,
    rsd_client=None,
) -> Dict[str, Any]:
    """
    Calls the stored procedure created in DDL:

      CALL control.set_config_from_s3(
        p_s3_path,
        p_iam_role_arn,
        p_upsert,
        p_ignoreheader,
        p_input_has_disabled,
        p_apply_soft_delete
      );

    Returns statement metadata if successful.
    """
    rsd = rsd_client or boto3.client("redshift-data")

    # Keep SQL simple and explicit
    sql = f"""
    CALL {proc_schema}.{proc_name}(
      '{s3_path}',
      '{iam_role_arn}',
      {str(bool(upsert)).lower()},
      {int(ignoreheader)},
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
    desc = _wait_for_statement(rsd, resp["Id"], poll_seconds=poll_seconds)
    if desc["Status"] != "FINISHED":
        # If the proc raised an exception, Redshift Data API usually includes it in Error/QueryString
        raise RuntimeError(f"set_config_from_s3 failed: {desc}")

    return {
        "status": "success",
        "statement_id": resp["Id"],
        "upsert": upsert,
        "s3_path": s3_path,
        "input_has_disabled": input_has_disabled,
        "apply_soft_delete": apply_soft_delete,
    }
