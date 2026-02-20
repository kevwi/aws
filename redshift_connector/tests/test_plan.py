
from redshift_package.plan import make_idempotent_copy_sql
def test_plan():
    sqls=make_idempotent_copy_sql("public","t","s3://b/k","arn:role")
    assert any("FORMAT AS PARQUET" in s for s in sqls)
import pytest
from unittest.mock import MagicMock
from executor import run_statement, submit_statement, poll_statement, get_statement_result, RedshiftPermanentError

def make_client_for_success():
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-1"}
    # describe_statement returns RUNNING then FINISHED
    seq = ["RUNNING", "FINISHED"]
    def describe_statement(Id):
        return {"Status": seq.pop(0) if seq else "FINISHED"}
    client.describe_statement.side_effect = describe_statement
    client.get_statement_result.return_value = {
        "ColumnMetadata": [{"name": "cnt"}],
        "Records": [[{"longValue": 5}]]
    }
    return client

def test_submit_and_run_success():
    client = make_client_for_success()
    ok, sid = run_statement(client, "SELECT 1", "db", "user", wait_for_completion=True, timeout_seconds=5, sleep_func=lambda _: None)
    assert ok is True
    assert sid == "stmt-1"
    client.execute_statement.assert_called_once()

def test_run_statement_permanent_failure():
    client = MagicMock()
    client.execute_statement.return_value = {"Id": "stmt-2"}
    # describe_statement returns FAILED with error text
    client.describe_statement.return_value = {"Status": "FAILED", "Error": "syntax error"}
    with pytest.raises(RedshiftPermanentError):
        run_statement(client, "BAD SQL", "db", "user", wait_for_completion=True, timeout_seconds=1, sleep_func=lambda _: None)

def test_get_statement_result_parsing():
    client = make_client_for_success()
    sid = submit_statement(client, "SELECT 1", "db", "user")
    rows = get_statement_result(client, sid)
    assert rows == [{"cnt": 5}]

# tests/test_copy_parquet_to_redshift.py
import os
import uuid
import pytest

from redshift_connector import client_factory, write, executor

@pytest.mark.integration
def test_copy_parquet_to_redshift_live(
    # provide values either by environment variables or by test call
    bucket=os.getenv("TEST_S3_BUCKET"),
    key=os.getenv("TEST_S3_KEY"),
    schema=os.getenv("TEST_REDSHIFT_SCHEMA", "public"),
    table=None,
    iam_role_arn=os.getenv("TEST_REDSHIFT_IAM_ROLE"),
    database=os.getenv("TEST_REDSHIFT_DATABASE"),
    db_user=os.getenv("TEST_REDSHIFT_DB_USER"),
    create_table_sql=os.getenv("TEST_CREATE_TABLE_SQL", None),
):
    """
    Minimal live integration test to COPY a parquet file from S3 into Redshift Serverless and assert row count > 0.

    Required environment variables if you don't pass function args:
      - TEST_S3_BUCKET (bucket name)
      - TEST_S3_KEY    (key, path/to/file.parquet)
      - TEST_REDSHIFT_IAM_ROLE  (IAM role ARN Redshift will assume to read S3)
      - TEST_REDSHIFT_DATABASE  (Redshift Serverless database name)
      - TEST_REDSHIFT_DB_USER   (DB user for Redshift Data API)
    Optional:
      - TEST_CREATE_TABLE_SQL: SQL string to create the target table prior to COPY (if needed)
    """
    # sanity checks
    assert bucket, "Provide TEST_S3_BUCKET"
    assert key, "Provide TEST_S3_KEY"
    assert iam_role_arn, "Provide TEST_REDSHIFT_IAM_ROLE"
    assert database, "Provide TEST_REDSHIFT_DATABASE"
    assert db_user, "Provide TEST_REDSHIFT_DB_USER"

    # Make a non-colliding table name if not supplied
    if not table:
        table = f"test_upload_{uuid.uuid4().hex[:8]}"

    client = client_factory.get_redshift_data_client()

    # If user provided a create-table SQL, run it first (optional)
    if create_table_sql:
        # Replace placeholder {schema}.{table} if present
        create_sql = create_table_sql.format(schema=schema, table=table)
        ok, sid = write.run_statement(client, create_sql, database, db_user, wait_for_completion=True)
        # optionally inspect get_statement_result if needed, but assume create succeeded if no exception

    s3_path = f"s3://{bucket}/{key}"
    copy_sql = f"COPY {schema}.{table} FROM '{s3_path}' IAM_ROLE '{iam_role_arn}' FORMAT AS PARQUET;"

    # Execute the COPY (this will run on Redshift Serverless)
    ok, copy_sid = write.run_statement(client, copy_sql, database, db_user, wait_for_completion=True)

    # Verify by running a COUNT query
    ok, cnt_sid = write.run_statement(client, f"SELECT COUNT(*) as cnt FROM {schema}.{table};", database, db_user, wait_for_completion=True)
    rows = executor.get_statement_result(client, cnt_sid)
    assert rows and len(rows) > 0, "Count query returned no rows"
    cnt = int(list(rows[0].values())[0])
    assert cnt > 0, f"Expected >0 rows after COPY, got {cnt}"

"""
rows = execute_sql_redshift_serverless(
    workgroup="my-wg",
    database="analytics",
    sql="SELECT COUNT(*) AS cnt FROM public.events;",
    secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:my-redshift-secret",
    db_user="etl_user",
    region="us-west-2",
)
print(rows)
"""

import boto3
import time
from botocore.config import Config


def run_redshift_query(
    sql: str,
    workgroup_name: str,
    database: str,
    secret_arn: str,
    region: str = "us-west-2"
):
    """
    Execute a query against Redshift Serverless and return results.
    SSL verification disabled (for Zscaler environments).
    """

    # Disable SSL validation
    session = boto3.session.Session()
    client = session.client(
        "redshift-data",
        region_name=region,
        verify=False,  # <---- the magic line
        config=Config(retries={"max_attempts": 3})
    )

    response = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        SecretArn=secret_arn,
        Sql=sql
    )

    statement_id = response["Id"]
    print("Statement ID:", statement_id)

    # Wait for completion
    while True:
        desc = client.describe_statement(Id=statement_id)
        status = desc["Status"]

        if status in ["FAILED", "ABORTED"]:
            raise Exception(desc.get("Error", "Query failed"))

        if status == "FINISHED":
            break

        time.sleep(1)

    # Fetch results
    if desc.get("HasResultSet", False):
        results = client.get_statement_result(Id=statement_id)

        columns = [col["name"] for col in results["ColumnMetadata"]]
        rows = []

        for record in results["Records"]:
            row = []
            for field in record:
                row.append(list(field.values())[0] if field else None)
            rows.append(dict(zip(columns, row)))

        return rows

    return "Query executed successfully (no result set)"

import boto3
import pandas as pd
from io import BytesIO
import time
from botocore.exceptions import ClientError

def s3_parquet_copy_and_pipecsv(
    source_bucket: str,
    source_key: str,
    target_bucket: str = None,
    prefix_out: str = "converted/",
    region: str = None,
    verify_ssl: bool = False,
    max_retries: int = 3,
    retry_backoff_seconds: float = 1.0
):
    """
    1) Copies parquet unchanged from source_key -> <prefix_out>/<filename>_copy.parquet
    2) Reads source parquet into memory
    3) Writes pipe-delimited CSV to <prefix_out>/<filename>.csv

    NOTE: verify_ssl=False disables SSL cert validation (for Zscaler). Use only for testing.
    """

    target_bucket = target_bucket or source_bucket

    session = boto3.session.Session()
    s3 = session.client("s3", region_name=region, verify=verify_ssl)

    base_name = source_key.split("/")[-1]
    if base_name.lower().endswith(".parquet"):
        copy_key = f"{prefix_out}{base_name[:-8]}_copy.parquet"
        csv_key = f"{prefix_out}{base_name[:-8]}.csv"
    else:
        copy_key = f"{prefix_out}{base_name}_copy.parquet"
        csv_key = f"{prefix_out}{base_name}.csv"

    # ---------- 1) COPY OBJECT ----------
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[copy] attempt {attempt}: copying s3://{source_bucket}/{source_key} -> s3://{target_bucket}/{copy_key}")
            s3.copy_object(
                Bucket=target_bucket,
                Key=copy_key,
                CopySource={"Bucket": source_bucket, "Key": source_key},
            )
            print(f"[copy] success: s3://{target_bucket}/{copy_key}")
            last_exc = None
            break
        except ClientError as e:
            last_exc = e
            print(f"[copy] attempt {attempt} failed: {e}")
            time.sleep(retry_backoff_seconds * attempt)
    if last_exc:
        raise last_exc

    # ---------- 2) DOWNLOAD PARQUET ----------
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[download] attempt {attempt}: downloading s3://{source_bucket}/{source_key}")
            obj = s3.get_object(Bucket=source_bucket, Key=source_key)
            parquet_bytes = obj["Body"].read()
            print("[download] success")
            last_exc = None
            break
        except ClientError as e:
            last_exc = e
            print(f"[download] attempt {attempt} failed: {e}")
            time.sleep(retry_backoff_seconds * attempt)
    if last_exc:
        raise last_exc

    # ---------- 3) READ PARQUET INTO PANDAS ----------
    try:
        df = pd.read_parquet(BytesIO(parquet_bytes))
        print(f"[read] loaded dataframe: rows={df.shape[0]} cols={df.shape[1]}")
    except Exception as e:
        raise RuntimeError(f"Failed to read parquet: {e}")

    # ---------- 4) WRITE PIPE CSV TO S3 ----------
    csv_buffer = BytesIO()
    try:
        df.to_csv(csv_buffer, sep="|", index=False)
        csv_body = csv_buffer.getvalue()
    except Exception as e:
        raise RuntimeError(f"Failed to convert to CSV: {e}")

    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[upload] attempt {attempt}: uploading CSV to s3://{target_bucket}/{csv_key}")
            s3.put_object(
                Bucket=target_bucket,
                Key=csv_key,
                Body=csv_body,
                ContentType="text/csv"
            )
            print(f"[upload] success: s3://{target_bucket}/{csv_key}")
            last_exc = None
            break
        except ClientError as e:
            last_exc = e
            print(f"[upload] attempt {attempt} failed: {e}")
            time.sleep(retry_backoff_seconds * attempt)
    if last_exc:
        raise last_exc

    return {
        "copied_parquet": f"s3://{target_bucket}/{copy_key}",
        "pipe_csv": f"s3://{target_bucket}/{csv_key}",
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1])
    }


"""
If this works:

Layer	Proven
IAM credentials	✔
S3 read	✔
S3 write	✔
Python environment	✔
Parquet compatibility	✔
Data usable for COPY	✔
"""

if __name__ == "__main__":
    res = s3_parquet_copy_and_pipecsv(
        source_bucket="my-bucket",
        source_key="incoming/sample.parquet",
        target_bucket="my-bucket",
        prefix_out="testing/",
        region="us-west-2",
        verify_ssl=False
    )
    print("Result:", res)
