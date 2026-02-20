
from typing import List, Optional
import uuid

def make_idempotent_copy_sql(schema: str, table: str, s3_parquet_path: str,
                             iam_role_arn: str, column_defs_sql: Optional[str] = None,
                             tmp_table_suffix: Optional[str] = None) -> List[str]:
    """Build ordered SQL for tmp CREATE -> COPY -> SANITY -> DROP -> RENAME."""
    suffix = tmp_table_suffix or uuid.uuid4().hex[:8]
    tmp = f"tmp__{table}__{suffix}"
    create = (f"CREATE TABLE {schema}.{tmp} ({column_defs_sql});"
              if column_defs_sql else
              f"CREATE TABLE {schema}.{tmp} (LIKE {schema}.{table});")
    copy = f"""COPY {schema}.{tmp}
FROM '{s3_parquet_path}'
IAM_ROLE '{iam_role_arn}'
FORMAT AS PARQUET;"""
    sanity = f"SELECT COUNT(*) FROM {schema}.{tmp};"
    drop = f"DROP TABLE IF EXISTS {schema}.{table};"
    rename = f"ALTER TABLE {schema}.{tmp} RENAME TO {table};"
    return [create, copy, sanity, drop, rename]
