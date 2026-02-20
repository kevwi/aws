
from typing import Any, Callable, Optional
from . import extract, plan, write

def run_idempotent_copy(s3_client: Any, client: Any, schema: str, table: str,
                        bucket: str, key: str, iam_role_arn: str, database: str, db_user: str,
                        column_defs_sql: Optional[str]=None,
                        sanity_check: Optional[Callable[[int],bool]]=None,
                        sleep_func=lambda _: None,
                        on_retry=None, timeout_seconds:int=300)->None:
    if not extract.s3_object_exists(s3_client,bucket,key):
        raise ValueError("missing s3 object")
    sqls=plan.make_idempotent_copy_sql(schema,table,f"s3://{bucket}/{key}",iam_role_arn,column_defs_sql)
    for i,sql in enumerate(sqls):
        ok,sid=write.run_statement(client,sql,database,db_user,sleep_func=sleep_func,on_retry=on_retry)
        if i==2:
            rows=write.fetch_statement_result(client,sid,sleep_func=sleep_func)
            count=int(list(rows[0].values())[0]) if rows else 0
            check=sanity_check or (lambda r:r>=1)
            if not check(count):
                write.run_statement(client,f"DROP TABLE IF EXISTS {schema}.tmp_cleanup;",database,db_user)
                raise write.RedshiftError("sanity check failed")
