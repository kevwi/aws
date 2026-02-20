
from redshift_package.plan import make_idempotent_copy_sql
def test_plan():
    sqls=make_idempotent_copy_sql("public","t","s3://b/k","arn:role")
    assert any("FORMAT AS PARQUET" in s for s in sqls)
