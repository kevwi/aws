# Developer Quick Start

## 1. Install Dependencies

pip install boto3

Ensure AWS credentials are configured.

------------------------------------------------------------------------

## 2. Create JSON Config

Example:

{ "domain": "shared", "entries": \[ { "key1": "metal_code", "key2":
"metal_name", "value1": "1", "value2": "bronze" } \] }

------------------------------------------------------------------------

## 3. Publish Config

python config_publish_json.py publish\
--file shared_seed.json\
--bucket my-bucket\
--prefix config/init\
--cluster my-cluster\
--database dev\
--user my_user\
--iam-role arn:aws:iam::123:role/RedshiftCopyRole

------------------------------------------------------------------------

## 4. Run Quality Checks

CALL control.run_quality_checks_for_table( 'run_001', 'pipeline',
'silver', 'members', TRUE );

------------------------------------------------------------------------

## 5. Review Results

SELECT \* FROM control.quality_results WHERE run_id = 'run_001';

------------------------------------------------------------------------

Summary:

1.  Create JSON.
2.  Publish.
3.  Execute procedure.
4.  Review results.
