import boto3
import time
import uuid
from typing import Any, Dict, List, Optional


class ConfigRepository:
    """
    Minimal config loader for Redshift config table (defaults aligned with your DDL).

    Expected default input TSV (pipe-delimited, header row):
      domain|key1|key2|value1|value2

    Optional (still simple) soft-delete support:
      domain|key1|key2|value1|value2|disabled

    Behavior:
      - Normalizes whitespace + lowercases: domain, key1, key2 (post-COPY in staging)
      - Required-field guardrail: domain, key1, value1 must be non-null/non-blank after normalization
      - Composite key checks: (domain, key1)
      - upsert=False (default): fails if any staged (domain,key1) already exist in target
      - upsert=True: updates existing + inserts missing
      - soft-delete: if input includes disabled column, it updates/inserts disabled accordingly

    Notes:
      - We do NOT pass/require id/created/modified; defaults handle those.
      - When upserting, modified is bumped to GETDATE().
    """

    # Minimal required target schema (types/lengths based on your DDL)
    REQUIRED_TARGET_COLUMNS = {
        "domain": ("character varying", 255),
        "key1": ("character varying", 50),
        "key2": ("character varying", 50),
        "value1": ("character varying", 255),
        "value2": ("character varying", 255),
        "disabled": ("boolean", None),  # needed for soft-delete behavior
    }

    def __init__(
        self,
        cluster_id: str,
        database: str,
        db_user: str,
        schema: str = "public",
        table: str = "config_lookup",
        rsd_client: Optional[Any] = None,
        poll_seconds: float = 0.5,
    ):
        self.cluster_id = cluster_id
        self.database = database
        self.db_user = db_user
        self.schema = schema
        self.table = table
        self.poll_seconds = poll_seconds
        self.rsd = rsd_client or boto3.client("redshift-data")

    # ---------- Public API ----------

    def get_config(
        self,
        where_sql: Optional[str] = None,
        limit: Optional[int] = None,
        include_disabled: bool = True,
    ) -> List[Dict[str, Any]]:
        cols = "domain, key1, key2, value1, value2" + (", disabled" if include_disabled else "")
        sql = f"SELECT {cols} FROM {self.schema}.{self.table}"
        if where_sql:
            sql += f" WHERE {where_sql}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        sql += ";"
        return self._query(sql)

    def validate_target_schema(self) -> Dict[str, Any]:
        """
        Validates target has required columns with expected data_type and max length.
        Ignores extra columns (key3/value3/etc).
        """
        actual = self._query(f"""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = '{self.schema}'
              AND table_name   = '{self.table}';
        """)

        actual_by = {r["column_name"].lower(): r for r in actual}

        missing = []
        mismatches = []

        for col, (exp_type, exp_len) in self.REQUIRED_TARGET_COLUMNS.items():
            a = actual_by.get(col)
            if not a:
                missing.append(col)
                continue

            a_type = (a["data_type"] or "").lower().strip()
            if a_type != exp_type:
                mismatches.append(f"{col}: type expected '{exp_type}' got '{a_type}'")

            if exp_len is not None:
                a_len = a.get("character_maximum_length")
                if a_len is None or int(a_len) != int(exp_len):
                    mismatches.append(f"{col}: length expected {exp_len} got {a_len}")

        ok = (not missing) and (not mismatches)
        detail = {"ok": ok, "missing": missing, "mismatches": mismatches}
        if not ok:
            raise ValueError(f"Target schema validation failed for {self.schema}.{self.table}: {detail}")
        return detail

    def set_config(
        self,
        s3_path: str,
        iam_role_arn: str,
        *,
        upsert: bool = False,
        validate_schema: bool = True,
        ignore_header: int = 1,
        input_has_disabled: bool = False,
        # If True, rows with disabled=true will be applied (soft-delete). If False, disabled is ignored even if present.
        apply_soft_delete: bool = True,
    ) -> Dict[str, Any]:
        """
        Load TSV from S3 to TEMP stage, normalize, validate, then insert/upsert.

        Default input columns:
          domain|key1|key2|value1|value2

        Optional last column:
          ...|disabled  (accepted when input_has_disabled=True)

        Composite key:
          (domain, key1)

        Default upsert=False: fail if any (domain,key1) already exist.
        """

        if validate_schema:
            self.validate_target_schema()

        stage = self._ephemeral_stage_table_name()

        # TEMP stage table
        if input_has_disabled:
            create_stage_sql = f"""
            CREATE TEMP TABLE {stage} (
              domain       VARCHAR(255),
              key1         VARCHAR(50),
              key2         VARCHAR(50),
              value1       VARCHAR(255),
              value2       VARCHAR(255),
              disabled_raw VARCHAR(16)
            );
            """
            copy_cols = "(domain, key1, key2, value1, value2, disabled_raw)"
        else:
            create_stage_sql = f"""
            CREATE TEMP TABLE {stage} (
              domain  VARCHAR(255),
              key1    VARCHAR(50),
              key2    VARCHAR(50),
              value1  VARCHAR(255),
              value2  VARCHAR(255)
            );
            """
            copy_cols = "(domain, key1, key2, value1, value2)"

        copy_sql = f"""
        COPY {stage} {copy_cols}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role_arn}'
        DELIMITER '|'
        IGNOREHEADER {int(ignore_header)}
        EMPTYASNULL
        BLANKSASNULL
        TRIMBLANKS
        ;
        """

        # Normalize whitespace + lowercase for domain/keys
        # - domain/key1/key2: lower(trim()) and blank -> NULL
        # - value1/value2: trim and blank -> NULL (keeps case as-is; you didn't ask to lowercase values)
        normalize_sql = f"""
        UPDATE {stage}
        SET
          domain = NULLIF(LOWER(TRIM(domain)), ''),
          key1   = NULLIF(LOWER(TRIM(key1)), ''),
          key2   = NULLIF(LOWER(TRIM(key2)), ''),
          value1 = NULLIF(TRIM(value1), ''),
          value2 = NULLIF(TRIM(value2), '')
        ;
        """

        # If disabled is supplied, normalize it too (trim+lower)
        normalize_disabled_sql = ""
        if input_has_disabled:
            normalize_disabled_sql = f"""
            UPDATE {stage}
            SET disabled_raw = NULLIF(LOWER(TRIM(disabled_raw)), '')
            ;
            """

        # Validations
        stage_count_sql = f"SELECT COUNT(*) AS cnt FROM {stage};"

        stage_dupes_sql = f"""
        SELECT domain, key1, COUNT(*) AS cnt
        FROM {stage}
        GROUP BY domain, key1
        HAVING COUNT(*) > 1
        LIMIT 50;
        """

        # Required field guardrail (after normalization)
        required_fields_sql = f"""
        SELECT domain, key1, value1
        FROM {stage}
        WHERE domain IS NULL
           OR key1  IS NULL
           OR value1 IS NULL
        LIMIT 50;
        """

        # Conflicts (strict mode)
        conflicts_sql = f"""
        SELECT s.domain, s.key1
        FROM {stage} s
        JOIN {self.schema}.{self.table} t
          ON t.domain = s.domain
         AND t.key1  = s.key1
        LIMIT 50;
        """

        # Disabled mapping expression (only if provided & applied)
        # Accepts: true/false, t/f, 1/0, yes/no, y/n, on/off
        disabled_expr = (
            "CASE "
            "WHEN s.disabled_raw IN ('true','t','1','yes','y','on') THEN TRUE "
            "WHEN s.disabled_raw IN ('false','f','0','no','n','off') THEN FALSE "
            "ELSE t.disabled "
            "END"
        )

        # Apply statements
        if upsert:
            if input_has_disabled and apply_soft_delete:
                update_sql = f"""
                UPDATE {self.schema}.{self.table} t
                SET
                  key2     = s.key2,
                  value1   = s.value1,
                  value2   = s.value2,
                  disabled = {disabled_expr},
                  modified = GETDATE()
                FROM {stage} s
                WHERE t.domain = s.domain
                  AND t.key1  = s.key1;
                """

                insert_sql = f"""
                INSERT INTO {self.schema}.{self.table} (domain, key1, key2, value1, value2, disabled)
                SELECT
                  s.domain, s.key1, s.key2, s.value1, s.value2,
                  CASE
                    WHEN s.disabled_raw IN ('true','t','1','yes','y','on') THEN TRUE
                    WHEN s.disabled_raw IN ('false','f','0','no','n','off') THEN FALSE
                    ELSE FALSE
                  END AS disabled
                FROM {stage} s
                LEFT JOIN {self.schema}.{self.table} t
                  ON t.domain = s.domain
                 AND t.key1  = s.key1
                WHERE t.domain IS NULL;
                """
            else:
                update_sql = f"""
                UPDATE {self.schema}.{self.table} t
                SET
                  key2     = s.key2,
                  value1   = s.value1,
                  value2   = s.value2,
                  modified = GETDATE()
                FROM {stage} s
                WHERE t.domain = s.domain
                  AND t.key1  = s.key1;
                """

                insert_sql = f"""
                INSERT INTO {self.schema}.{self.table} (domain, key1, key2, value1, value2)
                SELECT s.domain, s.key1, s.key2, s.value1, s.value2
                FROM {stage} s
                LEFT JOIN {self.schema}.{self.table} t
                  ON t.domain = s.domain
                 AND t.key1  = s.key1
                WHERE t.domain IS NULL;
                """
            apply_sqls = [update_sql, insert_sql]

        else:
            # strict insert only (fail if conflicts exist)
            if input_has_disabled and apply_soft_delete:
                insert_sql = f"""
                INSERT INTO {self.schema}.{self.table} (domain, key1, key2, value1, value2, disabled)
                SELECT
                  domain, key1, key2, value1, value2,
                  CASE
                    WHEN disabled_raw IN ('true','t','1','yes','y','on') THEN TRUE
                    WHEN disabled_raw IN ('false','f','0','no','n','off') THEN FALSE
                    ELSE FALSE
                  END AS disabled
                FROM {stage};
                """
            else:
                insert_sql = f"""
                INSERT INTO {self.schema}.{self.table} (domain, key1, key2, value1, value2)
                SELECT domain, key1, key2, value1, value2
                FROM {stage};
                """
            apply_sqls = [insert_sql]

        # Execute in one batch so TEMP stage survives
        sqls: List[str] = [
            "BEGIN;",
            create_stage_sql,
            copy_sql,
            normalize_sql,
        ]
        if normalize_disabled_sql:
            sqls.append(normalize_disabled_sql)

        # validations (all return result sets)
        sqls.extend([
            stage_count_sql,
            stage_dupes_sql,
            required_fields_sql,
        ])

        if not upsert:
            sqls.append(conflicts_sql)

        sqls.extend(apply_sqls)
        sqls.append("COMMIT;")

        result_sets = self._execute_batch_and_collect_selects(sqls)

        # Result sets appear in order of SELECTs:
        # 1) stage_count, 2) stage_dupes, 3) required_fields, 4) (optional) conflicts
        idx = 0

        staged_rows = self._first_cell_as_int(result_sets[idx]); idx += 1
        if staged_rows <= 0:
            raise ValueError("Stage load produced 0 rows (nothing to apply).")

        dupes = result_sets[idx]; idx += 1
        if dupes:
            raise ValueError(f"Duplicate (domain,key1) rows found in staged input. Examples: {dupes[:10]}")

        bad_required = result_sets[idx]; idx += 1
        if bad_required:
            raise ValueError(
                "Rows missing required fields after normalization (domain, key1, value1). "
                f"Examples: {bad_required[:10]}"
            )

        if not upsert:
            conflicts = result_sets[idx]; idx += 1
            if conflicts:
                raise ValueError(
                    "Conflicts detected: staged (domain,key1) already exist in target and upsert=False. "
                    f"Examples: {conflicts[:10]}"
                )

        return {
            "status": "success",
            "schema_validated": validate_schema,
            "stage_table": stage,
            "staged_rows": staged_rows,
            "upsert": upsert,
            "input_has_disabled": input_has_disabled,
            "apply_soft_delete": apply_soft_delete,
        }

    # ---------- Data API helpers ----------

    def _execute_batch(self, sqls: List[str]) -> str:
        resp = self.rsd.execute_batch_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            DbUser=self.db_user,
            Sqls=sqls,
        )
        return resp["Id"]

    def _wait(self, statement_id: str) -> Dict[str, Any]:
        while True:
            desc = self.rsd.describe_statement(Id=statement_id)
            if desc["Status"] in ("FINISHED", "FAILED", "ABORTED"):
                return desc
            time.sleep(self.poll_seconds)

    def _query(self, sql: str) -> List[Dict[str, Any]]:
        resp = self.rsd.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=self.database,
            DbUser=self.db_user,
            Sql=sql,
        )
        desc = self._wait(resp["Id"])
        if desc["Status"] != "FINISHED":
            raise RuntimeError(f"Query failed: {desc}")

        rs = self.rsd.get_statement_result(Id=resp["Id"])
        cols = [c["name"] for c in rs["ColumnMetadata"]]
        out: List[Dict[str, Any]] = []
        for rec in rs["Records"]:
            out.append({col: (list(val.values())[0] if val else None) for col, val in zip(cols, rec)})
        return out

    def _execute_batch_and_collect_selects(self, sqls: List[str]) -> List[List[Dict[str, Any]]]:
        batch_id = self._execute_batch(sqls)
        desc = self._wait(batch_id)
        if desc["Status"] != "FINISHED":
            raise RuntimeError(f"Batch failed: {desc}")

        batch_desc = self.rsd.describe_statement(Id=batch_id)
        subs = batch_desc.get("SubStatements", [])

        result_sets: List[List[Dict[str, Any]]] = []
        for ss in subs:
            if not ss.get("HasResultSet"):
                continue
            rs = self.rsd.get_statement_result(Id=ss["Id"])
            cols = [c["name"] for c in rs["ColumnMetadata"]]
            rows: List[Dict[str, Any]] = []
            for rec in rs["Records"]:
                rows.append({col: (list(val.values())[0] if val else None) for col, val in zip(cols, rec)})
            result_sets.append(rows)

        return result_sets

    # ---------- misc ----------

    def _ephemeral_stage_table_name(self) -> str:
        return f"stg_cfg_{uuid.uuid4().hex[:10]}"

    def _first_cell_as_int(self, rows: List[Dict[str, Any]]) -> int:
        if not rows or not rows[0]:
            return 0
        v = next(iter(rows[0].values()))
        return int(v) if v is not None else 0
