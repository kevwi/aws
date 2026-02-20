"""
executor.py — minimal Redshift Data API statement executor (submit/wait/run).

Design:
- The module depends on an injected `client` that conforms to ClientProtocol (execute_statement,
  describe_statement, get_statement_result). This keeps it testable and decoupled from boto3.
- Retries/backoff apply to polling / transient failures; permanent failures raise a RedshiftPermanentError.
- All sleeps are injectable via `sleep_func` to make tests deterministic.
"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple
import time
import random

# --- Domain exceptions -----------------------------------------------------------------
class RedshiftError(Exception):
    """Base exception for Redshift executor errors."""


class RedshiftTransientError(RedshiftError):
    """Transient error that is safe to retry."""


class RedshiftPermanentError(RedshiftError):
    """Permanent error that should not be retried."""


# --- Client typing --------------------------------------------------------------------
class ClientProtocol(Protocol):
    """
    Minimal protocol describing the calls used from a Redshift Data API client.
    Implementations: boto3.client("redshift-data") matches these call names.
    """

    def execute_statement(self, *, Sql: str, Database: str, DbUser: str, **kwargs: Any) -> Dict[str, Any]:
        ...

    def describe_statement(self, *, Id: str) -> Dict[str, Any]:
        ...

    def get_statement_result(self, *, Id: str) -> Dict[str, Any]:
        ...


# --- Backoff defaults -----------------------------------------------------------------
BASE = 0.5
FACTOR = 2.0
MAX_SLEEP = 10.0
JITTER_PCT = 0.10  # +/-10% jitter


def _compute_backoff(attempt: int, base: float = BASE, factor: float = FACTOR, max_sleep: float = MAX_SLEEP) -> float:
    """Deterministic backoff value with jitter applied."""
    sleep = min(max_sleep, base * (factor ** attempt))
    jitter = sleep * JITTER_PCT
    return sleep + random.uniform(-jitter, jitter)


# --- Low-level primitives -------------------------------------------------------------
def submit_statement(client: ClientProtocol, sql: str, database: str, db_user: str) -> str:
    """
    Submit SQL via the client and return the statement id.

    client is injected for testability.
    """
    resp = client.execute_statement(Sql=sql, Database=database, DbUser=db_user)
    sid = resp.get("Id")
    if not sid:
        raise RedshiftPermanentError("execute_statement returned no Id")
    return sid


def poll_statement(
    client: ClientProtocol,
    statement_id: str,
    timeout_seconds: int = 300,
    sleep_func: Callable[[float], None] = time.sleep,
    on_retry: Optional[Callable[[Dict[str, Any], int], None]] = None,
) -> Dict[str, Any]:
    """
    Poll describe_statement until FINISHED or FAILED or timeout.

    Returns the final describe dict on FINISHED; raises RedshiftPermanentError on FAILED.
    Raises RedshiftPermanentError on timeout.
    """
    start = time.time()
    attempt = 0
    while True:
        desc = client.describe_statement(Id=statement_id)
        status = desc.get("Status")
        if status == "FINISHED":
            return desc
        if status == "FAILED":
            # Inspect error text to decide retriable vs permanent.
            err = desc.get("Error", "") or ""
            # Simple heuristic: treat obvious throttling/timeouts as transient.
            if any(tok in err.lower() for tok in ("timeout", "throttl", "temporar")) and attempt < 3:
                # transient — call on_retry hook, sleep, and continue polling
                attempt += 1
                if on_retry:
                    try:
                        on_retry(desc, attempt)
                    except Exception:
                        pass
                sleep = _compute_backoff(attempt)
                sleep_func(sleep)
                # loop to poll again
                if time.time() - start > timeout_seconds:
                    raise RedshiftPermanentError(f"Timeout while polling statement {statement_id}")
                continue
            # else permanent
            raise RedshiftPermanentError(f"Statement {statement_id} failed: {err}")
        # Not finished & not failed -> WAIT and poll again
        attempt += 1
        sleep = _compute_backoff(attempt)
        sleep_func(sleep)
        if time.time() - start > timeout_seconds:
            raise RedshiftPermanentError(f"Statement {statement_id} timed out after {timeout_seconds}s")


def get_statement_result(client: ClientProtocol, statement_id: str) -> List[Dict[str, Any]]:
    """
    Fetch result rows for a finished statement using client's get_statement_result.

    Converts the Data API result shape into List[Dict[col_name, value]].
    """
    payload = client.get_statement_result(Id=statement_id)
    cols = [c.get("name") for c in payload.get("ColumnMetadata", [])]
    rows = []
    for record in payload.get("Records", []):
        # each record is a list of single-value dicts: {'stringValue': 'x'} or {'longValue': 1}
        row: Dict[str, Any] = {}
        for col_name, cell in zip(cols, record):
            # choose the present value key if any
            if not cell:
                row[col_name] = None
                continue
            if "longValue" in cell:
                row[col_name] = cell["longValue"]
            elif "doubleValue" in cell:
                row[col_name] = cell["doubleValue"]
            elif "stringValue" in cell:
                row[col_name] = cell["stringValue"]
            else:
                row[col_name] = None
        rows.append(row)
    return rows


# --- High-level runner ----------------------------------------------------------------
def run_statement(
    client: ClientProtocol,
    sql: str,
    database: str,
    db_user: str,
    wait_for_completion: bool = True,
    timeout_seconds: int = 300,
    max_submission_retries: int = 1,
    sleep_func: Callable[[float], None] = time.sleep,
    on_retry: Optional[Callable[[Dict[str, Any], int], None]] = None,
) -> Tuple[bool, Optional[str]]:
    """
    High-level helper: submit SQL and optionally wait for completion.

    Returns (True, statement_id) on success.
    Raises RedshiftPermanentError on permanent failures.

    - max_submission_retries: how many times to retry a submit failure (e.g., network blip)
    - sleep_func is injectable for tests
    - on_retry is called with (describe_dict, attempt) when polling detects a transient retriable describe
    """
    submit_attempt = 0
    while True:
        try:
            sid = submit_statement(client, sql, database, db_user)
        except Exception as exc:
            # If execute_statement itself failed, may be transient (network); retry a few times.
            if submit_attempt < max_submission_retries:
                submit_attempt += 1
                sleep_func(_compute_backoff(submit_attempt))
                continue
            raise RedshiftPermanentError(f"Failed to submit statement: {exc}") from exc

        if not wait_for_completion:
            return True, sid

        # poll for completion
        poll_result = poll_statement(client, sid, timeout_seconds=timeout_seconds, sleep_func=sleep_func, on_retry=on_retry)
        # if we reached here, status == FINISHED
        return True, sid