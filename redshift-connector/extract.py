
from typing import Any, Optional

def s3_object_exists(s3_client: Any, bucket: str, key: str) -> bool:
    """Return True if object exists via head_object."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def estimate_parquet_row_count(s3_client: Any, bucket: str, key: str) -> Optional[int]:
    """Best-effort; return None in this minimal implementation."""
    return None
