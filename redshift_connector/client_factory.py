"""
client_factory.py

Central place to construct AWS clients for the warehouse layer.
Keeps boto3 out of business logic and makes tests trivial.
"""

from __future__ import annotations
import os
from typing import Optional
import boto3
from botocore.config import Config


# ---- configuration helpers --------------------------------------------------

def _get_region(explicit: Optional[str]) -> str:
    """
    Resolve AWS region in a predictable order.
    """
    return (
        explicit
        or os.getenv("WAREHOUSE_AWS_REGION")
        or os.getenv("AWS_REGION")
        or os.getenv("AWS_DEFAULT_REGION")
        or "us-east-1"
    )


def _get_boto_config() -> Config:
    """
    Shared boto configuration: retries handled by AWS SDK,
    we still keep our own SQL retry logic above.
    """
    return Config(
        retries={
            "max_attempts": 5,
            "mode": "standard"
        },
        connect_timeout=5,
        read_timeout=60,
        tcp_keepalive=True
    )


# ---- public factory ---------------------------------------------------------

def get_redshift_data_client(region: Optional[str] = None):
    """
    Return a configured boto3 Redshift Data API client.

    Does NOT embed database/schema/cluster info —
    those belong to execution layer, not connection layer.
    """

    resolved_region = _get_region(region)

    return boto3.client(
        "redshift-data",
        region_name=resolved_region,
        config=_get_boto_config()
    )