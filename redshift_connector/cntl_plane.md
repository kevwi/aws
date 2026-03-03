# Config & Rules Control Plane

## What

This repository implements a lightweight configuration and rules control
plane backed by Redshift (postgres).

Core components:

-   `control.config_lookup` (central configuration table)
-   Deterministic shared dimension seeds
-   JSON-based configuration publishing
-   Stored procedures, functions for rule execution
-   Pipeline logging in `control.pipe_log`

------------------------------------------------------------------------

## Why

Modern data systems commonly suffer from:

-   Configuration drift
-   Hidden business logic in code
-   Inconsistent validation across environments
-   Limited auditability

This control plane centralizes configuration and validation logic as
data. It ensures rules are executable, versionable, and auditable.

------------------------------------------------------------------------

## How To Use

1.  Create a JSON configuration file.
2.  Publish using the CLI.
3.  Execute stored procedures for validation or logging.
4.  Query results for audit and verification.

------------------------------------------------------------------------

## Auditability & Control Plane Design

This system provides:

-   Explicit configuration storage
-   Execution-time rule enforcement
-   Deterministic logging
-   Reusable validation for ETL, unit tests, and business rules

It supports:

-   Bronze -> Silver -> Gold validation gates
-   Unit test validation using the same rule engine
-   Business rule enforcement
-   Pipeline event logging

Design principles:

1.  Configuration is explicit.
2.  Validation is executable.
3.  Rules are reusable.
4.  Changes are auditable.
5.  Simplicity over abstraction.
