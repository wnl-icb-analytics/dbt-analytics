"""Capture row counts for every PROD dbt model (tables and views).

Writes to DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_ROW_COUNT_BASELINE plus a
JSON snapshot at qa/baseline_<label>.json. Tables are sourced from
ROW_COUNT_LOG when a recent entry exists; views (and uncovered tables) get a
live COUNT(*) via a thread pool.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

import snowflake.connector

DEFAULT_DATABASES = [
    # STAGING (raw + staging views) excluded by design; counts equal upstream
    "MODELLING",
    "REPORTING",
    "PUBLISHED_REPORTING__DIRECT_CARE",
    "PUBLISHED_REPORTING__SECONDARY_USE",
]

EXCLUDE_SCHEMA_PATTERNS = [
    "INFORMATION_SCHEMA",
    "DBT_DEV%",
    "DBT_TEST%",
    "TEST_AUDIT%",
    "DBT_SNAPSHOTS",
    "ELEMENTARY%",
]

BASELINE_TABLE = "DATA_LAKE__NCL.DBT_OBSERVABILITY.MODEL_ROW_COUNT_BASELINE"

# Snowflake unquoted identifier rule (uppercase letters, digits, underscore).
# Used to validate any identifier we splice into SQL — Snowflake parameters
# only bind values, not identifiers, so we have to gate this ourselves.
_IDENT_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")


def _safe_ident(name: str, *, kind: str = "identifier") -> str:
    """Validate and quote a Snowflake identifier."""
    upper = name.upper()
    if not _IDENT_RE.fullmatch(upper):
        raise ValueError(f"Invalid {kind}: {name!r}")
    return f'"{upper}"'

CREATE_BASELINE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BASELINE_TABLE} (
  label VARCHAR,
  captured_at TIMESTAMP_NTZ,
  database_name VARCHAR,
  schema_name VARCHAR,
  model_name VARCHAR,
  table_type VARCHAR,
  row_count NUMBER,
  source VARCHAR,
  source_run_at TIMESTAMP_NTZ,
  error VARCHAR
)
""".strip()


@dataclass
class ModelCount:
    database_name: str
    schema_name: str
    model_name: str
    table_type: str
    row_count: Optional[int]
    source: str  # 'row_count_log' | 'live_count' | 'error'
    source_run_at: Optional[str]
    error: Optional[str]


def connect(connection_name: str) -> snowflake.connector.SnowflakeConnection:
    # Force externalbrowser SSO; connections.toml entries don't set authenticator
    # because the snow CLI infers it, but the Python connector defaults to password auth.
    return snowflake.connector.connect(
        connection_name=connection_name,
        authenticator="externalbrowser",
    )


def discover_models(
    conn: snowflake.connector.SnowflakeConnection,
    databases: list[str],
) -> list[tuple[str, str, str, str]]:
    """Return (db, schema, name, table_type) for every table/view in scope."""
    parts = []
    for db in databases:
        qdb = _safe_ident(db, kind="database")
        # `db` is now validated as an unquoted Snowflake identifier, so it's
        # safe to splice as both literal and identifier.
        parts.append(
            f"SELECT '{db.upper()}' AS table_catalog, table_schema, table_name, table_type "
            f"FROM {qdb}.INFORMATION_SCHEMA.TABLES "
            f"WHERE table_type IN ('BASE TABLE', 'VIEW')"
        )
    where_exclusions = " AND ".join(
        f"table_schema NOT LIKE '{pat}'" for pat in EXCLUDE_SCHEMA_PATTERNS
    )
    sql = f"SELECT * FROM ({' UNION ALL '.join(parts)}) WHERE {where_exclusions} ORDER BY 1, 2, 3"
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
    return [(r[0], r[1], r[2], r[3]) for r in rows]


def fetch_recent_row_count_log(
    conn: snowflake.connector.SnowflakeConnection,
    log_window_days: int,
) -> dict[tuple[str, str, str], tuple[int, datetime]]:
    """Return latest (row_count, run_started_at) per (db, schema, model)."""
    window = int(log_window_days)  # coerce; never interpolate untrusted text
    sql = f"""
    WITH ranked AS (
      SELECT
        database_name,
        schema_name,
        model_name,
        row_count,
        run_started_at,
        ROW_NUMBER() OVER (
          PARTITION BY database_name, schema_name, model_name
          ORDER BY run_started_at DESC
        ) AS rn
      FROM DATA_LAKE__NCL.DBT_OBSERVABILITY.ROW_COUNT_LOG
      WHERE run_started_at >= DATEADD('day', -{window}, CURRENT_TIMESTAMP())
    )
    SELECT database_name, schema_name, model_name, row_count, run_started_at
    FROM ranked
    WHERE rn = 1
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        cur.close()
    # ROW_COUNT_LOG stores model identifiers in lowercase; INFORMATION_SCHEMA
    # returns them uppercase. Index by upper() for case-insensitive lookup.
    return {(db.upper(), sch.upper(), name.upper()): (int(cnt), ts) for db, sch, name, cnt, ts in rows}


def live_count(
    conn_factory,
    db: str,
    schema: str,
    name: str,
    statement_timeout_seconds: int,
) -> tuple[Optional[int], Optional[str]]:
    """Run a COUNT(*) and return (count, error)."""
    # Defensive: db/schema/name come from INFORMATION_SCHEMA, but validate
    # anyway so a corrupt index can't construct arbitrary SQL.
    try:
        qdb = _safe_ident(db, kind="database")
        qschema = _safe_ident(schema, kind="schema")
        qname = _safe_ident(name, kind="table")
    except ValueError as exc:
        return None, str(exc)
    timeout = int(statement_timeout_seconds)
    conn = conn_factory()
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}")
            cur.execute(f"SELECT COUNT(*) FROM {qdb}.{qschema}.{qname}")
            (count,) = cur.fetchone()
            return int(count), None
        finally:
            cur.close()
    except Exception as exc:  # noqa: BLE001 - surface anything to baseline
        return None, str(exc)[:500]
    finally:
        conn.close()


def write_baseline(
    conn: snowflake.connector.SnowflakeConnection,
    label: str,
    captured_at: datetime,
    counts: list[ModelCount],
) -> None:
    # DDL outside the transaction (CREATE TABLE IF NOT EXISTS is idempotent
    # and Snowflake auto-commits DDL anyway). Replace + insert is wrapped in
    # an explicit transaction so a failed insert can't leave the label empty.
    cur = conn.cursor()
    try:
        cur.execute(CREATE_BASELINE_DDL)
        rows = [
            (
                label,
                captured_at,
                c.database_name,
                c.schema_name,
                c.model_name,
                c.table_type,
                c.row_count,
                c.source,
                c.source_run_at,
                c.error,
            )
            for c in counts
        ]
        cur.execute("BEGIN")
        try:
            cur.execute(
                f"DELETE FROM {BASELINE_TABLE} WHERE label = %s",
                (label,),
            )
            cur.executemany(
                f"INSERT INTO {BASELINE_TABLE} "
                f"(label, captured_at, database_name, schema_name, model_name, "
                f"table_type, row_count, source, source_run_at, error) "
                f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                rows,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
    finally:
        cur.close()


def write_json_snapshot(
    path: pathlib.Path,
    label: str,
    captured_at: datetime,
    counts: list[ModelCount],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "label": label,
        "captured_at": captured_at.isoformat(),
        "model_count": len(counts),
        "models": [asdict(c) for c in counts],
    }
    path.write_text(json.dumps(payload, indent=2, default=str))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--label", required=True, help="Baseline label, e.g. 'pre-olids-2026'")
    ap.add_argument(
        "--connection",
        default="data-platform-manager",
        help="Snowflake connection_name from connections.toml",
    )
    ap.add_argument(
        "--databases",
        nargs="+",
        default=DEFAULT_DATABASES,
        help="Databases to scan (default: prod 4)",
    )
    ap.add_argument(
        "--log-window-days",
        type=int,
        default=7,
        help="How recent a ROW_COUNT_LOG entry must be to skip a live COUNT(*).",
    )
    ap.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Parallel COUNT(*) workers (each opens its own connection).",
    )
    ap.add_argument(
        "--statement-timeout",
        type=int,
        default=300,
        help="Per-COUNT(*) statement timeout in seconds.",
    )
    ap.add_argument(
        "--json-out",
        type=pathlib.Path,
        default=None,
        help="JSON snapshot path (default: qa/baseline_<label>.json at repo root).",
    )
    ap.add_argument(
        "--no-snowflake-write",
        action="store_true",
        help="Skip writing to MODEL_ROW_COUNT_BASELINE (JSON only).",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of models scanned (smoke testing).",
    )
    args = ap.parse_args()

    repo_root = pathlib.Path(__file__).resolve().parents[2]
    json_out = args.json_out or repo_root / "qa" / f"baseline_{args.label}.json"

    captured_at = datetime.now(timezone.utc).replace(tzinfo=None)

    main_conn = connect(args.connection)
    try:
        print(f"Discovering models across {len(args.databases)} databases...", file=sys.stderr)
        models = discover_models(main_conn, args.databases)
        if args.limit:
            models = models[: args.limit]
        print(f"  -> {len(models):,} models in scope", file=sys.stderr)

        print(f"Loading ROW_COUNT_LOG (last {args.log_window_days}d)...", file=sys.stderr)
        log_index = fetch_recent_row_count_log(main_conn, args.log_window_days)
        print(f"  -> {len(log_index):,} entries indexed", file=sys.stderr)

        from_log: list[ModelCount] = []
        needs_live: list[tuple[str, str, str, str]] = []
        for db, schema, name, ttype in models:
            if ttype == "BASE TABLE":
                hit = log_index.get((db.upper(), schema.upper(), name.upper()))
                if hit is not None:
                    cnt, run_at = hit
                    from_log.append(
                        ModelCount(
                            database_name=db,
                            schema_name=schema,
                            model_name=name,
                            table_type=ttype,
                            row_count=cnt,
                            source="row_count_log",
                            source_run_at=run_at.isoformat() if run_at else None,
                            error=None,
                        )
                    )
                    continue
            needs_live.append((db, schema, name, ttype))

        print(
            f"  -> {len(from_log):,} from log, {len(needs_live):,} need live COUNT(*)",
            file=sys.stderr,
        )

    finally:
        main_conn.close()

    live_results: list[ModelCount] = []
    if needs_live:
        print(f"Running live counts with {args.max_workers} workers...", file=sys.stderr)
        start = time.time()
        done = 0

        def _factory():
            return connect(args.connection)

        with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
            futures = {
                pool.submit(live_count, _factory, db, schema, name, args.statement_timeout):
                    (db, schema, name, ttype)
                for db, schema, name, ttype in needs_live
            }
            for fut in as_completed(futures):
                db, schema, name, ttype = futures[fut]
                count, err = fut.result()
                live_results.append(
                    ModelCount(
                        database_name=db,
                        schema_name=schema,
                        model_name=name,
                        table_type=ttype,
                        row_count=count,
                        source="live_count" if err is None else "error",
                        source_run_at=None,
                        error=err,
                    )
                )
                done += 1
                if done % 50 == 0 or done == len(needs_live):
                    elapsed = time.time() - start
                    rate = done / elapsed if elapsed > 0 else 0
                    remaining = (len(needs_live) - done) / rate if rate > 0 else 0
                    print(
                        f"  {done}/{len(needs_live)} live counts done "
                        f"({elapsed:.0f}s elapsed, ~{remaining:.0f}s left)",
                        file=sys.stderr,
                    )

    counts = sorted(
        from_log + live_results,
        key=lambda c: (c.database_name, c.schema_name, c.model_name),
    )

    errors = [c for c in counts if c.error]
    print(
        f"Captured {len(counts):,} models "
        f"({len(from_log):,} from log, {len(live_results):,} live, "
        f"{len(errors):,} errored)",
        file=sys.stderr,
    )

    print(f"Writing JSON snapshot to {json_out}", file=sys.stderr)
    write_json_snapshot(json_out, args.label, captured_at, counts)

    if not args.no_snowflake_write:
        print(f"Writing to {BASELINE_TABLE} (label='{args.label}')", file=sys.stderr)
        snow = connect(args.connection)
        try:
            write_baseline(snow, args.label, captured_at, counts)
        finally:
            snow.close()

    if errors:
        print("\nErrored models (first 20):", file=sys.stderr)
        for c in errors[:20]:
            print(f"  {c.database_name}.{c.schema_name}.{c.model_name}: {c.error}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
