"""Seed script — downloads all public BCB data and populates the local DuckDB.

One command to go from a fresh fork to a working dashboard:

    python -m scripts.seed              # Full seed (~20-30 min)
    python -m scripts.seed --skip-4010  # Skip individual balancetes (~5-10 min)
    python -m scripts.seed --quarters 8 --months 12  # Less history
"""
from __future__ import annotations

import argparse
import time

import duckdb

from src.db import get_connection
from src.log import configure_logging
from src.settings import Settings

_TABLES = [
    "cadastro",
    "report_values",
    "fetch_log",
    "balancetes_raw",
    "institution_mapping",
    "balancetes_top50",
]


def _print_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print row counts for all tables."""
    print("\n" + "=" * 50)
    print("  SEED COMPLETE — Database Summary")
    print("=" * 50)
    for table in _TABLES:
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]  # noqa: S608
        status = "OK" if count > 0 else "EMPTY"
        print(f"  {table:<25} {count:>8,} rows  [{status}]")
    print("=" * 50)


def _fmt_elapsed(seconds: float) -> str:
    mins, secs = divmod(int(seconds), 60)
    return f"{mins}m{secs:02d}s"


def run_seed(
    quarters: int | None = None,
    months: int | None = None,
    skip_4010: bool = False,
    force: bool = False,
) -> None:
    """Run the full seed pipeline."""
    settings = Settings()
    total_start = time.monotonic()

    # Step 1: IF.data (cadastro + reports 1, 4, 5)
    print("\n[1/3] Fetching IF.data (cadastro + reports)...")
    step_start = time.monotonic()

    from scripts.refresh import run_refresh

    run_refresh(quarters=quarters, force=force)

    elapsed = time.monotonic() - step_start
    print(f"  Done in {_fmt_elapsed(elapsed)}")

    # Step 2: Balancetes 4040
    print("\n[2/3] Fetching Balancetes 4040...")
    step_start = time.monotonic()

    from scripts.refresh_balancetes import run_refresh_balancetes

    run_refresh_balancetes(months=months, force=force)

    elapsed = time.monotonic() - step_start
    print(f"  Done in {_fmt_elapsed(elapsed)}")

    # Step 3: Balancetes 4010
    if skip_4010:
        print("\n[3/3] Skipping Balancetes 4010 (--skip-4010)")
    else:
        print("\n[3/3] Fetching Balancetes 4010 (individual)...")
        step_start = time.monotonic()

        from scripts.refresh_4010 import run_refresh_4010

        run_refresh_4010(
            months=months or 24,
            all_institutions=True,
            force=force,
        )

        elapsed = time.monotonic() - step_start
        print(f"  Done in {_fmt_elapsed(elapsed)}")

    # Summary
    total_elapsed = time.monotonic() - total_start
    con = get_connection(settings.duckdb_path)
    _print_summary(con)
    con.close()
    print(f"\n  Total time: {_fmt_elapsed(total_elapsed)}")
    print("  Run 'python app.py' to start the dashboard.\n")


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(
        description="Seed the database with public BCB data",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        default=None,
        help="Number of IF.data quarterly periods (default: 20 = ~5 years)",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=None,
        help="Number of monthly periods for balancetes (default: 24)",
    )
    parser.add_argument(
        "--skip-4010",
        action="store_true",
        help="Skip individual 4010 balancetes (faster seed)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch even if already cached",
    )
    args = parser.parse_args()

    run_seed(
        quarters=args.quarters,
        months=args.months,
        skip_4010=args.skip_4010,
        force=args.force,
    )


if __name__ == "__main__":
    main()
