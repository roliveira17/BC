"""CLI script to fetch Balancetes 4040 and populate DuckDB.

Usage:
    python -m scripts.refresh_balancetes                  # Fetch configured months
    python -m scripts.refresh_balancetes --months 3       # Fetch only last 3 months
    python -m scripts.refresh_balancetes --force          # Re-fetch even if cached
"""
from __future__ import annotations

import argparse

import httpx
import structlog

from src.bcb_balancetes import fetch_balancetes, generate_monthly_periods
from src.db import get_connection
from src.ingest import ingest_balancetes, is_period_fetched
from src.log import configure_logging
from src.models import ZipDownloadError, ZipNotAvailableError
from src.settings import Settings


def run_refresh_balancetes(months: int | None = None, force: bool = False) -> None:
    """Fetch Balancetes 4040 and populate DuckDB.

    Can be called programmatically (from seed.py) or via CLI.
    """
    logger = structlog.get_logger()

    settings = Settings()
    months = months or settings.balancetes_history_months
    periods = generate_monthly_periods(months)

    logger.info("balancetes_refresh_starting", months=months, periods=periods)

    con = get_connection(settings.duckdb_path)

    with httpx.Client(
        timeout=httpx.Timeout(settings.balancetes_timeout_sec),
        follow_redirects=True,
    ) as client:
        for ano_mes in periods:
            if not force and is_period_fetched(con, ano_mes, "balancetes"):
                logger.info("balancetes_cached", ano_mes=ano_mes)
                continue

            try:
                rows = fetch_balancetes(client, settings, ano_mes)
                count = ingest_balancetes(con, rows, ano_mes)
                logger.info("balancetes_done", ano_mes=ano_mes, rows=count)
            except ZipNotAvailableError:
                logger.info("balancetes_not_available", ano_mes=ano_mes)
                continue
            except ZipDownloadError:
                logger.exception("balancetes_download_failed", ano_mes=ano_mes)
                continue

    con.close()
    logger.info("balancetes_refresh_complete", periods_attempted=len(periods))


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="Refresh Balancetes 4040 in DuckDB")
    parser.add_argument(
        "--months", type=int, default=None, help="Override number of months"
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-fetch even if cached"
    )
    args = parser.parse_args()

    run_refresh_balancetes(months=args.months, force=args.force)


if __name__ == "__main__":
    main()
