"""CLI script to fetch IF.data and populate DuckDB.

Usage:
    python -m scripts.refresh                    # Fetch all configured quarters
    python -m scripts.refresh --quarters 4       # Fetch only last 4 quarters
    python -m scripts.refresh --force            # Re-fetch even if already cached
"""
from __future__ import annotations

import argparse

import structlog

from src.db import get_connection
from src.ifdata_client import IFDataClient
from src.ingest import (
    generate_quarter_periods,
    ingest_cadastro,
    ingest_report_values,
    is_period_fetched,
)
from src.log import configure_logging
from src.settings import Settings


def main() -> None:
    configure_logging()
    logger = structlog.get_logger()

    parser = argparse.ArgumentParser(description="Refresh IF.data cache in DuckDB")
    parser.add_argument(
        "--quarters", type=int, default=None, help="Override number of quarters"
    )
    parser.add_argument(
        "--force", action="store_true", help="Re-fetch even if cached"
    )
    args = parser.parse_args()

    settings = Settings()
    quarters = args.quarters or settings.history_quarters
    periods = generate_quarter_periods(quarters)

    logger.info("refresh_starting", quarters=quarters, periods=periods)

    con = get_connection(settings.duckdb_path)

    with IFDataClient(settings) as client:
        for ano_mes in periods:
            # Fetch and ingest cadastro
            if args.force or not is_period_fetched(con, ano_mes, "cadastro"):
                try:
                    cadastro = client.fetch_cadastro(ano_mes)
                    count = ingest_cadastro(con, cadastro, ano_mes)
                    logger.info(
                        "cadastro_done", ano_mes=ano_mes, records=count
                    )
                except Exception:
                    logger.exception("cadastro_failed", ano_mes=ano_mes)
                    continue
            else:
                logger.info("cadastro_cached", ano_mes=ano_mes)

            # Fetch and ingest each report type
            for relatorio in settings.ifdata_relatorios:
                if not args.force and is_period_fetched(con, ano_mes, relatorio):
                    logger.info(
                        "report_cached", ano_mes=ano_mes, relatorio=relatorio
                    )
                    continue

                try:
                    values = client.fetch_report_values(
                        ano_mes=ano_mes,
                        tipo_instituicao=settings.ifdata_tipo_instituicao,
                        relatorio=relatorio,
                    )
                    count = ingest_report_values(con, values, ano_mes, relatorio)
                    logger.info(
                        "report_done",
                        ano_mes=ano_mes,
                        relatorio=relatorio,
                        rows=count,
                    )
                except Exception:
                    logger.exception(
                        "report_failed", ano_mes=ano_mes, relatorio=relatorio
                    )
                    continue

    con.close()
    logger.info("refresh_complete", periods_attempted=len(periods))


if __name__ == "__main__":
    main()
