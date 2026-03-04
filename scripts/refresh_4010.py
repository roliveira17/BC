"""CLI script to fetch Balancetes 4010 (individual) and populate DuckDB.

Downloads individual institution 4010 balancetes from BCB for all institutions
belonging to target conglomerados (default: Cora Peers).

Usage:
    python -m scripts.refresh_4010                    # Cora peers, last 24 months
    python -m scripts.refresh_4010 --months 12        # Last 12 months
    python -m scripts.refresh_4010 --all              # All mapped institutions
    python -m scripts.refresh_4010 --force            # Re-fetch cached periods
"""

from __future__ import annotations

import argparse
import time

import httpx
import structlog

from src.bcb_balancetes import fetch_4010_balancete
from src.db import get_connection
from src.ifdata_client import IFDataClient
from src.ingest import (
    ingest_4010_batch,
    ingest_institution_mapping,
    is_period_fetched,
)
from src.log import configure_logging
from src.models import BalanceteRow, ZipDownloadError, ZipNotAvailableError
from src.settings import Settings

_CONGL_BASE = 1_000_000_000
_REQUEST_DELAY = 0.3


def _fetch_individual_cadastro(
    client: IFDataClient,
    ano_mes: int,
) -> list[dict[str, object]]:
    """Fetch cadastro for tipo_inst=1006 (individual) and extract raw entries."""
    suffix = f"cadastro{ano_mes}_1006.json"
    file_path = client._find_file(ano_mes, suffix)
    if not file_path:
        return []
    raw = client._fetch_file(file_path)
    if not isinstance(raw, list):
        return []
    return raw


def _build_mapping(
    entries: list[dict[str, object]],
) -> list[dict[str, str | int | None]]:
    """Build CNPJ8 -> cod_conglomerado mapping from cadastro 1006 entries.

    - c0 = CNPJ (as integer string, zero-pad to 8 digits)
    - c15 = internal code -> cod_conglomerado = 1_000_000_000 + int(c15)
    - c2 = institution name
    - For independent institutions (c15 absent): cod_conglomerado = 1_000_000_000 + int(c0)
    """
    mappings: list[dict[str, str | int | None]] = []
    seen: set[str] = set()
    for entry in entries:
        cnpj_raw = str(entry.get("c0", ""))
        cnpj8 = cnpj_raw.zfill(8)[:8]
        if cnpj8 in seen:
            continue
        seen.add(cnpj8)

        nome_inst = str(entry.get("c2", ""))
        c15 = entry.get("c15")

        if c15 is not None and str(c15).strip():
            cod_conglomerado = _CONGL_BASE + int(c15)
        else:
            cod_conglomerado = _CONGL_BASE + int(cnpj_raw)

        nome_congl = str(entry.get("c16", nome_inst))

        mappings.append(
            {
                "cnpj8": cnpj8,
                "nome_inst": nome_inst,
                "cod_conglomerado": cod_conglomerado,
                "nome_conglomerado": nome_congl,
            }
        )
    return mappings


def _filter_by_conglomerados(
    mappings: list[dict[str, str | int | None]],
    target_codes: set[int],
) -> list[dict[str, str | int | None]]:
    """Filter mappings to institutions belonging to target conglomerados."""
    return [m for m in mappings if m["cod_conglomerado"] in target_codes]


def _generate_monthly_periods(n_months: int) -> list[int]:
    """Generate monthly AAAAMM periods working backwards from the latest available.

    Uses 180-day lag (~6 months) because BCB publishes individual 4010 files
    with a delay of approximately 5-6 months.
    """
    from datetime import date, timedelta

    reference = date.today() - timedelta(days=180)
    year = reference.year
    month = reference.month

    periods: list[int] = []
    for _ in range(n_months):
        periods.append(year * 100 + month)
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return periods


def run_refresh_4010(
    months: int = 24,
    all_institutions: bool = False,
    force: bool = False,
) -> None:
    """Fetch Balancetes 4010 (individual) and populate DuckDB.

    Can be called programmatically (from seed.py) or via CLI.
    """
    logger = structlog.get_logger()

    settings = Settings()
    con = get_connection(settings.duckdb_path)

    logger.info("fetching_individual_cadastro")
    with IFDataClient(settings) as ifdata_client:
        all_periods = ifdata_client.list_periods()
        latest_period = all_periods[0] if all_periods else None
        if not latest_period:
            logger.error("no_periods_available")
            return

        raw_entries = _fetch_individual_cadastro(ifdata_client, latest_period)
        if not raw_entries:
            logger.error("no_cadastro_1006", period=latest_period)
            return

    all_mappings = _build_mapping(raw_entries)
    ingest_institution_mapping(con, all_mappings)
    logger.info("mapping_complete", total_institutions=len(all_mappings))

    if all_institutions:
        target_mappings = all_mappings
    else:
        from app import CORA_PEERS_CODES

        target_codes = set(CORA_PEERS_CODES)
        target_mappings = _filter_by_conglomerados(all_mappings, target_codes)

    cnpj8_list = [str(m["cnpj8"]) for m in target_mappings]
    logger.info(
        "target_institutions",
        count=len(cnpj8_list),
        mode="all" if all_institutions else "cora_peers",
    )

    periods = _generate_monthly_periods(months)
    logger.info("periods_to_fetch", periods=periods)

    total_downloads = len(cnpj8_list) * len(periods)
    success_count = 0
    skip_count = 0
    fail_count = 0

    with httpx.Client(
        timeout=httpx.Timeout(settings.balancetes_timeout_sec),
        follow_redirects=True,
    ) as client:
        for period_idx, ano_mes in enumerate(periods):
            all_rows: list[BalanceteRow] = []

            if not force and is_period_fetched(con, ano_mes, "4010"):
                logger.info("4010_period_cached", ano_mes=ano_mes)
                skip_count += len(cnpj8_list)
                continue

            for inst_idx, cnpj8 in enumerate(cnpj8_list):
                progress = period_idx * len(cnpj8_list) + inst_idx + 1
                try:
                    rows = fetch_4010_balancete(
                        client, settings.balancetes_base_url, ano_mes, cnpj8
                    )
                    all_rows.extend(rows)
                    success_count += 1
                    logger.debug(
                        "4010_fetched",
                        ano_mes=ano_mes,
                        cnpj8=cnpj8,
                        rows=len(rows),
                        progress=f"{progress}/{total_downloads}",
                    )
                except ZipNotAvailableError:
                    skip_count += 1
                    logger.debug(
                        "4010_not_available",
                        ano_mes=ano_mes,
                        cnpj8=cnpj8,
                    )
                except ZipDownloadError:
                    fail_count += 1
                    logger.warning(
                        "4010_download_failed",
                        ano_mes=ano_mes,
                        cnpj8=cnpj8,
                    )

                time.sleep(_REQUEST_DELAY)

            if all_rows:
                count = ingest_4010_batch(con, all_rows, ano_mes)
                logger.info(
                    "4010_period_ingested",
                    ano_mes=ano_mes,
                    rows=count,
                )

    con.close()
    logger.info(
        "4010_refresh_complete",
        success=success_count,
        skipped=skip_count,
        failed=fail_count,
    )


def main() -> None:
    configure_logging()

    parser = argparse.ArgumentParser(description="Refresh Balancetes 4010 (individual) in DuckDB")
    parser.add_argument(
        "--months", type=int, default=24, help="Number of monthly periods (default 24)"
    )
    parser.add_argument(
        "--all", action="store_true", help="Fetch all mapped institutions (not just peers)"
    )
    parser.add_argument("--force", action="store_true", help="Re-fetch even if cached")
    args = parser.parse_args()

    run_refresh_4010(
        months=args.months,
        all_institutions=args.all,
        force=args.force,
    )


if __name__ == "__main__":
    main()
