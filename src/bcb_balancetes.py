from __future__ import annotations

import io
import zipfile
from datetime import date, timedelta

import httpx
import polars as pl
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.models import BalanceteRow, ZipDownloadError, ZipNotAvailableError
from src.settings import Settings

logger = structlog.get_logger()

# 4010 CSV column indices (after skipping 3 metadata lines + header)
_4010_COL_CNPJ = 2
_4010_COL_NOME_INST = 4
_4010_COL_CONTA = 8
_4010_COL_NOME_CONTA = 9
_4010_COL_SALDO = 10


def generate_monthly_periods(n_months: int) -> list[int]:
    """Generate the last N monthly AAAAMM values, accounting for publication lag.

    BCB publishes balancetes with ~60-day lag (90 days for December).
    Returns list like [202501, 202412, 202411, ...] (most recent first).
    """
    ref = date.today() - timedelta(days=60)
    year = ref.year
    month = ref.month

    periods: list[int] = []
    for _ in range(n_months):
        periods.append(year * 100 + month)
        month -= 1
        if month == 0:
            month = 12
            year -= 1

    return periods


def _build_zip_url(base_url: str, ano_mes: int) -> tuple[str, str]:
    """Build primary and fallback URLs for balancete ZIP download.

    Returns (primary_url, fallback_url).
    Primary uses AAAAMM format, fallback uses AAAAMM with leading zero.
    """
    year = ano_mes // 100
    month = ano_mes % 100
    primary = f"{base_url}/{year}{month:02d}.zip"
    fallback = f"{base_url}/b{year}{month:02d}.zip"
    return primary, fallback


def _download_zip_bytes(
    client: httpx.Client,
    primary_url: str,
    fallback_url: str,
    max_retries: int = 3,
) -> bytes:
    """Download ZIP file with fallback URL on 404."""
    for url in (primary_url, fallback_url):
        try:
            response = _fetch_url(client, url, max_retries)
            return response.content
        except ZipNotAvailableError:
            continue

    raise ZipNotAvailableError(0)


@retry(  # type: ignore[untyped-decorator]
    retry=retry_if_exception_type(httpx.TransportError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def _fetch_url(client: httpx.Client, url: str, max_retries: int) -> httpx.Response:
    """Fetch a single URL with retry on transport errors."""
    response = client.get(url)
    if response.status_code == 404:
        raise ZipNotAvailableError(0)
    if response.status_code != 200:
        raise ZipDownloadError(url, response.status_code)
    return response


def _extract_csv_from_zip(zip_bytes: bytes) -> bytes:
    """Extract the first CSV file from a ZIP archive in memory."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            msg = "No CSV file found in ZIP archive"
            raise ValueError(msg)
        return zf.read(csv_names[0])


def _parse_csv_rows(csv_bytes: bytes, ano_mes: int) -> list[BalanceteRow]:
    """Parse balancete CSV bytes into BalanceteRow list, filtering Documento=4040."""
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        encoding="latin1",
        separator=";",
        has_header=True,
        infer_schema_length=0,
    )

    col_map = {
        df.columns[0]: "cnpj",
        df.columns[1]: "nome_inst",
        df.columns[2]: "atributo",
        df.columns[3]: "documento",
        df.columns[4]: "conta",
        df.columns[5]: "nome_conta",
        df.columns[6]: "saldo_str",
    }
    df = df.select(list(col_map.keys())).rename(col_map)
    df = df.filter(pl.col("documento") == "4040")

    if df.is_empty():
        return []

    saldo_expr = (
        pl.col("saldo_str")
        .str.replace_all(r"\.", "")
        .str.replace(",", ".")
        .cast(pl.Float64)
        .alias("saldo")
    )
    df = df.with_columns(
        saldo_expr,
        pl.col("cnpj").str.slice(0, 8).alias("cnpj8"),
        pl.lit(ano_mes).alias("ano_mes"),
    )

    rows: list[BalanceteRow] = []
    for row in df.iter_rows(named=True):
        rows.append(
            BalanceteRow(
                ano_mes=row["ano_mes"],
                cnpj=row["cnpj"],
                cnpj8=row["cnpj8"],
                nome_inst=row["nome_inst"],
                atributo=row["atributo"],
                documento=row["documento"],
                conta=row["conta"],
                nome_conta=row["nome_conta"],
                saldo=row["saldo"],
            )
        )
    return rows


def fetch_balancetes(client: httpx.Client, settings: Settings, ano_mes: int) -> list[BalanceteRow]:
    """Orchestrate download, extraction, and parsing of balancete for a period."""
    primary, fallback = _build_zip_url(settings.balancetes_base_url, ano_mes)
    logger.info("fetching_balancete", ano_mes=ano_mes, url=primary)
    zip_bytes = _download_zip_bytes(client, primary, fallback, settings.balancetes_max_retries)
    csv_bytes = _extract_csv_from_zip(zip_bytes)
    rows = _parse_csv_rows(csv_bytes, ano_mes)
    logger.info("parsed_balancete", ano_mes=ano_mes, rows=len(rows))
    return rows


# ─── 4010 Individual Balancete Functions ───


def _build_4010_url(base_url: str, ano_mes: int, cnpj8: str) -> str:
    """Build URL for individual 4010 balancete download.

    Base URL for 4010: .../cont/balan/individualizados/{AAAAMM}/4010/{AAAAMM}-4010-{CNPJ8}.ZIP
    The settings base_url points to .../cont/balan/bancos, so we strip '/bancos'.
    """
    base = base_url.replace("/bancos", "")
    return f"{base}/individualizados/{ano_mes}/4010/{ano_mes}-4010-{cnpj8}.ZIP"


def _compressed_to_dotted(code: str) -> str:
    """Convert 8-digit compressed COSIF code to dotted format.

    Example: '71100001' -> '7.1.1.00.00-1'
    Format: X.Y.Z.WW.KK-D (8 digits: XYZWWKKD)
    """
    return f"{code[0]}.{code[1]}.{code[2]}.{code[3:5]}.{code[5:7]}-{code[7]}"


def _parse_saldo_br(value: str) -> float:
    """Parse Brazilian number format: '7.043.370,57' -> 7043370.57"""
    return float(value.replace(".", "").replace(",", "."))


def _parse_4010_csv(csv_bytes: bytes, ano_mes: int) -> list[BalanceteRow]:
    """Parse 4010 CSV (3 metadata lines, then header + data, latin1, ';' separator).

    Columns: DATA_BASE;DOCUMENTO;CNPJ;AGENCIA;NOME_INSTITUICAO;
             COD_CONGL;NOME_CONGL;TAXONOMIA;CONTA;NOME_CONTA;SALDO[;...]
    """
    text = csv_bytes.decode("latin1")
    lines = text.strip().splitlines()

    # Skip 3 metadata lines + 1 header line (prefixed with #)
    data_lines = [ln for ln in lines[3:] if ln.strip() and not ln.startswith("#")]
    if not data_lines:
        return []

    rows: list[BalanceteRow] = []
    for line in data_lines:
        cols = line.split(";")
        if len(cols) <= _4010_COL_SALDO:
            continue

        conta_compressed = cols[_4010_COL_CONTA].strip()
        if len(conta_compressed) != 8:
            continue

        saldo_str = cols[_4010_COL_SALDO].strip()
        if not saldo_str:
            continue

        cnpj_raw = cols[_4010_COL_CNPJ].strip()
        rows.append(
            BalanceteRow(
                ano_mes=ano_mes,
                cnpj=cnpj_raw,
                cnpj8=cnpj_raw[:8],
                nome_inst=cols[_4010_COL_NOME_INST].strip(),
                atributo="",
                documento="4010",
                conta=_compressed_to_dotted(conta_compressed),
                nome_conta=cols[_4010_COL_NOME_CONTA].strip(),
                saldo=_parse_saldo_br(saldo_str),
            )
        )
    return rows


def fetch_4010_balancete(
    client: httpx.Client,
    base_url: str,
    ano_mes: int,
    cnpj8: str,
) -> list[BalanceteRow]:
    """Download, extract, and parse a single institution's 4010 balancete."""
    url = _build_4010_url(base_url, ano_mes, cnpj8)
    response = client.get(url)
    if response.status_code == 404:
        raise ZipNotAvailableError(ano_mes)
    if response.status_code != 200:
        raise ZipDownloadError(url, response.status_code)
    csv_bytes = _extract_csv_from_zip(response.content)
    return _parse_4010_csv(csv_bytes, ano_mes)
