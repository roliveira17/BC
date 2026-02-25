from __future__ import annotations

import io
import zipfile

import httpx
import pytest

from src.bcb_balancetes import (
    _build_zip_url,
    _extract_csv_from_zip,
    _parse_csv_rows,
    fetch_balancetes,
    generate_monthly_periods,
)
from src.models import ZipDownloadError, ZipNotAvailableError
from src.settings import Settings

# --- generate_monthly_periods ---


class TestGenerateMonthlyPeriods:
    def test_returns_correct_count(self) -> None:
        periods = generate_monthly_periods(6)
        assert len(periods) == 6

    def test_returns_descending_order(self) -> None:
        periods = generate_monthly_periods(12)
        for i in range(len(periods) - 1):
            assert periods[i] > periods[i + 1]

    def test_all_periods_have_valid_months(self) -> None:
        periods = generate_monthly_periods(24)
        for p in periods:
            month = p % 100
            assert 1 <= month <= 12, f"Invalid month: {month}"

    def test_wraps_year_boundary(self) -> None:
        periods = generate_monthly_periods(24)
        years = {p // 100 for p in periods}
        assert len(years) >= 2


# --- _build_zip_url ---


class TestBuildZipUrl:
    def test_returns_primary_and_fallback(self) -> None:
        primary, fallback = _build_zip_url("https://example.com/balan", 202501)
        assert primary == "https://example.com/balan/202501.zip"
        assert fallback == "https://example.com/balan/b202501.zip"

    def test_pads_month_with_zero(self) -> None:
        primary, fallback = _build_zip_url("https://example.com/balan", 202503)
        assert "202503" in primary
        assert "b202503" in fallback

    def test_december(self) -> None:
        primary, _ = _build_zip_url("https://example.com/balan", 202412)
        assert primary == "https://example.com/balan/202412.zip"


# --- ZIP helpers ---


def _make_zip_with_csv(csv_content: bytes, filename: str = "data.csv") -> bytes:
    """Create a ZIP archive in memory containing a single CSV file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, csv_content)
    return buf.getvalue()


class TestExtractCsvFromZip:
    def test_extracts_csv(self) -> None:
        csv_data = b"col1;col2\nval1;val2"
        zip_bytes = _make_zip_with_csv(csv_data)
        result = _extract_csv_from_zip(zip_bytes)
        assert result == csv_data

    def test_raises_on_no_csv(self) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csv here")
        with pytest.raises(ValueError, match="No CSV file"):
            _extract_csv_from_zip(buf.getvalue())


# --- _parse_csv_rows ---

SAMPLE_CSV = (
    "CNPJ;NomeInst;Atrib;Documento;Conta;NomeConta;Saldo\n"
    "00000000000100;BANCO TEST SA;A;4040;6.0.0.00.00-2;Patrimonio Liquido;1.234.567,89\n"
    "00000000000100;BANCO TEST SA;A;4040;1.0.0.00.00-7;Ativo Total;9.876.543,21\n"
    "99999999000100;OUTRO BANCO;A;7777;6.0.0.00.00-2;Patrimonio Liquido;100,00\n"
).encode("latin-1")


class TestParseCsvRows:
    def test_filters_documento_4040(self) -> None:
        rows = _parse_csv_rows(SAMPLE_CSV, 202501)
        assert len(rows) == 2
        for r in rows:
            assert r.documento == "4040"

    def test_parses_saldo_with_comma_decimal(self) -> None:
        rows = _parse_csv_rows(SAMPLE_CSV, 202501)
        pl_row = next(r for r in rows if r.conta == "6.0.0.00.00-2")
        assert pl_row.saldo == pytest.approx(1234567.89, rel=1e-4)

    def test_extracts_cnpj8(self) -> None:
        rows = _parse_csv_rows(SAMPLE_CSV, 202501)
        assert rows[0].cnpj8 == "00000000"

    def test_sets_ano_mes(self) -> None:
        rows = _parse_csv_rows(SAMPLE_CSV, 202501)
        for r in rows:
            assert r.ano_mes == 202501

    def test_empty_after_filter(self) -> None:
        csv = b"CNPJ;NomeInst;Atrib;Documento;Conta;NomeConta;Saldo\n"
        rows = _parse_csv_rows(csv, 202501)
        assert rows == []


# --- Download with MockTransport ---


class TestDownloadZipBytes:
    def test_successful_download(self) -> None:
        csv_data = SAMPLE_CSV
        zip_bytes = _make_zip_with_csv(csv_data)

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=zip_bytes)

        settings = Settings(
            balancetes_base_url="https://example.com/balan",
            balancetes_timeout_sec=10,
        )
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            rows = fetch_balancetes(client, settings, 202501)

        assert len(rows) == 2

    def test_404_on_both_urls_raises(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        settings = Settings(
            balancetes_base_url="https://example.com/balan",
            balancetes_timeout_sec=10,
        )
        with (
            httpx.Client(transport=httpx.MockTransport(handler)) as client,
            pytest.raises(ZipNotAvailableError),
        ):
            fetch_balancetes(client, settings, 202501)

    def test_fallback_url_used_on_primary_404(self) -> None:
        csv_data = SAMPLE_CSV
        zip_bytes = _make_zip_with_csv(csv_data)
        call_urls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            call_urls.append(str(request.url))
            if "b202501" not in str(request.url):
                return httpx.Response(404)
            return httpx.Response(200, content=zip_bytes)

        settings = Settings(
            balancetes_base_url="https://example.com/balan",
            balancetes_timeout_sec=10,
        )
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            rows = fetch_balancetes(client, settings, 202501)

        assert len(rows) == 2
        assert len(call_urls) == 2

    def test_non_200_non_404_raises_download_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500)

        settings = Settings(
            balancetes_base_url="https://example.com/balan",
            balancetes_timeout_sec=10,
        )
        with (
            httpx.Client(transport=httpx.MockTransport(handler)) as client,
            pytest.raises(ZipDownloadError),
        ):
            fetch_balancetes(client, settings, 202501)
