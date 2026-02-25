from __future__ import annotations

import httpx
import pytest

from src.ifdata_client import IFDataClient
from src.models import APIFetchError
from src.settings import Settings


def _make_odata_response(
    records: list[dict[str, object]],
    next_link: str | None = None,
) -> httpx.Response:
    """Build a mock OData JSON response."""
    body: dict[str, object] = {"value": records}
    if next_link:
        body["@odata.nextLink"] = next_link
    return httpx.Response(200, json=body)


def _make_settings(base_url: str = "https://test.example.com/odata") -> Settings:
    return Settings(
        ifdata_base_url=base_url,
        ifdata_timeout_sec=5,
        ifdata_max_retries=1,
    )


class TestFetchCadastro:
    def test_parses_single_page(self) -> None:
        records = [
            {
                "CodConglomerado": 1,
                "NomeConglomerado": "BANCO A",
                "CodInst": 10,
                "NomeInst": "BANCO A SA",
                "CNPJ": "111",
                "Segmento": "S1",
                "TipoInstituicao": 1,
                "Cidade": "SP",
                "UF": "SP",
            }
        ]
        transport = httpx.MockTransport(
            lambda req: _make_odata_response(records)
        )
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_cadastro(202409)
        assert len(result) == 1
        assert result[0].cod_conglomerado == 1
        assert result[0].segmento == "S1"

    def test_handles_pagination(self) -> None:
        page1_records = [
            {
                "CodConglomerado": 1,
                "NomeConglomerado": "A",
                "CodInst": 10,
                "NomeInst": "A",
                "CNPJ": "1",
                "Segmento": "S1",
                "TipoInstituicao": 1,
                "Cidade": "",
                "UF": "",
            }
        ]
        page2_records = [
            {
                "CodConglomerado": 2,
                "NomeConglomerado": "B",
                "CodInst": 20,
                "NomeInst": "B",
                "CNPJ": "2",
                "Segmento": "S2",
                "TipoInstituicao": 1,
                "Cidade": "",
                "UF": "",
            }
        ]
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_odata_response(
                    page1_records, next_link="https://test.example.com/odata/next"
                )
            return _make_odata_response(page2_records)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_cadastro(202409)
        assert len(result) == 2
        assert result[0].cod_conglomerado == 1
        assert result[1].cod_conglomerado == 2


class TestFetchReportValues:
    def test_parses_report_values(self) -> None:
        records = [
            {
                "CodConglomerado": 100,
                "NomeConglomerado": "ITAU",
                "CodigoColuna": "c1",
                "NomeColuna": "Col 1",
                "ValorA": 15.5,
                "NomeLinha": "Indice de Basileia",
                "Ordenacao": 1,
            }
        ]
        transport = httpx.MockTransport(
            lambda req: _make_odata_response(records)
        )
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_report_values(202409, 1, "5")
        assert len(result) == 1
        assert result[0].valor_a == 15.5
        assert result[0].nome_linha == "Indice de Basileia"


class TestErrorHandling:
    def test_raises_api_fetch_error_on_non_200(self) -> None:
        transport = httpx.MockTransport(
            lambda req: httpx.Response(500, text="Internal Server Error")
        )
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        with pytest.raises(APIFetchError) as exc_info:
            client.fetch_cadastro(202409)
        assert exc_info.value.status_code == 500

    def test_retries_on_transport_error(self) -> None:
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ConnectError("connection refused")
            return _make_odata_response([])

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_cadastro(202409)
        assert result == []
        assert call_count == 3
