from __future__ import annotations

import httpx
import pytest

from src.ifdata_client import IFDataClient
from src.models import APIFetchError, DataNotFoundError
from src.settings import Settings


def _make_settings(base_url: str = "https://test.example.com/ifdata/rest") -> Settings:
    return Settings(
        ifdata_base_url=base_url,
        ifdata_timeout_sec=5,
        ifdata_max_retries=1,
        ifdata_tipo_inst_id=1009,
    )


def _make_index_entry(
    dt: int,
    tipo_inst: int = 1009,
    trel_ids: list[tuple[int, str]] | None = None,
) -> dict[str, object]:
    """Build a period index entry with files for cadastro, info, dados, and trels."""
    prefix = f"ifdata/{dt}"
    files: list[dict[str, object]] = [
        {"f": f"{prefix}/cadastro{dt}_{tipo_inst}.json"},
        {"f": f"{prefix}/info{dt}.json"},
        {"f": f"{prefix}/dados{dt}_1.json"},
        {"f": f"{prefix}/dados{dt}_5.json"},
    ]
    if trel_ids:
        for trel_id, name in trel_ids:
            files.append({
                "f": f"{prefix}/trel{dt}_{trel_id}.json",
                "trel": {"id": trel_id, "n": name, "s": [{"id": tipo_inst}]},
            })
    return {"dt": dt, "files": files}


def _make_cadastro_entry(
    c0: str = "100",
    c2: str = "BANCO TEST",
    c10: str = "SP",
    c11: str = "SAO PAULO",
    c12: str = "S1",
) -> dict[str, str]:
    return {
        "c0": c0, "c1": "202412", "c2": c2,
        "c10": c10, "c11": c11, "c12": c12,
    }


def _make_info_entry(
    info_id: int,
    name: str,
    group: int = 1,
    lid: int | None = None,
) -> dict[str, object]:
    return {"id": info_id, "n": name, "a": group, "lid": lid or info_id}


def _make_dados(
    group: int,
    entities: list[dict[str, object]],
) -> dict[str, object]:
    return {"id": group, "values": entities}


def _make_entity(
    entity_code: int,
    values: list[tuple[int, float]],
) -> dict[str, object]:
    return {"e": entity_code, "v": [{"i": i, "v": v} for i, v in values]}


def _make_trel(
    trel_id: int,
    name: str,
    columns: list[tuple[int, int]],
) -> dict[str, object]:
    """Build a trel with columns. Each column is (ifd, order)."""
    return {
        "id": trel_id,
        "n": name,
        "c": [{"ifd": ifd, "o": order} for ifd, order in columns],
    }


class TestListPeriods:
    def test_merges_both_indexes(self) -> None:
        index_2024 = [_make_index_entry(202409), _make_index_entry(202412)]
        index_2030 = [_make_index_entry(202503)]

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if "relatorios2000a2024" in path:
                return httpx.Response(200, json=index_2024)
            if "relatorios2025a2030" in path:
                return httpx.Response(200, json=index_2030)
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        periods = client.list_periods()
        assert periods == [202503, 202412, 202409]

    def test_returns_descending_order(self) -> None:
        index = [
            _make_index_entry(202403),
            _make_index_entry(202406),
            _make_index_entry(202409),
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if "relatorios2000a2024" in path:
                return httpx.Response(200, json=index)
            if "relatorios2025a2030" in path:
                return httpx.Response(200, json=[])
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        periods = client.list_periods()
        assert periods == [202409, 202406, 202403]


class TestFetchCadastro:
    def test_parses_cadastro_entries(self) -> None:
        index_data = [_make_index_entry(202412)]
        cadastro_data = [
            _make_cadastro_entry(c0="100", c2="BANCO A", c12="S1"),
            _make_cadastro_entry(c0="200", c2="BANCO B", c12="S2"),
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if "relatorios2000a2024" in path:
                return httpx.Response(200, json=index_data)
            if "relatorios2025a2030" in path:
                return httpx.Response(200, json=[])
            if "arquivos" in path:
                return httpx.Response(200, json=cadastro_data)
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_cadastro(202412)
        assert len(result) == 2
        assert result[0].cod_conglomerado == 100
        assert result[0].nome_conglomerado == "BANCO A"
        assert result[0].segmento == "S1"
        assert result[0].tipo_instituicao == 1009
        assert result[1].cod_conglomerado == 200

    def test_raises_data_not_found_for_missing_period(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if "relatorios" in path:
                return httpx.Response(200, json=[])
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        with pytest.raises(DataNotFoundError):
            client.fetch_cadastro(209912)


class TestFetchReportValues:
    def _build_handler(self) -> tuple[httpx.MockTransport, int]:
        """Build a mock handler with Resumo report data for 202412."""
        ano_mes = 202412
        trel_id = 104

        index_data = [_make_index_entry(
            ano_mes, trel_ids=[(trel_id, "Resumo")],
        )]
        info_data = [
            _make_info_entry(78182, "Ativo Total", group=1),
            _make_info_entry(78186, "Patrimônio Líquido", group=1),
        ]
        trel_data = _make_trel(trel_id, "Resumo", [
            (78182, 9),   # Ativo Total
            (78186, 14),  # PL
        ])
        cadastro_data = [
            _make_cadastro_entry(c0="100", c2="BANCO A"),
        ]
        dados_data = _make_dados(1, [
            _make_entity(100, [(78182, 1000000.0), (78186, 500000.0)]),
            _make_entity(999, [(78182, 9999.0)]),  # Not in cadastro
        ])

        def handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if "relatorios2000a2024" in url_str:
                return httpx.Response(200, json=index_data)
            if "relatorios2025a2030" in url_str:
                return httpx.Response(200, json=[])
            if "arquivos" in url_str:
                nome = request.url.params.get("nomeArquivo", "")
                if "trel" in nome:
                    return httpx.Response(200, json=trel_data)
                if "info" in nome:
                    return httpx.Response(200, json=info_data)
                if "cadastro" in nome:
                    return httpx.Response(200, json=cadastro_data)
                if "dados" in nome:
                    return httpx.Response(200, json=dados_data)
            return httpx.Response(404)

        return httpx.MockTransport(handler), ano_mes

    def test_cross_references_trel_info_dados(self) -> None:
        transport, ano_mes = self._build_handler()
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_report_values(ano_mes, 1009, "1")
        assert len(result) == 2  # 1 entity × 2 columns
        ativo = next(r for r in result if r.nome_linha == "Ativo Total")
        assert ativo.valor_a == 1000000.0
        assert ativo.cod_conglomerado == 100
        pl = next(r for r in result if r.nome_linha == "Patrimônio Líquido")
        assert pl.valor_a == 500000.0

    def test_filters_entities_by_cadastro(self) -> None:
        transport, ano_mes = self._build_handler()
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_report_values(ano_mes, 1009, "1")
        entity_codes = {r.cod_conglomerado for r in result}
        assert 100 in entity_codes
        assert 999 not in entity_codes  # Not in cadastro

    def test_handles_null_values(self) -> None:
        ano_mes = 202412
        trel_id = 104
        index_data = [_make_index_entry(
            ano_mes, trel_ids=[(trel_id, "Resumo")],
        )]
        info_data = [_make_info_entry(78182, "Ativo Total", group=1)]
        trel_data = _make_trel(trel_id, "Resumo", [(78182, 9)])
        cadastro_data = [_make_cadastro_entry(c0="100")]
        # Entity has no value for 78182
        dados_data = _make_dados(1, [_make_entity(100, [])])

        def handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            if "relatorios2000a2024" in url_str:
                return httpx.Response(200, json=index_data)
            if "relatorios2025a2030" in url_str:
                return httpx.Response(200, json=[])
            if "arquivos" in url_str:
                nome = request.url.params.get("nomeArquivo", "")
                if "trel" in nome:
                    return httpx.Response(200, json=trel_data)
                if "info" in nome:
                    return httpx.Response(200, json=info_data)
                if "cadastro" in nome:
                    return httpx.Response(200, json=cadastro_data)
                if "dados" in nome:
                    return httpx.Response(200, json=dados_data)
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_report_values(ano_mes, 1009, "1")
        assert len(result) == 1
        assert result[0].valor_a is None

    def test_returns_empty_for_unknown_report(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if "relatorios" in str(request.url):
                return httpx.Response(200, json=[])
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        result = client.fetch_report_values(202412, 1009, "99")
        assert result == []


class TestErrorHandling:
    def test_raises_api_fetch_error_on_non_200(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if "relatorios" in str(request.url):
                return httpx.Response(500, text="Internal Server Error")
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        with pytest.raises(APIFetchError) as exc_info:
            client.list_periods()
        assert exc_info.value.status_code == 500

    def test_retries_on_transport_error(self) -> None:
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.ConnectError("connection refused")
            if "relatorios2000a2024" in str(request.url):
                return httpx.Response(200, json=[_make_index_entry(202412)])
            return httpx.Response(200, json=[])

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        client = IFDataClient(settings)
        client._client = httpx.Client(transport=transport)

        periods = client.list_periods()
        assert 202412 in periods
        # 2 failures + 1 success for first endpoint + 1 for second endpoint = 4
        assert call_count == 4
