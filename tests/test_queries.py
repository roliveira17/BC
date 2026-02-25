from __future__ import annotations

import duckdb

from src.ingest import ingest_balancetes, ingest_cadastro, ingest_report_values
from src.models import BalanceteRow, InstitutionRecord, ReportValue
from src.queries import (
    BALANCETES_KPI_MAP,
    COSIF_ATIVO_TOTAL,
    COSIF_DEPOSITOS,
    COSIF_OPERACOES_CREDITO,
    COSIF_PATRIMONIO_LIQUIDO,
    COSIF_RESULTADO_LIQUIDO,
    compare_institutions,
    get_available_indicators,
    get_available_periods,
    get_balancetes_kpi_trend,
    get_balancetes_multi_kpi,
    get_balancetes_top50,
    get_balancetes_trend,
    get_capital_indicators,
    get_segment_ranking,
    get_summary_indicators,
    list_balancetes_periods,
    list_institutions,
)


def _institution(
    cod_conglomerado: int, cod_inst: int, segmento: str = "S1"
) -> InstitutionRecord:
    return InstitutionRecord(
        cod_conglomerado=cod_conglomerado,
        nome_conglomerado=f"BANCO {cod_conglomerado}",
        cod_inst=cod_inst,
        nome_inst=f"BANCO {cod_inst} SA",
        cnpj=f"{cod_inst:014d}",
        segmento=segmento,
        tipo_instituicao=1,
        cidade="SP",
        uf="SP",
    )


def _report_val(
    cod_conglomerado: int,
    valor_a: float,
    nome_linha: str = "Indice de Basileia",
) -> ReportValue:
    return ReportValue(
        cod_conglomerado=cod_conglomerado,
        nome_conglomerado=f"BANCO {cod_conglomerado}",
        codigo_coluna="c1",
        nome_coluna="Coluna 1",
        valor_a=valor_a,
        nome_linha=nome_linha,
        ordenacao=1,
    )


def _seed_data(con: duckdb.DuckDBPyConnection) -> None:
    """Seed test data: 2 institutions, 2 periods, reports 1 and 5."""
    # Cadastro
    ingest_cadastro(
        con,
        [_institution(1, 10, "S1"), _institution(2, 20, "S2")],
        202403,
    )
    ingest_cadastro(
        con,
        [_institution(1, 10, "S1"), _institution(2, 20, "S2")],
        202406,
    )

    # Report 5 (Capital)
    for ano_mes in [202403, 202406]:
        ingest_report_values(
            con,
            [
                _report_val(1, 14.5, "Indice de Basileia"),
                _report_val(2, 12.3, "Indice de Basileia"),
                _report_val(1, 1000.0, "Patrimonio de Referencia"),
                _report_val(2, 800.0, "Patrimonio de Referencia"),
            ],
            ano_mes,
            "5",
        )

    # Report 1 (Summary)
    ingest_report_values(
        con,
        [
            _report_val(1, 500000.0, "Ativo Total"),
            _report_val(2, 300000.0, "Ativo Total"),
        ],
        202406,
        "1",
    )


class TestListInstitutions:
    def test_returns_all(self, db_con: duckdb.DuckDBPyConnection) -> None:
        _seed_data(db_con)
        result = list_institutions(db_con)
        assert result.shape[0] == 2

    def test_filters_by_segment(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = list_institutions(db_con, segmento="S1")
        assert result.shape[0] == 1
        assert result["nome_conglomerado"][0] == "BANCO 1"

    def test_returns_empty_for_unknown_segment(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = list_institutions(db_con, segmento="S5")
        assert result.shape[0] == 0


class TestCompareInstitutions:
    def test_returns_correct_shape(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = compare_institutions(
            db_con, [1, 2], "Indice de Basileia", "5"
        )
        # 2 institutions x 2 periods = 4 rows
        assert result.shape[0] == 4

    def test_empty_list_returns_empty_df(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = compare_institutions(db_con, [], "X", "5")
        assert result.shape[0] == 0

    def test_single_institution(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = compare_institutions(
            db_con, [1], "Indice de Basileia", "5"
        )
        assert result.shape[0] == 2  # 1 institution x 2 periods


class TestGetCapitalIndicators:
    def test_returns_report_5_data(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = get_capital_indicators(db_con, 1)
        assert result.shape[0] > 0
        assert "nome_linha" in result.columns


class TestGetSummaryIndicators:
    def test_returns_report_1_data(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = get_summary_indicators(db_con, 1)
        assert result.shape[0] == 1
        assert result["nome_linha"][0] == "Ativo Total"


class TestSegmentRanking:
    def test_ranks_descending(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = get_segment_ranking(db_con, "Indice de Basileia", "5")
        assert result.shape[0] == 2
        values = result["valor_a"].to_list()
        assert values[0] >= values[1]

    def test_filters_by_segment(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        result = get_segment_ranking(
            db_con, "Indice de Basileia", "5", segmento="S1"
        )
        assert result.shape[0] == 1


class TestAvailablePeriods:
    def test_returns_periods(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        periods = get_available_periods(db_con)
        assert 202406 in periods
        assert 202403 in periods

    def test_empty_db(self, db_con: duckdb.DuckDBPyConnection) -> None:
        periods = get_available_periods(db_con)
        assert periods == []


class TestAvailableIndicators:
    def test_returns_indicators(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_data(db_con)
        indicators = get_available_indicators(db_con, "5")
        assert "Indice de Basileia" in indicators
        assert "Patrimonio de Referencia" in indicators


# --- Balancetes Query Tests ---


def _balancete_row(
    cnpj: str = "00000000000100",
    saldo: float = 1000000.0,
    conta: str = "6.0.0.00.00-2",
    ano_mes: int = 202501,
) -> BalanceteRow:
    return BalanceteRow(
        ano_mes=ano_mes,
        cnpj=cnpj,
        cnpj8=cnpj[:8],
        nome_inst=f"BANCO {cnpj[:8]}",
        atributo="A",
        documento="4040",
        conta=conta,
        nome_conta="Patrimonio Liquido",
        saldo=saldo,
    )


def _seed_balancetes(con: duckdb.DuckDBPyConnection) -> None:
    """Seed balancetes test data: 3 institutions, 2 periods."""
    for ano_mes in [202501, 202502]:
        rows = [
            _balancete_row(cnpj="11111111000100", saldo=5000000.0, ano_mes=ano_mes),
            _balancete_row(cnpj="22222222000100", saldo=3000000.0, ano_mes=ano_mes),
            _balancete_row(cnpj="33333333000100", saldo=1000000.0, ano_mes=ano_mes),
        ]
        ingest_balancetes(con, rows, ano_mes)


class TestBalancetesTop50:
    def test_returns_ranked_data(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_balancetes(db_con)
        result = get_balancetes_top50(db_con, 202501)
        assert result.shape[0] == 3
        pls = result["patrimonio_liquido"].to_list()
        assert pls[0] >= pls[1] >= pls[2]

    def test_uses_latest_period_when_none(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_balancetes(db_con)
        result = get_balancetes_top50(db_con)
        assert result.shape[0] == 3
        assert result["ano_mes"][0] == 202502

    def test_empty_when_no_data(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = get_balancetes_top50(db_con, 202501)
        assert result.shape[0] == 0


class TestBalancetesTrend:
    def test_returns_trend_for_institution(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_balancetes(db_con)
        result = get_balancetes_trend(db_con, "11111111")
        assert result.shape[0] == 2
        assert "patrimonio_liquido" in result.columns

    def test_empty_for_unknown_cnpj8(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_balancetes(db_con)
        result = get_balancetes_trend(db_con, "99999999")
        assert result.shape[0] == 0


class TestBalancetesPeriods:
    def test_returns_periods_descending(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_balancetes(db_con)
        periods = list_balancetes_periods(db_con)
        assert periods == [202502, 202501]

    def test_empty_db(self, db_con: duckdb.DuckDBPyConnection) -> None:
        periods = list_balancetes_periods(db_con)
        assert periods == []


# --- Multi-KPI and KPI Trend Tests ---


def _seed_multi_kpi(con: duckdb.DuckDBPyConnection) -> None:
    """Seed balancetes with multiple COSIF accounts for multi-KPI tests."""
    for ano_mes in [202501, 202502]:
        rows = [
            # PL (6.0.0.00.00-2)
            _balancete_row(cnpj="11111111000100", saldo=5000000.0, ano_mes=ano_mes),
            _balancete_row(cnpj="22222222000100", saldo=3000000.0, ano_mes=ano_mes),
            # Ativo Total (1.0.0.00.00-7)
            _balancete_row(
                cnpj="11111111000100", saldo=20000000.0,
                conta=COSIF_ATIVO_TOTAL, ano_mes=ano_mes,
            ),
            _balancete_row(
                cnpj="22222222000100", saldo=15000000.0,
                conta=COSIF_ATIVO_TOTAL, ano_mes=ano_mes,
            ),
            # Operações de Crédito (1.6.0.00.00-1)
            _balancete_row(
                cnpj="11111111000100", saldo=8000000.0,
                conta=COSIF_OPERACOES_CREDITO, ano_mes=ano_mes,
            ),
            # Depósitos (4.1.0.00.00-7)
            _balancete_row(
                cnpj="11111111000100", saldo=12000000.0,
                conta=COSIF_DEPOSITOS, ano_mes=ano_mes,
            ),
            # Resultado Líquido (7.0.0.00.00-9)
            _balancete_row(
                cnpj="11111111000100", saldo=500000.0,
                conta=COSIF_RESULTADO_LIQUIDO, ano_mes=ano_mes,
            ),
        ]
        ingest_balancetes(con, rows, ano_mes)


class TestBalancetesMultiKpi:
    def test_returns_all_kpi_columns(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_multi_kpi(db_con, 202501)
        expected_cols = {
            "rank", "cnpj8", "nome_inst", "cod_conglomerado",
            "nome_conglomerado", "patrimonio_liquido", "ativo_total",
            "operacoes_credito", "depositos", "resultado_liquido",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_returns_ranked_by_pl(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_multi_kpi(db_con, 202501)
        assert result.shape[0] == 2
        pls = result["patrimonio_liquido"].to_list()
        assert pls[0] >= pls[1]

    def test_additional_kpi_values(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_multi_kpi(db_con, 202501)
        top = result.filter(result["cnpj8"] == "11111111")
        assert top["ativo_total"][0] == 20000000.0
        assert top["operacoes_credito"][0] == 8000000.0
        assert top["depositos"][0] == 12000000.0
        assert top["resultado_liquido"][0] == 500000.0

    def test_missing_kpi_returns_none(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_multi_kpi(db_con, 202501)
        # Second institution has no credito/deposito/resultado data
        second = result.filter(result["cnpj8"] == "22222222")
        assert second["operacoes_credito"][0] is None
        assert second["depositos"][0] is None

    def test_uses_latest_period_when_none(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_multi_kpi(db_con)
        assert result.shape[0] == 2

    def test_empty_when_no_data(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = get_balancetes_multi_kpi(db_con, 202501)
        assert result.shape[0] == 0


class TestBalancetesKpiTrend:
    def test_returns_trend_for_ativo_total(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_kpi_trend(db_con, "11111111", COSIF_ATIVO_TOTAL)
        assert result.shape[0] == 2
        assert "valor" in result.columns
        assert result["valor"][0] == 20000000.0

    def test_returns_trend_for_credito(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_kpi_trend(db_con, "11111111", COSIF_OPERACOES_CREDITO)
        assert result.shape[0] == 2
        assert result["valor"][0] == 8000000.0

    def test_empty_for_missing_account(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        # Institution 2 has no credit data
        result = get_balancetes_kpi_trend(db_con, "22222222", COSIF_OPERACOES_CREDITO)
        assert result.shape[0] == 0

    def test_empty_for_unknown_cnpj8(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_kpi(db_con)
        result = get_balancetes_kpi_trend(db_con, "99999999", COSIF_ATIVO_TOTAL)
        assert result.shape[0] == 0


class TestBalancetesKpiMap:
    def test_contains_expected_kpis(self) -> None:
        assert "Patrimônio Líquido" in BALANCETES_KPI_MAP
        assert "Ativo Total" in BALANCETES_KPI_MAP
        assert "Operações de Crédito" in BALANCETES_KPI_MAP
        assert "Depósitos" in BALANCETES_KPI_MAP
        assert "Resultado Líquido" in BALANCETES_KPI_MAP

    def test_maps_to_correct_cosif_codes(self) -> None:
        assert BALANCETES_KPI_MAP["Patrimônio Líquido"] == COSIF_PATRIMONIO_LIQUIDO
        assert BALANCETES_KPI_MAP["Ativo Total"] == COSIF_ATIVO_TOTAL
        assert BALANCETES_KPI_MAP["Operações de Crédito"] == COSIF_OPERACOES_CREDITO
        assert BALANCETES_KPI_MAP["Depósitos"] == COSIF_DEPOSITOS
        assert BALANCETES_KPI_MAP["Resultado Líquido"] == COSIF_RESULTADO_LIQUIDO
