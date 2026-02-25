from __future__ import annotations

import duckdb

from src.ingest import ingest_cadastro, ingest_report_values
from src.models import InstitutionRecord, ReportValue
from src.queries import (
    compare_institutions,
    get_available_indicators,
    get_available_periods,
    get_capital_indicators,
    get_segment_ranking,
    get_summary_indicators,
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
