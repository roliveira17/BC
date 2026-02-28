from __future__ import annotations

import duckdb
import pytest

from src.ingest import ingest_cadastro, ingest_report_values
from src.models import InstitutionRecord, ReportValue
from src.queries import get_financial_ratios, get_market_share_pl, get_ratio_ranking


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


# --- Report 1 indicator names that match the LIKE patterns in queries.py ---
NOME_ATIVO_TOTAL = "Ativo Total"
NOME_LUCRO_LIQUIDO = "Lucro Liquido"
NOME_PATRIMONIO_LIQUIDO = "Patrimonio Liquido"
NOME_CARTEIRA_CREDITO = "Carteira de Credito"
NOME_CAPTACOES = "Captacoes"
NOME_TVM = "Titulos e Valores Mobiliarios"
NOME_PASSIVO_EXIGIVEL = "Passivo Exigivel"
NOME_PR = "Referencia para Comparacao"

# --- Report 5 indicator names ---
NOME_BASILEIA = "Indice de Basileia"
NOME_CAPITAL_PRINCIPAL = "Indice de Capital Principal"
NOME_CAPITAL_NIVEL1 = "Capital Nivel I"
NOME_RAZAO_ALAVANCAGEM = "Razao de Alavancagem"

# --- Report 4 indicator names ---
NOME_RESULTADO_ANTES = "Resultado antes da Tributacao"
NOME_IR_CSLL = "Imposto de Renda e CSLL"


def _make_report1_records(
    cod: int,
    *,
    ativo: float = 10000.0,
    lucro: float = 150.0,
    pl: float = 1000.0,
    carteira: float = 600.0,
    captacoes: float = 1000.0,
    tvm: float = 2000.0,
    passivo_exigivel: float = 9000.0,
    pr: float = 500.0,
) -> list[ReportValue]:
    """Build Report 1 records for one institution (without inserting)."""
    return [
        _report_val(cod, ativo, NOME_ATIVO_TOTAL),
        _report_val(cod, lucro, NOME_LUCRO_LIQUIDO),
        _report_val(cod, pl, NOME_PATRIMONIO_LIQUIDO),
        _report_val(cod, carteira, NOME_CARTEIRA_CREDITO),
        _report_val(cod, captacoes, NOME_CAPTACOES),
        _report_val(cod, tvm, NOME_TVM),
        _report_val(cod, passivo_exigivel, NOME_PASSIVO_EXIGIVEL),
        _report_val(cod, pr, NOME_PR),
    ]


def _seed_report1(
    con: duckdb.DuckDBPyConnection,
    cod: int,
    ano_mes: int,
    **kwargs: float,
) -> None:
    """Insert a full set of Report 1 lines for one institution/period."""
    ingest_report_values(con, _make_report1_records(cod, **kwargs), ano_mes, "1")


def _seed_report5(
    con: duckdb.DuckDBPyConnection,
    cod: int,
    ano_mes: int,
    *,
    basileia: float = 0.15,
    capital_principal: float = 0.10,
    capital_nivel1: float = 0.12,
    razao_alavancagem: float = 0.08,
) -> None:
    """Insert a full set of Report 5 lines for one institution/period."""
    records = [
        _report_val(cod, basileia, NOME_BASILEIA),
        _report_val(cod, capital_principal, NOME_CAPITAL_PRINCIPAL),
        _report_val(cod, capital_nivel1, NOME_CAPITAL_NIVEL1),
        _report_val(cod, razao_alavancagem, NOME_RAZAO_ALAVANCAGEM),
    ]
    ingest_report_values(con, records, ano_mes, "5")


def _seed_report4(
    con: duckdb.DuckDBPyConnection,
    cod: int,
    ano_mes: int,
    *,
    resultado_antes: float = 1000.0,
    ir_csll: float = -300.0,
) -> None:
    """Insert a full set of Report 4 lines for one institution/period."""
    records = [
        _report_val(cod, resultado_antes, NOME_RESULTADO_ANTES),
        _report_val(cod, ir_csll, NOME_IR_CSLL),
    ]
    ingest_report_values(con, records, ano_mes, "4")


def _seed_cadastro(
    con: duckdb.DuckDBPyConnection,
    cod: int,
    ano_mes: int,
    segmento: str = "S1",
) -> None:
    ingest_cadastro(con, [_institution(cod, cod * 10, segmento)], ano_mes)


def _seed_multi_cadastro(
    con: duckdb.DuckDBPyConnection,
    cods: list[int],
    ano_mes: int,
) -> None:
    """Insert cadastro for multiple institutions in one call."""
    records = [_institution(cod, cod * 10) for cod in cods]
    ingest_cadastro(con, records, ano_mes)


# ============================================================
# Tests: get_financial_ratios
# ============================================================


class TestGetFinancialRatios:
    def test_financial_ratios_returns_all_columns(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202406)
        _seed_report5(db_con, 1, 202406)
        _seed_report4(db_con, 1, 202406)
        result = get_financial_ratios(db_con, 1)
        expected_cols = {
            "ano_mes", "roe", "roa", "loan_to_deposit", "credit_intensity",
            "securities_share", "leverage", "debt_equity",
            "funding_dependency", "pr_coverage", "basileia",
            "capital_principal", "capital_nivel1", "capital_excess",
            "razao_alavancagem", "tax_rate",
        }
        assert expected_cols.issubset(set(result.columns))

    def test_financial_ratios_roe_calculation(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202406, pl=1000.0, lucro=150.0)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["roe"] == pytest.approx(15.0, rel=1e-6)

    def test_financial_ratios_roa_calculation(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202406, ativo=10000.0, lucro=150.0)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["roa"] == pytest.approx(1.5, rel=1e-6)

    def test_financial_ratios_loan_to_deposit(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202406, carteira=600.0, captacoes=1000.0)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["loan_to_deposit"] == pytest.approx(60.0, rel=1e-6)

    def test_financial_ratios_leverage(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202406, ativo=10000.0, pl=1000.0)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["leverage"] == pytest.approx(10.0, rel=1e-6)

    def test_financial_ratios_debt_equity(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(
            db_con, 1, 202406, passivo_exigivel=9000.0, pl=1000.0
        )
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["debt_equity"] == pytest.approx(9.0, rel=1e-6)

    def test_financial_ratios_basileia_from_report5(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report5(db_con, 1, 202406, basileia=0.15)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["basileia"] == pytest.approx(15.0, rel=1e-6)

    def test_financial_ratios_capital_excess(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report5(db_con, 1, 202406, basileia=0.15)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        # capital_excess = (0.15 - 0.105) * 100 = 4.5
        assert row["capital_excess"] == pytest.approx(4.5, rel=1e-6)

    def test_financial_ratios_tax_rate(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report4(
            db_con, 1, 202406, resultado_antes=1000.0, ir_csll=-300.0
        )
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["tax_rate"] == pytest.approx(-30.0, rel=1e-6)

    def test_financial_ratios_zero_pl_returns_none(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202406, pl=0.0)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["roe"] is None
        assert row["leverage"] is None
        assert row["debt_equity"] is None

    def test_financial_ratios_missing_report5_returns_none(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202406)
        result = get_financial_ratios(db_con, 1)
        row = result.row(0, named=True)
        assert row["basileia"] is None
        assert row["capital_principal"] is None
        assert row["capital_nivel1"] is None
        assert row["capital_excess"] is None

    def test_financial_ratios_multiple_periods(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_report1(db_con, 1, 202403, lucro=100.0, pl=1000.0)
        _seed_report1(db_con, 1, 202406, lucro=200.0, pl=1000.0)
        result = get_financial_ratios(db_con, 1)
        assert result.shape[0] == 2
        periods = result["ano_mes"].to_list()
        assert periods == [202403, 202406]

    def test_financial_ratios_empty_for_unknown_institution(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = get_financial_ratios(db_con, 99999)
        assert result.shape[0] == 0


# ============================================================
# Tests: get_ratio_ranking
# ============================================================


class TestGetRatioRanking:
    def test_ratio_ranking_returns_correct_columns(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_cadastro(db_con, 1, 202406)
        _seed_report1(db_con, 1, 202406, pl=1000.0, lucro=100.0)
        result = get_ratio_ranking(db_con, "roe", 202406)
        expected_cols = {
            "cod_conglomerado", "nome_conglomerado", "segmento", "valor",
        }
        assert set(result.columns) == expected_cols

    def test_ratio_ranking_roe_order(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        # Batch all 3 institutions into a single ingest call per table
        _seed_multi_cadastro(db_con, [1, 2, 3], 202406)
        all_records: list[ReportValue] = []
        for cod, lucro in [(1, 300.0), (2, 100.0), (3, 200.0)]:
            all_records.extend(
                _make_report1_records(cod, pl=1000.0, lucro=lucro)
            )
        ingest_report_values(db_con, all_records, 202406, "1")

        result = get_ratio_ranking(db_con, "roe", 202406)
        assert result.shape[0] == 3
        values = result["valor"].to_list()
        assert values[0] >= values[1] >= values[2]
        assert values[0] == pytest.approx(30.0, rel=1e-6)
        assert values[2] == pytest.approx(10.0, rel=1e-6)

    def test_ratio_ranking_filters_nulls(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_cadastro(db_con, [1, 2], 202406)
        all_records = (
            _make_report1_records(1, pl=1000.0, lucro=100.0)
            + _make_report1_records(2, pl=0.0, lucro=100.0)
        )
        ingest_report_values(db_con, all_records, 202406, "1")

        result = get_ratio_ranking(db_con, "roe", 202406)
        # Institution 2 has PL=0 -> ROE is NULL -> filtered out by HAVING
        assert result.shape[0] == 1
        assert result["cod_conglomerado"][0] == 1

    def test_ratio_ranking_invalid_ratio_returns_empty(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = get_ratio_ranking(db_con, "invalid_ratio", 202406)
        assert result.shape[0] == 0

    def test_ratio_ranking_specific_period(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_cadastro(db_con, 1, 202403)
        _seed_report1(db_con, 1, 202403, pl=1000.0, lucro=100.0)
        _seed_cadastro(db_con, 1, 202406)
        _seed_report1(db_con, 1, 202406, pl=1000.0, lucro=200.0)
        result_q1 = get_ratio_ranking(db_con, "roe", 202403)
        result_q2 = get_ratio_ranking(db_con, "roe", 202406)
        assert result_q1.shape[0] == 1
        assert result_q2.shape[0] == 1
        assert result_q1["valor"][0] == pytest.approx(10.0, rel=1e-6)
        assert result_q2["valor"][0] == pytest.approx(20.0, rel=1e-6)


# ============================================================
# Tests: get_market_share_pl
# ============================================================


class TestGetMarketSharePl:
    def test_market_share_returns_correct_columns(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_cadastro(db_con, 1, 202406)
        _seed_report1(db_con, 1, 202406, pl=1000.0)
        result = get_market_share_pl(db_con, 202406)
        expected_cols = {
            "cod_conglomerado", "nome_conglomerado", "segmento",
            "pl_value", "market_share_pct",
        }
        assert set(result.columns) == expected_cols

    def test_market_share_percentages_sum(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_cadastro(db_con, [1, 2, 3], 202406)
        all_records: list[ReportValue] = []
        for cod, pl_val in [(1, 5000.0), (2, 3000.0), (3, 2000.0)]:
            all_records.extend(_make_report1_records(cod, pl=pl_val))
        ingest_report_values(db_con, all_records, 202406, "1")

        result = get_market_share_pl(db_con, 202406, top_n=3)
        total = sum(result["market_share_pct"].to_list())
        assert total == pytest.approx(100.0, rel=1e-6)

    def test_market_share_ordered_desc(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_cadastro(db_con, [1, 2, 3], 202406)
        all_records: list[ReportValue] = []
        for cod, pl_val in [(1, 2000.0), (2, 5000.0), (3, 3000.0)]:
            all_records.extend(_make_report1_records(cod, pl=pl_val))
        ingest_report_values(db_con, all_records, 202406, "1")

        result = get_market_share_pl(db_con, 202406, top_n=3)
        pls = result["pl_value"].to_list()
        assert pls[0] >= pls[1] >= pls[2]

    def test_market_share_top_n_limit(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        _seed_multi_cadastro(db_con, list(range(1, 6)), 202406)
        all_records: list[ReportValue] = []
        for cod in range(1, 6):
            all_records.extend(
                _make_report1_records(cod, pl=float(cod * 1000))
            )
        ingest_report_values(db_con, all_records, 202406, "1")

        result = get_market_share_pl(db_con, 202406, top_n=3)
        assert result.shape[0] == 3

    def test_market_share_empty_returns_empty(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        result = get_market_share_pl(db_con, 209901)
        assert result.shape[0] == 0
