from __future__ import annotations

import duckdb

from src.ingest import (
    generate_quarter_periods,
    ingest_cadastro,
    ingest_report_values,
    is_period_fetched,
)
from src.models import InstitutionRecord, ReportValue


def _make_institution(
    cod_conglomerado: int = 1,
    cod_inst: int = 10,
    segmento: str = "S1",
) -> InstitutionRecord:
    return InstitutionRecord(
        cod_conglomerado=cod_conglomerado,
        nome_conglomerado="BANCO TEST",
        cod_inst=cod_inst,
        nome_inst="BANCO TEST SA",
        cnpj="00000000000100",
        segmento=segmento,
        tipo_instituicao=1,
        cidade="SP",
        uf="SP",
    )


def _make_report_value(
    cod_conglomerado: int = 1,
    valor_a: float = 15.5,
    nome_linha: str = "Indice de Basileia",
) -> ReportValue:
    return ReportValue(
        cod_conglomerado=cod_conglomerado,
        nome_conglomerado="BANCO TEST",
        codigo_coluna="c1",
        nome_coluna="Coluna 1",
        valor_a=valor_a,
        nome_linha=nome_linha,
        ordenacao=1,
    )


class TestGenerateQuarterPeriods:
    def test_returns_correct_count(self) -> None:
        periods = generate_quarter_periods(4)
        assert len(periods) == 4

    def test_returns_descending_order(self) -> None:
        periods = generate_quarter_periods(8)
        for i in range(len(periods) - 1):
            assert periods[i] > periods[i + 1]

    def test_all_periods_are_valid_quarters(self) -> None:
        periods = generate_quarter_periods(20)
        for p in periods:
            month = p % 100
            assert month in (3, 6, 9, 12), f"Invalid quarter month: {month}"


class TestIsPeriodFetched:
    def test_returns_false_when_not_fetched(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        assert is_period_fetched(db_con, 202409, "5") is False

    def test_returns_true_after_ingest(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        records = [_make_report_value()]
        ingest_report_values(db_con, records, 202409, "5")
        assert is_period_fetched(db_con, 202409, "5") is True

    def test_different_relatorio_not_fetched(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        records = [_make_report_value()]
        ingest_report_values(db_con, records, 202409, "5")
        assert is_period_fetched(db_con, 202409, "1") is False


class TestIngestCadastro:
    def test_inserts_records(self, db_con: duckdb.DuckDBPyConnection) -> None:
        records = [_make_institution(cod_inst=1), _make_institution(cod_inst=2)]
        count = ingest_cadastro(db_con, records, 202409)
        assert count == 2

        rows = db_con.execute(
            "SELECT COUNT(*) FROM cadastro WHERE ano_mes = 202409"
        ).fetchone()
        assert rows is not None
        assert rows[0] == 2

    def test_is_idempotent(self, db_con: duckdb.DuckDBPyConnection) -> None:
        records = [_make_institution()]
        ingest_cadastro(db_con, records, 202409)
        ingest_cadastro(db_con, records, 202409)

        rows = db_con.execute(
            "SELECT COUNT(*) FROM cadastro WHERE ano_mes = 202409"
        ).fetchone()
        assert rows is not None
        assert rows[0] == 1

    def test_empty_records_returns_zero(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        count = ingest_cadastro(db_con, [], 202409)
        assert count == 0


class TestIngestReportValues:
    def test_inserts_records(self, db_con: duckdb.DuckDBPyConnection) -> None:
        records = [_make_report_value(valor_a=100.0)]
        count = ingest_report_values(db_con, records, 202409, "5")
        assert count == 1

    def test_is_idempotent(self, db_con: duckdb.DuckDBPyConnection) -> None:
        records = [_make_report_value(valor_a=100.0)]
        ingest_report_values(db_con, records, 202409, "5")
        ingest_report_values(db_con, records, 202409, "5")

        rows = db_con.execute(
            "SELECT COUNT(*) FROM report_values "
            "WHERE ano_mes = 202409 AND relatorio = '5'"
        ).fetchone()
        assert rows is not None
        assert rows[0] == 1

    def test_updates_fetch_log(
        self, db_con: duckdb.DuckDBPyConnection
    ) -> None:
        records = [_make_report_value(), _make_report_value(cod_conglomerado=2)]
        ingest_report_values(db_con, records, 202409, "5")

        log_row = db_con.execute(
            "SELECT row_count FROM fetch_log "
            "WHERE ano_mes = 202409 AND relatorio = '5'"
        ).fetchone()
        assert log_row is not None
        assert log_row[0] == 2
