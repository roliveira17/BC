from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    APIFetchError,
    DataNotFoundError,
    InstitutionRecord,
    QuarterPeriod,
    ReportCatalogEntry,
    ReportValue,
)


class TestInstitutionRecord:
    def test_parses_api_response(self) -> None:
        raw = {
            "CodConglomerado": 1234,
            "NomeConglomerado": "BANCO DO BRASIL",
            "CodInst": 5678,
            "NomeInst": "BANCO DO BRASIL SA",
            "CNPJ": "00000000000191",
            "Segmento": "S1",
            "TipoInstituicao": 1,
            "Cidade": "BRASILIA",
            "UF": "DF",
        }
        record = InstitutionRecord.model_validate(raw)
        assert record.segmento == "S1"
        assert record.cod_conglomerado == 1234
        assert record.nome_inst == "BANCO DO BRASIL SA"
        assert record.uf == "DF"

    def test_rejects_missing_required_field(self) -> None:
        raw = {
            "CodConglomerado": 1234,
            "NomeConglomerado": "BANCO DO BRASIL",
            # Missing CodInst and others
        }
        with pytest.raises(ValidationError):
            InstitutionRecord.model_validate(raw)

    def test_defaults_cidade_uf_when_missing(self) -> None:
        raw = {
            "CodConglomerado": 1,
            "NomeConglomerado": "TEST",
            "CodInst": 2,
            "NomeInst": "TEST SA",
            "CNPJ": "123",
            "Segmento": "S2",
            "TipoInstituicao": 1,
        }
        record = InstitutionRecord.model_validate(raw)
        assert record.cidade == ""
        assert record.uf == ""


class TestReportValue:
    def test_parses_api_response(self) -> None:
        raw = {
            "CodConglomerado": 100,
            "NomeConglomerado": "ITAU UNIBANCO",
            "CodigoColuna": "col1",
            "NomeColuna": "Coluna 1",
            "ValorA": 42.5,
            "NomeLinha": "Indice de Basileia",
            "Ordenacao": 1,
        }
        record = ReportValue.model_validate(raw)
        assert record.valor_a == 42.5
        assert record.nome_linha == "Indice de Basileia"

    def test_allows_null_valor_a(self) -> None:
        raw = {
            "CodConglomerado": 100,
            "NomeConglomerado": "ITAU",
            "CodigoColuna": "col1",
            "NomeColuna": "Coluna 1",
            "ValorA": None,
            "NomeLinha": "Linha",
            "Ordenacao": 1,
        }
        record = ReportValue.model_validate(raw)
        assert record.valor_a is None


class TestReportCatalogEntry:
    def test_parses_catalog(self) -> None:
        raw = {
            "TipoInstituicao": "1",
            "TipoInstituicaoDescricao": "Conglomerado Financeiro",
            "Relatorio": "5",
            "RelatorioDescricao": "Informacoes de Capital",
            "CodigoColuna": "col_cap",
            "NomeColuna": "Capital",
        }
        entry = ReportCatalogEntry.model_validate(raw)
        assert entry.relatorio == "5"
        assert entry.relatorio_descricao == "Informacoes de Capital"


class TestQuarterPeriod:
    def test_year_and_month(self) -> None:
        q = QuarterPeriod(ano_mes=202409)
        assert q.year == 2024
        assert q.month == 9

    def test_label_q3(self) -> None:
        q = QuarterPeriod(ano_mes=202409)
        assert q.label == "2024 Q3"

    def test_label_q4(self) -> None:
        q = QuarterPeriod(ano_mes=202312)
        assert q.label == "2023 Q4"

    def test_label_q1(self) -> None:
        q = QuarterPeriod(ano_mes=202503)
        assert q.label == "2025 Q1"


class TestErrors:
    def test_api_fetch_error_message(self) -> None:
        err = APIFetchError("IfDataValores", 500, "Internal Server Error")
        assert "IfDataValores" in str(err)
        assert "500" in str(err)
        assert err.endpoint == "IfDataValores"
        assert err.status_code == 500

    def test_data_not_found_error(self) -> None:
        err = DataNotFoundError(202403)
        assert err.ano_mes == 202403
        assert "202403" in str(err)
