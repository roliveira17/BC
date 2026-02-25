from __future__ import annotations

from pydantic import BaseModel, Field

# --- API Response Models ---


class InstitutionRecord(BaseModel):  # type: ignore[misc]
    """Single record from IfDataCadastro endpoint."""

    cod_conglomerado: int = Field(alias="CodConglomerado")
    nome_conglomerado: str = Field(alias="NomeConglomerado")
    cod_inst: int = Field(alias="CodInst")
    nome_inst: str = Field(alias="NomeInst")
    cnpj: str = Field(alias="CNPJ")
    segmento: str = Field(alias="Segmento")
    tipo_instituicao: int = Field(alias="TipoInstituicao")
    cidade: str = Field(alias="Cidade", default="")
    uf: str = Field(alias="UF", default="")

    model_config = {"populate_by_name": True}


class ReportValue(BaseModel):  # type: ignore[misc]
    """Single record from IfDataValores endpoint."""

    cod_conglomerado: int = Field(alias="CodConglomerado")
    nome_conglomerado: str = Field(alias="NomeConglomerado")
    codigo_coluna: str = Field(alias="CodigoColuna")
    nome_coluna: str = Field(alias="NomeColuna")
    valor_a: float | None = Field(alias="ValorA", default=None)
    nome_linha: str = Field(alias="NomeLinha")
    ordenacao: int = Field(alias="Ordenacao")

    model_config = {"populate_by_name": True}


class ReportCatalogEntry(BaseModel):  # type: ignore[misc]
    """Single record from ListaDeRelatorio endpoint."""

    tipo_instituicao: str = Field(alias="TipoInstituicao")
    tipo_instituicao_descricao: str = Field(alias="TipoInstituicaoDescricao")
    relatorio: str = Field(alias="Relatorio")
    relatorio_descricao: str = Field(alias="RelatorioDescricao")
    codigo_coluna: str = Field(alias="CodigoColuna")
    nome_coluna: str = Field(alias="NomeColuna")

    model_config = {"populate_by_name": True}


# --- Typed Errors ---


class IFDataError(Exception):
    """Base error for IF.data operations."""


class APIFetchError(IFDataError):
    """Failed to fetch data from IF.data API."""

    def __init__(self, endpoint: str, status_code: int, detail: str) -> None:
        self.endpoint = endpoint
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"{endpoint} returned {status_code}: {detail}")


class DataNotFoundError(IFDataError):
    """Requested data period not available."""

    def __init__(self, ano_mes: int) -> None:
        self.ano_mes = ano_mes
        super().__init__(f"No data available for period {ano_mes}")


# --- Balancetes 4040 Models ---


class BalanceteRow(BaseModel):  # type: ignore[misc]
    """Single parsed row from a balancete CSV (filtered to Documento=4040)."""

    ano_mes: int
    cnpj: str
    cnpj8: str
    nome_inst: str
    atributo: str
    documento: str
    conta: str
    nome_conta: str
    saldo: float


class BalancetesError(Exception):
    """Base error for balancetes operations."""


class ZipDownloadError(BalancetesError):
    """Failed to download balancete ZIP file."""

    def __init__(self, url: str, status_code: int) -> None:
        self.url = url
        self.status_code = status_code
        super().__init__(f"Download failed for {url}: HTTP {status_code}")


class ZipNotAvailableError(BalancetesError):
    """Balancete ZIP not yet published (404)."""

    def __init__(self, ano_mes: int) -> None:
        self.ano_mes = ano_mes
        super().__init__(f"Balancete not available for period {ano_mes}")


# --- Domain Types ---


class QuarterPeriod(BaseModel):  # type: ignore[misc]
    """Represents a quarterly reporting period as AAAAMM."""

    ano_mes: int

    @property
    def year(self) -> int:
        return self.ano_mes // 100

    @property
    def month(self) -> int:
        return self.ano_mes % 100

    @property
    def label(self) -> str:
        q_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
        return f"{self.year} {q_map.get(self.month, f'M{self.month}')}"
