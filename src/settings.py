from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):  # type: ignore[misc]
    """Application settings loaded from environment variables with BC_ prefix."""

    # IF.data REST API
    ifdata_base_url: str = Field(
        default="https://www3.bcb.gov.br/ifdata/rest",
    )
    ifdata_timeout_sec: int = Field(default=120)
    ifdata_tipo_inst_id: int = Field(
        default=1009,
        description="1009=Congl. Prudencial, 1005=Congl. Financeiro, 1006=Individual",
    )
    ifdata_relatorios: list[str] = Field(
        default=["1", "4", "5"],
        description="Report codes: 1=Resumo, 4=DRE, 5=Capital",
    )
    ifdata_max_retries: int = Field(default=3)

    # DuckDB
    duckdb_path: str = Field(default="data/bc_dashboard.duckdb")

    # Data range
    history_quarters: int = Field(
        default=20,
        description="Number of quarterly periods to fetch (5 years)",
    )

    # Balancetes 4040
    balancetes_base_url: str = Field(
        default="https://www4.bcb.gov.br/fis/cosif/cont/balan/bancos",
    )
    balancetes_timeout_sec: int = Field(default=120)
    balancetes_max_retries: int = Field(default=3)
    balancetes_history_months: int = Field(
        default=24,
        description="Number of monthly periods to fetch for balancetes",
    )

    # Dashboard
    dash_debug: bool = Field(default=False)
    dash_host: str = Field(default="0.0.0.0")
    dash_port: int = Field(default=8050)

    model_config = {"env_prefix": "BC_"}
