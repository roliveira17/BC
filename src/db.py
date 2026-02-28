from __future__ import annotations

import os

import duckdb
import structlog

logger = structlog.get_logger()

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cadastro (
    ano_mes          INTEGER NOT NULL,
    cod_conglomerado INTEGER NOT NULL,
    nome_conglomerado VARCHAR NOT NULL,
    cod_inst         INTEGER NOT NULL,
    nome_inst        VARCHAR NOT NULL,
    cnpj             VARCHAR NOT NULL,
    segmento         VARCHAR NOT NULL,
    tipo_instituicao INTEGER NOT NULL,
    cidade           VARCHAR,
    uf               VARCHAR(2),
    PRIMARY KEY (ano_mes, cod_inst)
);

CREATE TABLE IF NOT EXISTS report_values (
    ano_mes          INTEGER NOT NULL,
    relatorio        VARCHAR NOT NULL,
    cod_conglomerado INTEGER NOT NULL,
    nome_conglomerado VARCHAR NOT NULL,
    codigo_coluna    VARCHAR NOT NULL,
    nome_coluna      VARCHAR NOT NULL,
    valor_a          DOUBLE,
    nome_linha       VARCHAR NOT NULL,
    ordenacao        INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rv_lookup
    ON report_values (ano_mes, relatorio, cod_conglomerado);

CREATE INDEX IF NOT EXISTS idx_cadastro_segment
    ON cadastro (ano_mes, segmento);

CREATE TABLE IF NOT EXISTS fetch_log (
    ano_mes    INTEGER NOT NULL,
    relatorio  VARCHAR NOT NULL,
    fetched_at TIMESTAMP DEFAULT current_timestamp,
    row_count  INTEGER NOT NULL,
    PRIMARY KEY (ano_mes, relatorio)
);

CREATE TABLE IF NOT EXISTS balancetes_raw (
    ano_mes    INTEGER NOT NULL,
    cnpj       VARCHAR NOT NULL,
    cnpj8      VARCHAR NOT NULL,
    nome_inst  VARCHAR NOT NULL,
    atributo   VARCHAR NOT NULL,
    documento  VARCHAR NOT NULL,
    conta      VARCHAR NOT NULL,
    nome_conta VARCHAR NOT NULL,
    saldo      DOUBLE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bal_raw_lookup
    ON balancetes_raw (ano_mes, conta, cnpj8);

CREATE TABLE IF NOT EXISTS institution_mapping (
    cnpj8              VARCHAR NOT NULL,
    nome_inst          VARCHAR NOT NULL,
    cod_conglomerado   INTEGER,
    nome_conglomerado  VARCHAR,
    PRIMARY KEY (cnpj8)
);

CREATE TABLE IF NOT EXISTS balancetes_top50 (
    ano_mes            INTEGER NOT NULL,
    rank               INTEGER NOT NULL,
    cnpj8              VARCHAR NOT NULL,
    nome_inst          VARCHAR NOT NULL,
    cod_conglomerado   INTEGER,
    nome_conglomerado  VARCHAR,
    patrimonio_liquido DOUBLE NOT NULL,
    PRIMARY KEY (ano_mes, rank)
);
"""


def get_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open a persistent DuckDB connection and ensure schema exists."""
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    logger.info("duckdb_schema_initialized", path=db_path)
    return con


def get_memory_connection() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with schema. Useful for tests."""
    con = duckdb.connect(":memory:")
    con.execute(SCHEMA_SQL)
    return con
