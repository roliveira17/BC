from __future__ import annotations

import duckdb
import polars as pl

# COSIF account codes for balancetes
COSIF_PATRIMONIO_LIQUIDO = "6.0.0.00.00-2"
COSIF_ATIVO_TOTAL = "1.0.0.00.00-7"
COSIF_RESULTADO_LIQUIDO = "7.0.0.00.00-9"
COSIF_OPERACOES_CREDITO = "1.6.0.00.00-1"
COSIF_DEPOSITOS = "4.1.0.00.00-7"

# Friendly name → COSIF account code mapping for balancetes KPIs
BALANCETES_KPI_MAP: dict[str, str] = {
    "Patrimônio Líquido": COSIF_PATRIMONIO_LIQUIDO,
    "Ativo Total": COSIF_ATIVO_TOTAL,
    "Operações de Crédito": COSIF_OPERACOES_CREDITO,
    "Depósitos": COSIF_DEPOSITOS,
    "Resultado Líquido": COSIF_RESULTADO_LIQUIDO,
}


def list_institutions(
    con: duckdb.DuckDBPyConnection,
    segmento: str | None = None,
) -> pl.DataFrame:
    """Return distinct institutions, optionally filtered by segment.

    Uses the most recent cadastro period available.
    Returns: DataFrame [cod_conglomerado, nome_conglomerado, segmento]
    """
    params: list[str] = []
    seg_filter = ""
    if segmento and segmento != "ALL":
        seg_filter = "AND segmento = ?"
        params.append(segmento)

    sql = f"""
        SELECT DISTINCT cod_conglomerado, nome_conglomerado, segmento
        FROM cadastro
        WHERE ano_mes = (SELECT MAX(ano_mes) FROM cadastro)
        {seg_filter}
        ORDER BY nome_conglomerado
    """
    return con.execute(sql, params).pl()


def get_institution_indicators(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
    relatorio: str | None = None,
) -> pl.DataFrame:
    """All indicators over time for a single institution.

    Returns: DataFrame [ano_mes, relatorio, nome_linha, nome_coluna, valor_a, ordenacao]
    """
    params: list[int | str] = [cod_conglomerado]
    rel_filter = ""
    if relatorio:
        rel_filter = "AND relatorio = ?"
        params.append(relatorio)

    sql = f"""
        SELECT ano_mes, relatorio, nome_linha, nome_coluna, valor_a, ordenacao
        FROM report_values
        WHERE cod_conglomerado = ?
        {rel_filter}
        ORDER BY ano_mes, ordenacao
    """
    return con.execute(sql, params).pl()


def get_capital_indicators(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
) -> pl.DataFrame:
    """Capital indicators over time for one institution (report 5).

    Returns: DataFrame [ano_mes, nome_linha, valor_a]
    """
    sql = """
        SELECT ano_mes, nome_linha, valor_a
        FROM report_values
        WHERE cod_conglomerado = ?
          AND relatorio = '5'
        ORDER BY ano_mes, ordenacao
    """
    return con.execute(sql, [cod_conglomerado]).pl()


def compare_institutions(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerados: list[int],
    indicator_name: str,
    relatorio: str,
) -> pl.DataFrame:
    """Compare a specific indicator across multiple institutions over time.

    Returns: DataFrame [ano_mes, cod_conglomerado, nome_conglomerado, valor_a]
    """
    if not cod_conglomerados:
        return pl.DataFrame(
            schema={
                "ano_mes": pl.Int64,
                "cod_conglomerado": pl.Int64,
                "nome_conglomerado": pl.Utf8,
                "valor_a": pl.Float64,
            }
        )
    placeholders = ", ".join(["?"] * len(cod_conglomerados))
    sql = f"""
        SELECT rv.ano_mes, rv.cod_conglomerado, rv.nome_conglomerado, rv.valor_a
        FROM report_values rv
        WHERE rv.relatorio = ?
          AND rv.nome_linha = ?
          AND rv.cod_conglomerado IN ({placeholders})
        ORDER BY rv.ano_mes, rv.nome_conglomerado
    """
    params: list[str | int] = [relatorio, indicator_name, *cod_conglomerados]
    return con.execute(sql, params).pl()


def get_segment_ranking(
    con: duckdb.DuckDBPyConnection,
    indicator_name: str,
    relatorio: str,
    ano_mes: int | None = None,
    segmento: str | None = None,
) -> pl.DataFrame:
    """Rank institutions by a given indicator for a specific period.

    If ano_mes is None, uses the most recent period.
    Returns: DataFrame [cod_conglomerado, nome_conglomerado, segmento, valor_a]
    """
    params: list[str | int] = [relatorio, indicator_name]

    period_clause = "rv.ano_mes = (SELECT MAX(ano_mes) FROM report_values)"
    if ano_mes is not None:
        period_clause = "rv.ano_mes = ?"
        params.append(ano_mes)

    seg_filter = ""
    if segmento and segmento != "ALL":
        seg_filter = "AND c.segmento = ?"
        params.append(segmento)

    sql = f"""
        SELECT rv.cod_conglomerado, rv.nome_conglomerado,
               COALESCE(c.segmento, '') as segmento, rv.valor_a
        FROM report_values rv
        LEFT JOIN cadastro c
            ON c.cod_conglomerado = rv.cod_conglomerado
            AND c.ano_mes = rv.ano_mes
        WHERE rv.relatorio = ?
          AND rv.nome_linha = ?
          AND {period_clause}
          {seg_filter}
          AND rv.valor_a IS NOT NULL
        ORDER BY rv.valor_a DESC
    """
    return con.execute(sql, params).pl()


def get_available_periods(con: duckdb.DuckDBPyConnection) -> list[int]:
    """Return all AAAAMM periods that have report data, descending."""
    result = con.execute(
        "SELECT DISTINCT ano_mes FROM report_values ORDER BY ano_mes DESC"
    ).fetchall()
    return [row[0] for row in result]


def get_available_indicators(
    con: duckdb.DuckDBPyConnection,
    relatorio: str,
) -> list[str]:
    """Return distinct indicator names (nome_linha) for a report type."""
    result = con.execute(
        "SELECT DISTINCT nome_linha FROM report_values "
        "WHERE relatorio = ? ORDER BY nome_linha",
        [relatorio],
    ).fetchall()
    return [row[0] for row in result]


def get_summary_indicators(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
) -> pl.DataFrame:
    """Summary indicators over time for one institution (report 1).

    Returns: DataFrame [ano_mes, nome_linha, valor_a]
    """
    sql = """
        SELECT ano_mes, nome_linha, valor_a
        FROM report_values
        WHERE cod_conglomerado = ?
          AND relatorio = '1'
        ORDER BY ano_mes, ordenacao
    """
    return con.execute(sql, [cod_conglomerado]).pl()


def get_dre_indicators(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
) -> pl.DataFrame:
    """DRE indicators over time for one institution (report 4).

    Returns: DataFrame [ano_mes, nome_linha, valor_a]
    """
    sql = """
        SELECT ano_mes, nome_linha, valor_a
        FROM report_values
        WHERE cod_conglomerado = ?
          AND relatorio = '4'
        ORDER BY ano_mes, ordenacao
    """
    return con.execute(sql, [cod_conglomerado]).pl()


def get_balancetes_top50(
    con: duckdb.DuckDBPyConnection,
    ano_mes: int | None = None,
) -> pl.DataFrame:
    """Return Top 50 institutions by Patrimônio Líquido for a given period.

    If ano_mes is None, uses the most recent available period.
    Returns: DataFrame [ano_mes, rank, cnpj8, nome_inst, cod_conglomerado,
                        nome_conglomerado, patrimonio_liquido]
    """
    params: list[int] = []
    if ano_mes is not None:
        period_clause = "ano_mes = ?"
        params.append(ano_mes)
    else:
        period_clause = "ano_mes = (SELECT MAX(ano_mes) FROM balancetes_top50)"

    sql = f"""
        SELECT ano_mes, rank, cnpj8, nome_inst, cod_conglomerado,
               nome_conglomerado, patrimonio_liquido
        FROM balancetes_top50
        WHERE {period_clause}
        ORDER BY rank
    """
    return con.execute(sql, params).pl()


def get_balancetes_trend(
    con: duckdb.DuckDBPyConnection,
    cnpj8: str,
) -> pl.DataFrame:
    """Return PL trend over time for a single institution (by cnpj8).

    Returns: DataFrame [ano_mes, patrimonio_liquido]
    """
    sql = """
        SELECT ano_mes, patrimonio_liquido
        FROM balancetes_top50
        WHERE cnpj8 = ?
        ORDER BY ano_mes
    """
    return con.execute(sql, [cnpj8]).pl()


def list_balancetes_periods(con: duckdb.DuckDBPyConnection) -> list[int]:
    """Return all AAAAMM periods with balancetes data, descending."""
    result = con.execute(
        "SELECT DISTINCT ano_mes FROM balancetes_top50 ORDER BY ano_mes DESC"
    ).fetchall()
    return [row[0] for row in result]


def get_balancetes_multi_kpi(
    con: duckdb.DuckDBPyConnection,
    ano_mes: int | None = None,
) -> pl.DataFrame:
    """Return Top 50 institutions with multiple COSIF KPIs for a given period.

    Joins balancetes_top50 (ranking) with balancetes_raw (additional accounts).
    Returns: DataFrame [rank, cnpj8, nome_inst, cod_conglomerado,
             nome_conglomerado, patrimonio_liquido, ativo_total,
             operacoes_credito, depositos, resultado_liquido]
    """
    params: list[int] = []
    if ano_mes is not None:
        top50_filter = "ano_mes = ?"
        raw_filter = "ano_mes = ?"
        params = [ano_mes, ano_mes]
    else:
        top50_filter = "ano_mes = (SELECT MAX(ano_mes) FROM balancetes_top50)"
        raw_filter = "ano_mes = (SELECT MAX(ano_mes) FROM balancetes_top50)"

    sql = f"""
        WITH top50 AS (
            SELECT cnpj8, nome_inst, rank, patrimonio_liquido,
                   cod_conglomerado, nome_conglomerado
            FROM balancetes_top50
            WHERE {top50_filter}
        ),
        kpis AS (
            SELECT cnpj8, conta, SUM(saldo) AS saldo
            FROM balancetes_raw
            WHERE {raw_filter}
              AND conta IN (?, ?, ?, ?)
            GROUP BY cnpj8, conta
        )
        SELECT
            t.rank, t.cnpj8, t.nome_inst,
            t.cod_conglomerado, t.nome_conglomerado,
            t.patrimonio_liquido,
            MAX(CASE WHEN k.conta = ? THEN k.saldo END) AS ativo_total,
            MAX(CASE WHEN k.conta = ? THEN k.saldo END) AS operacoes_credito,
            MAX(CASE WHEN k.conta = ? THEN k.saldo END) AS depositos,
            MAX(CASE WHEN k.conta = ? THEN k.saldo END) AS resultado_liquido
        FROM top50 t
        LEFT JOIN kpis k ON t.cnpj8 = k.cnpj8
        GROUP BY t.rank, t.cnpj8, t.nome_inst,
                 t.cod_conglomerado, t.nome_conglomerado,
                 t.patrimonio_liquido
        ORDER BY t.rank
    """
    all_params: list[int | str] = [
        *params,
        COSIF_ATIVO_TOTAL, COSIF_OPERACOES_CREDITO,
        COSIF_DEPOSITOS, COSIF_RESULTADO_LIQUIDO,
        COSIF_ATIVO_TOTAL, COSIF_OPERACOES_CREDITO,
        COSIF_DEPOSITOS, COSIF_RESULTADO_LIQUIDO,
    ]
    return con.execute(sql, all_params).pl()


def get_balancetes_kpi_trend(
    con: duckdb.DuckDBPyConnection,
    cnpj8: str,
    conta: str,
) -> pl.DataFrame:
    """Return KPI trend over time for a single institution by COSIF account.

    Returns: DataFrame [ano_mes, valor]
    """
    sql = """
        SELECT ano_mes, SUM(saldo) AS valor
        FROM balancetes_raw
        WHERE cnpj8 = ? AND conta = ?
        GROUP BY ano_mes
        ORDER BY ano_mes
    """
    return con.execute(sql, [cnpj8, conta]).pl()


def get_balancetes_ratio_trend(
    con: duckdb.DuckDBPyConnection,
    cnpj8: str,
) -> pl.DataFrame:
    """Return derived ratios (ROE, ROA, Alavancagem) over time for one institution.

    Computes from PL, Ativo Total and Resultado Líquido in balancetes_raw.
    Returns: DataFrame [ano_mes, patrimonio_liquido, ativo_total,
             resultado_liquido, roe, roa, alavancagem]
    """
    sql = """
        SELECT
            ano_mes,
            SUM(CASE WHEN conta = ? THEN saldo END) AS patrimonio_liquido,
            SUM(CASE WHEN conta = ? THEN saldo END) AS ativo_total,
            SUM(CASE WHEN conta = ? THEN saldo END) AS resultado_liquido
        FROM balancetes_raw
        WHERE cnpj8 = ?
          AND conta IN (?, ?, ?)
        GROUP BY ano_mes
        ORDER BY ano_mes
    """
    params: list[str] = [
        COSIF_PATRIMONIO_LIQUIDO, COSIF_ATIVO_TOTAL, COSIF_RESULTADO_LIQUIDO,
        cnpj8,
        COSIF_PATRIMONIO_LIQUIDO, COSIF_ATIVO_TOTAL, COSIF_RESULTADO_LIQUIDO,
    ]
    df = con.execute(sql, params).pl()
    if df.is_empty():
        return df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("roe"),
            pl.lit(None, dtype=pl.Float64).alias("roa"),
            pl.lit(None, dtype=pl.Float64).alias("alavancagem"),
        )
    return df.with_columns(
        (pl.col("resultado_liquido") / pl.col("patrimonio_liquido")).alias("roe"),
        (pl.col("resultado_liquido") / pl.col("ativo_total")).alias("roa"),
        (pl.col("ativo_total") / pl.col("patrimonio_liquido")).alias("alavancagem"),
    )
