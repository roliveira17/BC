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
    cod_conglomerados: list[int] | None = None,
) -> pl.DataFrame:
    """Return distinct institutions, optionally filtered by segment and/or list.

    Uses the most recent cadastro period available.
    Returns: DataFrame [cod_conglomerado, nome_conglomerado, segmento]
    """
    params: list[str | int] = []
    seg_filter = ""
    if segmento and segmento != "ALL":
        seg_filter = "AND segmento = ?"
        params.append(segmento)

    cong_filter = ""
    if cod_conglomerados:
        placeholders = ", ".join(["?"] * len(cod_conglomerados))
        cong_filter = f"AND cod_conglomerado IN ({placeholders})"
        params.extend(cod_conglomerados)

    sql = f"""
        SELECT DISTINCT cod_conglomerado, nome_conglomerado, segmento
        FROM cadastro
        WHERE ano_mes = (SELECT MAX(ano_mes) FROM cadastro)
        {seg_filter}
        {cong_filter}
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


def get_institution_details(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
) -> pl.DataFrame:
    """Return metadata for one institution from the latest cadastro period.

    Returns: DataFrame [cod_conglomerado, nome_conglomerado, cnpj, segmento, cidade, uf]
    """
    sql = """
        SELECT DISTINCT cod_conglomerado, nome_conglomerado, cnpj, segmento, cidade, uf
        FROM cadastro
        WHERE cod_conglomerado = ?
          AND ano_mes = (SELECT MAX(ano_mes) FROM cadastro)
        LIMIT 1
    """
    return con.execute(sql, [cod_conglomerado]).pl()


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


def get_top50_enriched(
    con: duckdb.DuckDBPyConnection,
    ano_mes: int | None = None,
) -> pl.DataFrame:
    """Top 50 by PL enriched with IF.data indicators from the closest quarter.

    LEFT JOINs Basileia (report 5), Ativo Total (report 1), and Lucro Líquido
    (report 4) via the cod_conglomerado bridge.

    Returns: DataFrame [ano_mes, rank, cnpj8, nome_inst, cod_conglomerado,
                        nome_conglomerado, patrimonio_liquido, basileia,
                        ativo_total, lucro_liquido, ifdata_periodo]
    """
    params: list[int] = []
    if ano_mes is not None:
        period_clause = "b.ano_mes = ?"
        params.append(ano_mes)
    else:
        period_clause = "b.ano_mes = (SELECT MAX(ano_mes) FROM balancetes_top50)"

    sql = f"""
        WITH top50 AS (
            SELECT *
            FROM balancetes_top50 b
            WHERE {period_clause}
        ),
        closest_q AS (
            SELECT MAX(ano_mes) AS q_period
            FROM report_values
            WHERE ano_mes <= (SELECT MAX(ano_mes) FROM top50)
        ),
        basileia AS (
            SELECT cod_conglomerado, MAX(valor_a) AS basileia
            FROM report_values
            WHERE relatorio = '5'
              AND nome_linha LIKE '%asileia%'
              AND ano_mes = (SELECT q_period FROM closest_q)
            GROUP BY cod_conglomerado
        ),
        ativo AS (
            SELECT cod_conglomerado, MAX(valor_a) AS ativo_total
            FROM report_values
            WHERE relatorio = '1'
              AND nome_linha LIKE '%tivo Total%'
              AND ano_mes = (SELECT q_period FROM closest_q)
            GROUP BY cod_conglomerado
        ),
        lucro AS (
            SELECT cod_conglomerado, MAX(valor_a) AS lucro_liquido
            FROM report_values
            WHERE relatorio = '4'
              AND nome_linha LIKE '%ucro%'
              AND ano_mes = (SELECT q_period FROM closest_q)
            GROUP BY cod_conglomerado
        )
        SELECT t.ano_mes, t.rank, t.cnpj8, t.nome_inst,
               t.cod_conglomerado, t.nome_conglomerado,
               t.patrimonio_liquido,
               bas.basileia,
               atv.ativo_total,
               luc.lucro_liquido,
               cq.q_period AS ifdata_periodo
        FROM top50 t
        LEFT JOIN basileia bas ON bas.cod_conglomerado = t.cod_conglomerado
        LEFT JOIN ativo atv ON atv.cod_conglomerado = t.cod_conglomerado
        LEFT JOIN lucro luc ON luc.cod_conglomerado = t.cod_conglomerado
        CROSS JOIN closest_q cq
        ORDER BY t.rank
    """
    return con.execute(sql, params).pl()


def compare_pl_trend(
    con: duckdb.DuckDBPyConnection,
    cnpj8_list: list[str],
) -> pl.DataFrame:
    """PL trend over time for multiple institutions.

    Returns: DataFrame [ano_mes, cnpj8, nome_inst, patrimonio_liquido]
    """
    if not cnpj8_list:
        return pl.DataFrame(
            schema={
                "ano_mes": pl.Int64,
                "cnpj8": pl.Utf8,
                "nome_inst": pl.Utf8,
                "patrimonio_liquido": pl.Float64,
            }
        )
    placeholders = ", ".join(["?"] * len(cnpj8_list))
    sql = f"""
        SELECT ano_mes, cnpj8, nome_inst, patrimonio_liquido
        FROM balancetes_top50
        WHERE cnpj8 IN ({placeholders})
        ORDER BY ano_mes, cnpj8
    """
    return con.execute(sql, cnpj8_list).pl()


def get_financial_ratios(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
) -> pl.DataFrame:
    """Compute financial ratios from Reports 1, 4, and 5 for one institution.

    Returns: DataFrame [ano_mes, roe, roa, loan_to_deposit, credit_intensity,
             securities_share, leverage, debt_equity, funding_dependency,
             pr_coverage, basileia, capital_principal, capital_nivel1,
             capital_excess, tax_rate]
    """
    sql = """
        WITH rep1 AS (
            SELECT ano_mes, nome_linha, valor_a
            FROM report_values
            WHERE cod_conglomerado = ? AND relatorio = '1'
        ),
        p1 AS (
            SELECT
                ano_mes,
                MAX(CASE WHEN nome_linha LIKE '%ucro L%' THEN valor_a END)
                    AS lucro_liquido,
                MAX(CASE WHEN nome_linha LIKE '%atrim%nio L%'
                    AND nome_linha NOT LIKE '%efer%' THEN valor_a END)
                    AS pl,
                MAX(CASE WHEN nome_linha = 'Ativo Total' THEN valor_a END)
                    AS ativo_total,
                MAX(CASE WHEN nome_linha LIKE '%arteira de Cr%'
                    AND nome_linha NOT LIKE '%lass%' THEN valor_a END)
                    AS carteira_credito,
                MAX(CASE WHEN nome_linha LIKE '%apta%' THEN valor_a END)
                    AS captacoes,
                MAX(CASE WHEN nome_linha LIKE '%tulos e Valores%' THEN valor_a END)
                    AS tvm,
                MAX(CASE WHEN nome_linha LIKE '%assivo Exig%'
                    AND nome_linha NOT LIKE '%irculante%' THEN valor_a END)
                    AS passivo_exigivel,
                MAX(CASE WHEN nome_linha LIKE '%efer%ncia para Compara%'
                    THEN valor_a END)
                    AS pr
            FROM rep1
            GROUP BY ano_mes
        ),
        rep5 AS (
            SELECT ano_mes, nome_linha, valor_a
            FROM report_values
            WHERE cod_conglomerado = ? AND relatorio = '5'
        ),
        p5 AS (
            SELECT
                ano_mes,
                MAX(CASE WHEN nome_linha LIKE '%asileia%' THEN valor_a END)
                    AS basileia,
                MAX(CASE WHEN nome_linha LIKE '%ndice de Capital Principal'
                    THEN valor_a END)
                    AS capital_principal,
                MAX(CASE WHEN nome_linha LIKE '%apital N%vel I'
                    THEN valor_a END)
                    AS capital_nivel1,
                MAX(CASE WHEN nome_linha LIKE '%az%o de Alavancagem%'
                    THEN valor_a END)
                    AS razao_alavancagem
            FROM rep5
            GROUP BY ano_mes
        ),
        rep4 AS (
            SELECT ano_mes, nome_linha, valor_a
            FROM report_values
            WHERE cod_conglomerado = ? AND relatorio = '4'
        ),
        p4 AS (
            SELECT
                ano_mes,
                MAX(CASE WHEN nome_linha LIKE '%esultado antes%'
                    THEN valor_a END)
                    AS resultado_antes_trib,
                MAX(CASE WHEN nome_linha LIKE '%mposto de Renda%'
                    THEN valor_a END)
                    AS ir_csll
            FROM rep4
            GROUP BY ano_mes
        )
        SELECT
            COALESCE(p1.ano_mes, p5.ano_mes, p4.ano_mes) AS ano_mes,
            CASE WHEN p1.pl != 0 THEN p1.lucro_liquido / p1.pl * 100 END
                AS roe,
            CASE WHEN p1.ativo_total != 0
                THEN p1.lucro_liquido / p1.ativo_total * 100 END
                AS roa,
            CASE WHEN p1.captacoes != 0
                THEN p1.carteira_credito / p1.captacoes * 100 END
                AS loan_to_deposit,
            CASE WHEN p1.ativo_total != 0
                THEN p1.carteira_credito / p1.ativo_total * 100 END
                AS credit_intensity,
            CASE WHEN p1.ativo_total != 0
                THEN p1.tvm / p1.ativo_total * 100 END
                AS securities_share,
            CASE WHEN p1.pl != 0 THEN p1.ativo_total / p1.pl END
                AS leverage,
            CASE WHEN p1.pl != 0 THEN p1.passivo_exigivel / p1.pl END
                AS debt_equity,
            CASE WHEN p1.ativo_total != 0
                THEN p1.captacoes / p1.ativo_total * 100 END
                AS funding_dependency,
            CASE WHEN p1.ativo_total != 0
                THEN p1.pr / p1.ativo_total * 100 END
                AS pr_coverage,
            p5.basileia * 100 AS basileia,
            p5.capital_principal * 100 AS capital_principal,
            p5.capital_nivel1 * 100 AS capital_nivel1,
            (p5.basileia - 0.105) * 100 AS capital_excess,
            p5.razao_alavancagem * 100 AS razao_alavancagem,
            CASE WHEN p4.resultado_antes_trib != 0
                THEN p4.ir_csll / p4.resultado_antes_trib * 100 END
                AS tax_rate
        FROM p1
        FULL OUTER JOIN p5 ON p1.ano_mes = p5.ano_mes
        FULL OUTER JOIN p4 ON COALESCE(p1.ano_mes, p5.ano_mes) = p4.ano_mes
        ORDER BY ano_mes
    """
    return con.execute(
        sql, [cod_conglomerado, cod_conglomerado, cod_conglomerado]
    ).pl()


def get_ratio_ranking(
    con: duckdb.DuckDBPyConnection,
    ratio_name: str,
    ano_mes: int | None = None,
) -> pl.DataFrame:
    """Rank all institutions by a computed ratio for a given period.

    ratio_name must be one of the SQL column aliases from get_financial_ratios.
    Returns: DataFrame [cod_conglomerado, nome_conglomerado, segmento, valor]
    """
    valid_ratios = {
        "roe", "roa", "loan_to_deposit", "credit_intensity",
        "securities_share", "leverage", "debt_equity",
        "funding_dependency", "pr_coverage", "basileia",
        "capital_principal", "capital_nivel1", "capital_excess",
        "razao_alavancagem", "tax_rate",
    }
    if ratio_name not in valid_ratios:
        return pl.DataFrame(
            schema={
                "cod_conglomerado": pl.Int64,
                "nome_conglomerado": pl.Utf8,
                "segmento": pl.Utf8,
                "valor": pl.Float64,
            }
        )

    params: list[int] = []
    if ano_mes is not None:
        period_clause = "rv.ano_mes = ?"
        params.append(ano_mes)
    else:
        period_clause = "rv.ano_mes = (SELECT MAX(ano_mes) FROM report_values)"

    # Build ratio expression based on ratio_name
    ratio_expressions = {
        "roe": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%ucro L%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) * 100 END",
            "'1'"
        ),
        "roa": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%ucro L%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'"
        ),
        "loan_to_deposit": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%apta%' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%arteira de Cr%' "
            "AND rv.nome_linha NOT LIKE '%lass%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%apta%' "
            "THEN rv.valor_a END) * 100 END",
            "'1'"
        ),
        "credit_intensity": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%arteira de Cr%' "
            "AND rv.nome_linha NOT LIKE '%lass%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'"
        ),
        "securities_share": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%tulos e Valores%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'"
        ),
        "leverage": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) END",
            "'1'"
        ),
        "debt_equity": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%assivo Exig%' "
            "AND rv.nome_linha NOT LIKE '%irculante%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) END",
            "'1'"
        ),
        "funding_dependency": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%apta%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'"
        ),
        "pr_coverage": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%efer%ncia para Compara%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'"
        ),
        "basileia": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%asileia%' "
            "THEN rv.valor_a END) * 100",
            "'5'"
        ),
        "capital_principal": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%ndice de Capital Principal' "
            "THEN rv.valor_a END) * 100",
            "'5'"
        ),
        "capital_nivel1": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%apital N%vel I' "
            "THEN rv.valor_a END) * 100",
            "'5'"
        ),
        "capital_excess": (
            "(MAX(CASE WHEN rv.nome_linha LIKE '%asileia%' "
            "THEN rv.valor_a END) - 0.105) * 100",
            "'5'"
        ),
        "razao_alavancagem": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%az%o de Alavancagem%' "
            "THEN rv.valor_a END) * 100",
            "'5'"
        ),
        "tax_rate": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%esultado antes%' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%mposto de Renda%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%esultado antes%' "
            "THEN rv.valor_a END) * 100 END",
            "'4'"
        ),
    }

    expr, report = ratio_expressions[ratio_name]

    sql = f"""
        SELECT
            rv.cod_conglomerado,
            rv.nome_conglomerado,
            COALESCE(c.segmento, '') AS segmento,
            {expr} AS valor
        FROM report_values rv
        LEFT JOIN cadastro c
            ON c.cod_conglomerado = rv.cod_conglomerado
            AND c.ano_mes = rv.ano_mes
        WHERE rv.relatorio = {report}
          AND {period_clause}
        GROUP BY rv.cod_conglomerado, rv.nome_conglomerado, c.segmento
        HAVING valor IS NOT NULL
        ORDER BY valor DESC
    """
    return con.execute(sql, params).pl()


def get_market_share_pl(
    con: duckdb.DuckDBPyConnection,
    ano_mes: int | None = None,
    top_n: int = 20,
) -> pl.DataFrame:
    """Compute market share by Patrimônio Líquido for top N institutions.

    Returns: DataFrame [cod_conglomerado, nome_conglomerado, segmento,
             pl_value, market_share_pct]
    """
    params: list[int] = []
    if ano_mes is not None:
        period_clause = "rv.ano_mes = ?"
        params.append(ano_mes)
    else:
        period_clause = (
            "rv.ano_mes = (SELECT MAX(ano_mes) FROM report_values)"
        )

    sql = f"""
        WITH pl_data AS (
            SELECT
                rv.cod_conglomerado,
                rv.nome_conglomerado,
                COALESCE(c.segmento, '') AS segmento,
                rv.valor_a AS pl_value
            FROM report_values rv
            LEFT JOIN cadastro c
                ON c.cod_conglomerado = rv.cod_conglomerado
                AND c.ano_mes = rv.ano_mes
            WHERE rv.relatorio = '1'
              AND rv.nome_linha LIKE '%atrim%nio L%'
              AND rv.nome_linha NOT LIKE '%efer%'
              AND {period_clause}
              AND rv.valor_a IS NOT NULL
        ),
        total AS (
            SELECT SUM(pl_value) AS total_pl FROM pl_data
        )
        SELECT
            p.cod_conglomerado,
            p.nome_conglomerado,
            p.segmento,
            p.pl_value,
            p.pl_value / t.total_pl * 100 AS market_share_pct
        FROM pl_data p
        CROSS JOIN total t
        ORDER BY p.pl_value DESC
        LIMIT ?
    """
    params.append(top_n)
    return con.execute(sql, params).pl()
