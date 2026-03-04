from __future__ import annotations

import duckdb
import polars as pl

# COSIF account codes for balancetes
COSIF_PATRIMONIO_LIQUIDO = "6.0.0.00.00-2"
COSIF_ATIVO_TOTAL = "1.0.0.00.00-7"
COSIF_RESULTADO_LIQUIDO = "7.0.0.00.00-9"
COSIF_DESPESAS = "8.0.0.00.00-6"
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

# DRE account ordering for summarized COSIF 4040 DRE display
DRE_ACCOUNT_ORDER: dict[str, int] = {
    "7.1": 10,
    "8.1": 20,
    "RESULTADO BRUTO": 30,
    "7.3": 40,
    "7.5": 50,
    "7.7": 60,
    "8.3": 70,
    "8.5": 80,
    "8.7": 90,
    "8.9": 100,
    "RESULTADO OPERACIONAL": 110,
    "7.9": 120,
    "RESULTADO ANTES TRIBUTAÇÃO": 130,
}


def semester_annualization_factor(ano_mes: int) -> float:
    """Return the factor to annualize semester-accumulated COSIF/IF.data values.

    Brazilian banking DRE data accumulates per fiscal semester (Jan-Jun, Jul-Dec),
    resetting in July. Factor = 12 / months_elapsed_in_semester.
    """
    mes = ano_mes % 100
    mes_no_semestre = mes - 6 if mes > 6 else mes
    return 12.0 / mes_no_semestre


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
        "SELECT DISTINCT nome_linha FROM report_values WHERE relatorio = ? ORDER BY nome_linha",
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
        SELECT ano_mes,
            CASE nome_linha
                WHEN 'Resultado antes da Tributação, Lucro e Participação'
                    THEN 'Resultado antes da Tributação e Participações'
                WHEN 'Participação nos Lucros'
                    THEN 'Participações no Lucro'
                ELSE nome_linha
            END AS nome_linha,
            valor_a
        FROM report_values
        WHERE cod_conglomerado = ?
          AND relatorio = '4'
          AND nome_linha != 'Ativo Total'
        ORDER BY ano_mes, ordenacao
    """
    return con.execute(sql, [cod_conglomerado]).pl()


def get_cosif_dre(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
) -> pl.DataFrame:
    """Build detailed DRE from COSIF 4010 data, aggregated across institutions.

    Joins balancetes_raw (documento='4010') with institution_mapping to
    aggregate saldo across all individual institutions in the conglomerado.
    Filters to COSIF groups 7 (receitas) and 8 (despesas).

    Returns: DataFrame [ano_mes, conta, nome_conta, saldo]
    """
    sql = """
        WITH agg AS (
            SELECT
                b.ano_mes,
                LEFT(b.conta, LENGTH(b.conta) - 2) AS conta,
                SUM(b.saldo) AS saldo
            FROM balancetes_raw b
            INNER JOIN institution_mapping m
                ON b.cnpj8 = m.cnpj8
            WHERE b.documento = '4010'
              AND m.cod_conglomerado = ?
              AND (b.conta LIKE '7.%' OR b.conta LIKE '8.%')
            GROUP BY b.ano_mes, LEFT(b.conta, LENGTH(b.conta) - 2)
        ),
        names AS (
            SELECT
                LEFT(b.conta, LENGTH(b.conta) - 2) AS conta,
                FIRST(b.nome_conta ORDER BY b.ano_mes DESC) AS nome_conta
            FROM balancetes_raw b
            INNER JOIN institution_mapping m
                ON b.cnpj8 = m.cnpj8
            WHERE b.documento = '4010'
              AND m.cod_conglomerado = ?
              AND (b.conta LIKE '7.%' OR b.conta LIKE '8.%')
            GROUP BY LEFT(b.conta, LENGTH(b.conta) - 2)
        )
        SELECT a.ano_mes, a.conta, n.nome_conta, a.saldo
        FROM agg a
        INNER JOIN names n ON a.conta = n.conta
        ORDER BY a.ano_mes, a.conta
    """
    return con.execute(sql, [cod_conglomerado, cod_conglomerado]).pl()


def get_cosif_dre_4040(
    con: duckdb.DuckDBPyConnection,
    cod_conglomerado: int,
) -> pl.DataFrame:
    """Build summarized DRE from COSIF 4010 data (level 2 accounts).

    Joins balancetes_raw (documento='4010') with institution_mapping to
    aggregate saldo across all individual institutions in the conglomerado.
    Filters to COSIF level-2 accounts in groups 7 (receitas) and 8 (despesas).

    Returns: DataFrame [ano_mes, conta, nome_conta, saldo]
    """
    sql = """
        WITH agg AS (
            SELECT
                b.ano_mes,
                LEFT(b.conta, LENGTH(b.conta) - 2) AS conta,
                SUM(b.saldo) AS saldo
            FROM balancetes_raw b
            INNER JOIN institution_mapping m
                ON b.cnpj8 = m.cnpj8
            WHERE b.documento = '4010'
              AND m.cod_conglomerado = ?
              AND b.conta LIKE '_._.0.00.00-%'
              AND b.conta NOT LIKE '_.0.0.00.00-%'
              AND (b.conta LIKE '7.%' OR b.conta LIKE '8.%')
            GROUP BY b.ano_mes, LEFT(b.conta, LENGTH(b.conta) - 2)
        ),
        names AS (
            SELECT
                LEFT(b.conta, LENGTH(b.conta) - 2) AS conta,
                FIRST(b.nome_conta ORDER BY b.ano_mes DESC) AS nome_conta
            FROM balancetes_raw b
            INNER JOIN institution_mapping m
                ON b.cnpj8 = m.cnpj8
            WHERE b.documento = '4010'
              AND m.cod_conglomerado = ?
              AND b.conta LIKE '_._.0.00.00-%'
              AND b.conta NOT LIKE '_.0.0.00.00-%'
              AND (b.conta LIKE '7.%' OR b.conta LIKE '8.%')
            GROUP BY LEFT(b.conta, LENGTH(b.conta) - 2)
        )
        SELECT a.ano_mes, a.conta, n.nome_conta, a.saldo
        FROM agg a
        INNER JOIN names n ON a.conta = n.conta
        ORDER BY a.ano_mes, a.conta
    """
    return con.execute(sql, [cod_conglomerado, cod_conglomerado]).pl()


def desacumulate_dre_semesters(dre_df: pl.DataFrame) -> pl.DataFrame:
    """Convert semester-accumulated DRE balances to monthly values.

    COSIF DRE accounts (groups 7/8) accumulate within each fiscal semester
    (Jan-Jun, Jul-Dec) and reset at the start of the next semester.
    This function subtracts the previous month's accumulated balance
    to recover each month's individual contribution.
    """
    if dre_df.is_empty():
        return dre_df

    year = pl.col("ano_mes") // 100
    month = pl.col("ano_mes") % 100
    semester = year * 2 + (month >= 7).cast(pl.Int64)

    prev = pl.col("saldo").shift(1).over(["conta", semester])
    monthly = pl.col("saldo") - prev.fill_null(0)

    return dre_df.sort("conta", "ano_mes").with_columns(
        monthly.alias("saldo")
    )


def compute_dre_subtotals(dre_df: pl.DataFrame) -> pl.DataFrame:
    """Add subtotal rows and ordering to a COSIF DRE DataFrame.

    Computes per period:
      - Resultado Bruto (7.1 + 8.1)
      - Resultado Operacional (all except 7.9)
      - Resultado antes da Tributação (all)

    Returns: DataFrame [ano_mes, conta, nome_conta, saldo, ordering]
    """
    if dre_df.is_empty():
        return dre_df.with_columns(pl.lit(0).alias("ordering"))

    periods = dre_df["ano_mes"].unique().to_list()
    subtotal_rows: list[dict[str, object]] = []

    for period in periods:
        period_df = dre_df.filter(pl.col("ano_mes") == period)

        bruto = period_df.filter(
            pl.col("conta").str.starts_with("7.1")
            | pl.col("conta").str.starts_with("8.1")
        )["saldo"].sum()
        subtotal_rows.append({
            "ano_mes": period,
            "conta": "RESULTADO BRUTO",
            "nome_conta": "= Resultado Bruto de Intermediação Financeira",
            "saldo": bruto,
        })

        operacional = period_df.filter(
            ~pl.col("conta").str.starts_with("7.9")
        )["saldo"].sum()
        subtotal_rows.append({
            "ano_mes": period,
            "conta": "RESULTADO OPERACIONAL",
            "nome_conta": "= Resultado Operacional",
            "saldo": operacional,
        })

        total = period_df["saldo"].sum()
        subtotal_rows.append({
            "ano_mes": period,
            "conta": "RESULTADO ANTES TRIBUTAÇÃO",
            "nome_conta": "= Resultado antes da Tributação",
            "saldo": total,
        })

    subtotals = pl.DataFrame(subtotal_rows, schema=dre_df.schema)
    combined = pl.concat([dre_df, subtotals])

    combined = combined.with_columns(
        pl.col("conta").map_elements(
            lambda c: DRE_ACCOUNT_ORDER.get(
                c, DRE_ACCOUNT_ORDER.get(c[:3], 999)
            ),
            return_dtype=pl.Int64,
        ).alias("ordering")
    )
    return combined.sort(["ano_mes", "ordering"])


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


_RV_ATIVO = "Ativo Total"
_RV_CREDITO = "Carteira de Crédito"
_RV_DEPOSITOS = "Captações"
_RV_RESULTADO = "Lucro Líquido"


def get_balancetes_multi_kpi(
    con: duckdb.DuckDBPyConnection,
    ano_mes: int | None = None,
) -> pl.DataFrame:
    """Return Top 50 institutions with KPIs from IF.data Report 1 for a given period.

    Joins balancetes_top50 (ranking/PL) with report_values (Ativo Total,
    Carteira de Crédito, Captações, Lucro Líquido) on cod_conglomerado.
    Returns: DataFrame [rank, cnpj8, nome_inst, cod_conglomerado,
             nome_conglomerado, patrimonio_liquido, ativo_total,
             operacoes_credito, depositos, resultado_liquido]
    """
    if ano_mes is not None:
        top50_filter = "ano_mes = ?"
        kpi_filter = "rv.ano_mes = ?"
        params: list[int | str] = [ano_mes, ano_mes]
    else:
        top50_filter = "ano_mes = (SELECT MAX(ano_mes) FROM balancetes_top50)"
        kpi_filter = "rv.ano_mes = (SELECT MAX(ano_mes) FROM balancetes_top50)"
        params = []

    rv_lines = [_RV_ATIVO, _RV_CREDITO, _RV_DEPOSITOS, _RV_RESULTADO]
    sql = f"""
        WITH top50 AS (
            SELECT cnpj8, nome_inst, rank, patrimonio_liquido,
                   cod_conglomerado, nome_conglomerado
            FROM balancetes_top50
            WHERE {top50_filter}
        ),
        kpis AS (
            SELECT rv.cod_conglomerado, rv.nome_linha, rv.valor_a
            FROM report_values rv
            WHERE {kpi_filter}
              AND rv.relatorio = '1'
              AND rv.nome_linha IN (?, ?, ?, ?)
        )
        SELECT
            t.rank, t.cnpj8, t.nome_inst,
            t.cod_conglomerado, t.nome_conglomerado,
            t.patrimonio_liquido,
            MAX(CASE WHEN k.nome_linha = ? THEN k.valor_a END) AS ativo_total,
            MAX(CASE WHEN k.nome_linha = ? THEN k.valor_a END) AS operacoes_credito,
            MAX(CASE WHEN k.nome_linha = ? THEN k.valor_a END) AS depositos,
            MAX(CASE WHEN k.nome_linha = ? THEN k.valor_a END) AS resultado_liquido
        FROM top50 t
        LEFT JOIN kpis k ON t.cod_conglomerado = k.cod_conglomerado
        GROUP BY t.rank, t.cnpj8, t.nome_inst,
                 t.cod_conglomerado, t.nome_conglomerado,
                 t.patrimonio_liquido
        ORDER BY t.rank
    """
    all_params: list[int | str] = [*params, *rv_lines, *rv_lines]
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
            SUM(CASE WHEN conta IN (?, ?) THEN saldo END) AS resultado_liquido
        FROM balancetes_raw
        WHERE cnpj8 = ?
          AND conta IN (?, ?, ?, ?)
        GROUP BY ano_mes
        ORDER BY ano_mes
    """
    params: list[str] = [
        COSIF_PATRIMONIO_LIQUIDO,
        COSIF_ATIVO_TOTAL,
        COSIF_RESULTADO_LIQUIDO,
        COSIF_DESPESAS,
        cnpj8,
        COSIF_PATRIMONIO_LIQUIDO,
        COSIF_ATIVO_TOTAL,
        COSIF_RESULTADO_LIQUIDO,
        COSIF_DESPESAS,
    ]
    df = con.execute(sql, params).pl()
    if df.is_empty():
        return df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("roe"),
            pl.lit(None, dtype=pl.Float64).alias("roa"),
            pl.lit(None, dtype=pl.Float64).alias("alavancagem"),
        )
    mes = pl.col("ano_mes") % 100
    mes_no_sem = pl.when(mes > 6).then(mes - 6).otherwise(mes)
    fator = pl.lit(12.0) / mes_no_sem
    resultado_anual = pl.col("resultado_liquido") * fator
    return df.with_columns(
        (resultado_anual / pl.col("patrimonio_liquido")).alias("roe"),
        (resultado_anual / pl.col("ativo_total")).alias("roa"),
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
                12.0 / (CASE WHEN ano_mes % 100 > 6
                             THEN ano_mes % 100 - 6
                             ELSE ano_mes % 100 END) AS fator_anual,
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
            CASE WHEN p1.pl != 0
                THEN p1.lucro_liquido * p1.fator_anual / p1.pl * 100 END
                AS roe,
            CASE WHEN p1.ativo_total != 0
                THEN p1.lucro_liquido * p1.fator_anual / p1.ativo_total * 100 END
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
    return con.execute(sql, [cod_conglomerado, cod_conglomerado, cod_conglomerado]).pl()


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
        "roe",
        "roa",
        "loan_to_deposit",
        "credit_intensity",
        "securities_share",
        "leverage",
        "debt_equity",
        "funding_dependency",
        "pr_coverage",
        "basileia",
        "capital_principal",
        "capital_nivel1",
        "capital_excess",
        "razao_alavancagem",
        "tax_rate",
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
    # SQL equivalent of semester_annualization_factor() for use inside GROUP BY
    _sql_annual = (
        "(12.0 / (CASE WHEN MAX(rv.ano_mes) % 100 > 6 "
        "THEN MAX(rv.ano_mes) % 100 - 6 "
        "ELSE MAX(rv.ano_mes) % 100 END))"
    )
    _lucro = "MAX(CASE WHEN rv.nome_linha LIKE '%ucro L%' THEN rv.valor_a END)"
    ratio_expressions = {
        "roe": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) != 0 "
            f"THEN {_lucro} * {_sql_annual} "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) * 100 END",
            "'1'",
        ),
        "roa": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            f"THEN {_lucro} * {_sql_annual} "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'",
        ),
        "loan_to_deposit": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%apta%' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%arteira de Cr%' "
            "AND rv.nome_linha NOT LIKE '%lass%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%apta%' "
            "THEN rv.valor_a END) * 100 END",
            "'1'",
        ),
        "credit_intensity": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%arteira de Cr%' "
            "AND rv.nome_linha NOT LIKE '%lass%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'",
        ),
        "securities_share": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%tulos e Valores%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'",
        ),
        "leverage": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) END",
            "'1'",
        ),
        "debt_equity": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%assivo Exig%' "
            "AND rv.nome_linha NOT LIKE '%irculante%' THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%atrim%nio L%' "
            "AND rv.nome_linha NOT LIKE '%efer%' THEN rv.valor_a END) END",
            "'1'",
        ),
        "funding_dependency": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%apta%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'",
        ),
        "pr_coverage": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%efer%ncia para Compara%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha = 'Ativo Total' "
            "THEN rv.valor_a END) * 100 END",
            "'1'",
        ),
        "basileia": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%asileia%' THEN rv.valor_a END) * 100",
            "'5'",
        ),
        "capital_principal": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%ndice de Capital Principal' "
            "THEN rv.valor_a END) * 100",
            "'5'",
        ),
        "capital_nivel1": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%apital N%vel I' THEN rv.valor_a END) * 100",
            "'5'",
        ),
        "capital_excess": (
            "(MAX(CASE WHEN rv.nome_linha LIKE '%asileia%' THEN rv.valor_a END) - 0.105) * 100",
            "'5'",
        ),
        "razao_alavancagem": (
            "MAX(CASE WHEN rv.nome_linha LIKE '%az%o de Alavancagem%' THEN rv.valor_a END) * 100",
            "'5'",
        ),
        "tax_rate": (
            "CASE WHEN MAX(CASE WHEN rv.nome_linha LIKE '%esultado antes%' "
            "THEN rv.valor_a END) != 0 "
            "THEN MAX(CASE WHEN rv.nome_linha LIKE '%mposto de Renda%' "
            "THEN rv.valor_a END) "
            "/ MAX(CASE WHEN rv.nome_linha LIKE '%esultado antes%' "
            "THEN rv.valor_a END) * 100 END",
            "'4'",
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
        period_clause = "rv.ano_mes = (SELECT MAX(ano_mes) FROM report_values)"

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
