from __future__ import annotations

from datetime import date, timedelta

import duckdb
import polars as pl
import structlog

from src.models import BalanceteRow, InstitutionRecord, ReportValue

logger = structlog.get_logger()


def generate_quarter_periods(n_quarters: int) -> list[int]:
    """Generate the last N quarterly AAAAMM values, working backwards from today.

    Accounts for ~75-day publication lag (BCB publishes quarterly data with a delay).
    Returns list like [202409, 202406, 202403, 202312, ...] (most recent first).
    """
    # Available quarter end months
    quarter_months = [3, 6, 9, 12]

    # Start from today minus publication lag
    reference = date.today() - timedelta(days=75)

    # Find the most recent completed quarter before reference date
    year = reference.year
    month = reference.month

    # Find the largest quarter month <= current month
    available = [m for m in quarter_months if m <= month]
    if available:
        current_q_month = max(available)
    else:
        # Roll back to previous year's Q4
        year -= 1
        current_q_month = 12

    periods: list[int] = []
    for _ in range(n_quarters):
        periods.append(year * 100 + current_q_month)
        # Move to previous quarter
        idx = quarter_months.index(current_q_month)
        if idx == 0:
            year -= 1
            current_q_month = 12
        else:
            current_q_month = quarter_months[idx - 1]

    return periods


def is_period_fetched(
    con: duckdb.DuckDBPyConnection, ano_mes: int, relatorio: str
) -> bool:
    """Check fetch_log for whether this period+report is already ingested."""
    result = con.execute(
        "SELECT 1 FROM fetch_log WHERE ano_mes = ? AND relatorio = ?",
        [ano_mes, relatorio],
    ).fetchone()
    return result is not None


def ingest_cadastro(
    con: duckdb.DuckDBPyConnection,
    records: list[InstitutionRecord],
    ano_mes: int,
) -> int:
    """Insert cadastro records into DuckDB. Returns row count."""
    if not records:
        return 0
    rows = [
        {
            "ano_mes": ano_mes,
            "cod_conglomerado": r.cod_conglomerado,
            "nome_conglomerado": r.nome_conglomerado,
            "cod_inst": r.cod_inst,
            "nome_inst": r.nome_inst,
            "cnpj": r.cnpj,
            "segmento": r.segmento,
            "tipo_instituicao": r.tipo_instituicao,
            "cidade": r.cidade,
            "uf": r.uf,
        }
        for r in records
    ]
    df = pl.DataFrame(rows)  # noqa: F841 — referenced by DuckDB SQL
    # Delete and re-insert for idempotency
    con.execute("DELETE FROM cadastro WHERE ano_mes = ?", [ano_mes])
    con.execute("INSERT INTO cadastro SELECT * FROM df")

    # Track in fetch_log
    con.execute(
        "INSERT OR REPLACE INTO fetch_log (ano_mes, relatorio, row_count) "
        "VALUES (?, ?, ?)",
        [ano_mes, "cadastro", len(rows)],
    )
    logger.info("ingested_cadastro", ano_mes=ano_mes, rows=len(rows))
    return len(rows)


def ingest_report_values(
    con: duckdb.DuckDBPyConnection,
    records: list[ReportValue],
    ano_mes: int,
    relatorio: str,
) -> int:
    """Insert report values into DuckDB. Returns row count."""
    if not records:
        return 0
    rows = [
        {
            "ano_mes": ano_mes,
            "relatorio": relatorio,
            "cod_conglomerado": r.cod_conglomerado,
            "nome_conglomerado": r.nome_conglomerado,
            "codigo_coluna": r.codigo_coluna,
            "nome_coluna": r.nome_coluna,
            "valor_a": r.valor_a,
            "nome_linha": r.nome_linha,
            "ordenacao": r.ordenacao,
        }
        for r in records
    ]
    df = pl.DataFrame(rows)  # noqa: F841 — referenced by DuckDB SQL
    # Delete and re-insert for idempotency
    con.execute(
        "DELETE FROM report_values WHERE ano_mes = ? AND relatorio = ?",
        [ano_mes, relatorio],
    )
    con.execute("INSERT INTO report_values SELECT * FROM df")

    # Track in fetch_log
    con.execute(
        "INSERT OR REPLACE INTO fetch_log (ano_mes, relatorio, row_count) "
        "VALUES (?, ?, ?)",
        [ano_mes, relatorio, len(rows)],
    )
    logger.info(
        "ingested_report", ano_mes=ano_mes, relatorio=relatorio, rows=len(rows)
    )
    return len(rows)


def _compute_and_insert_top50(
    con: duckdb.DuckDBPyConnection, ano_mes: int
) -> int:
    """Compute Top 50 by PL from balancetes_raw and insert into balancetes_top50.

    Uses COSIF account 6.0.0.00.00-2 (Patrimônio Líquido).
    LEFT JOINs cadastro to resolve cod_conglomerado via CNPJ8 bridge.
    Returns row count inserted.
    """
    con.execute("DELETE FROM balancetes_top50 WHERE ano_mes = ?", [ano_mes])

    sql = """
        INSERT INTO balancetes_top50
        WITH pl_by_inst AS (
            SELECT cnpj8, nome_inst, SUM(saldo) AS patrimonio_liquido
            FROM balancetes_raw
            WHERE ano_mes = ?
              AND conta = '6.0.0.00.00-2'
            GROUP BY cnpj8, nome_inst
        ),
        ranked AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY patrimonio_liquido DESC) AS rank,
                p.cnpj8, p.nome_inst, p.patrimonio_liquido,
                c.cod_conglomerado, c.nome_conglomerado
            FROM pl_by_inst p
            LEFT JOIN (
                SELECT DISTINCT cnpj, cod_conglomerado, nome_conglomerado
                FROM cadastro
                WHERE ano_mes = (SELECT MAX(ano_mes) FROM cadastro)
            ) c ON SUBSTRING(c.cnpj, 1, 8) = p.cnpj8
        )
        SELECT ?, rank, cnpj8, nome_inst, cod_conglomerado,
               nome_conglomerado, patrimonio_liquido
        FROM ranked
        WHERE rank <= 50
        ORDER BY rank
    """
    con.execute(sql, [ano_mes, ano_mes])
    result = con.execute(
        "SELECT COUNT(*) FROM balancetes_top50 WHERE ano_mes = ?", [ano_mes]
    ).fetchone()
    count = result[0] if result else 0
    logger.info("computed_top50", ano_mes=ano_mes, rows=count)
    return count


def compute_top50_from_ifdata(
    con: duckdb.DuckDBPyConnection, ano_mes: int
) -> int:
    """Compute Top 50 by PL from report_values and insert into balancetes_top50.

    Uses Resumo report (relatorio='1') Patrimônio Líquido line.
    Returns row count inserted.
    """
    con.execute("DELETE FROM balancetes_top50 WHERE ano_mes = ?", [ano_mes])

    sql = """
        INSERT INTO balancetes_top50
        WITH pl_data AS (
            SELECT
                rv.cod_conglomerado,
                rv.nome_conglomerado,
                rv.valor_a AS patrimonio_liquido
            FROM report_values rv
            WHERE rv.ano_mes = ?
              AND rv.relatorio = '1'
              AND rv.nome_linha LIKE '%atrim%nio L%quido'
        ),
        ranked AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY patrimonio_liquido DESC) AS rank,
                CAST(p.cod_conglomerado AS VARCHAR) AS cnpj8,
                p.nome_conglomerado AS nome_inst,
                p.cod_conglomerado,
                p.nome_conglomerado,
                p.patrimonio_liquido
            FROM pl_data p
            WHERE p.patrimonio_liquido IS NOT NULL
        )
        SELECT ?, rank, cnpj8, nome_inst, cod_conglomerado,
               nome_conglomerado, patrimonio_liquido
        FROM ranked
        WHERE rank <= 50
        ORDER BY rank
    """
    con.execute(sql, [ano_mes, ano_mes])
    result = con.execute(
        "SELECT COUNT(*) FROM balancetes_top50 WHERE ano_mes = ?", [ano_mes]
    ).fetchone()
    count = result[0] if result else 0
    logger.info("computed_top50_ifdata", ano_mes=ano_mes, rows=count)
    return count


def ingest_balancetes(
    con: duckdb.DuckDBPyConnection,
    records: list[BalanceteRow],
    ano_mes: int,
) -> int:
    """Insert balancete rows into DuckDB and compute Top 50. Returns raw row count."""
    if not records:
        return 0
    rows = [
        {
            "ano_mes": r.ano_mes,
            "cnpj": r.cnpj,
            "cnpj8": r.cnpj8,
            "nome_inst": r.nome_inst,
            "atributo": r.atributo,
            "documento": r.documento,
            "conta": r.conta,
            "nome_conta": r.nome_conta,
            "saldo": r.saldo,
        }
        for r in records
    ]
    df = pl.DataFrame(rows)  # noqa: F841 — referenced by DuckDB SQL

    con.execute("DELETE FROM balancetes_raw WHERE ano_mes = ?", [ano_mes])
    con.execute("INSERT INTO balancetes_raw SELECT * FROM df")

    _compute_and_insert_top50(con, ano_mes)

    con.execute(
        "INSERT OR REPLACE INTO fetch_log (ano_mes, relatorio, row_count) "
        "VALUES (?, ?, ?)",
        [ano_mes, "balancetes", len(rows)],
    )
    logger.info("ingested_balancetes", ano_mes=ano_mes, rows=len(rows))
    return len(rows)
