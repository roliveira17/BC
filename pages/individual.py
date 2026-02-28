from __future__ import annotations

import re

import dash
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html

from src.db import get_connection
from src.queries import (
    get_capital_indicators,
    get_cosif_dre,
    get_dre_indicators,
    get_institution_details,
    get_summary_indicators,
)
from src.settings import Settings

dash.register_page(__name__, path="/", name="Análise Individual")

settings = Settings()
con = get_connection(settings.duckdb_path)

REGULATORY_SUFFIXES = [" - PRUDENCIAL", " - FINANCEIRO"]
ENTITY_SUFFIXES = [" S.A.", " S/A", " LTDA.", " LTDA"]

_LABEL_STYLE: dict[str, str] = {
    "fontWeight": "bold",
    "fontSize": "12px",
    "color": "#666",
}

# Indicators stored as fractions (0.36 = 36%) that need *100
_PCT_PATTERNS = re.compile(
    r"(?i)(índice|indice|razão|razao|irrbb|adicional de capital)",
)


def _is_pct_indicator(nome_linha: str) -> bool:
    """Check if an indicator is a percentage stored as fraction."""
    return bool(_PCT_PATTERNS.search(nome_linha))


def clean_commercial_name(nome: str) -> str:
    """Strip regulatory and entity suffixes for a friendlier name."""
    result = nome
    for suffix in REGULATORY_SUFFIXES:
        if result.upper().endswith(suffix):
            result = result[: len(result) - len(suffix)]
            break
    for suffix in ENTITY_SUFFIXES:
        if result.upper().endswith(suffix):
            result = result[: len(result) - len(suffix)]
            break
    return result.strip()


def _extract_latest_value(
    df: pl.DataFrame,
    pattern: str,
    exclude: str | None = None,
) -> float | None:
    """Get the latest period's value for a matching indicator."""
    if df.is_empty():
        return None
    latest = df["ano_mes"].max()
    mask = (df["ano_mes"] == latest) & df["nome_linha"].str.contains(pattern)
    if exclude:
        mask = mask & ~df["nome_linha"].str.contains(exclude)
    filtered = df.filter(mask)
    if filtered.is_empty():
        return None
    return filtered["valor_a"][0]


def _build_details_card(
    nome_conglomerado: str,
    cnpj: str,
    segmento: str,
    cidade: str,
    uf: str,
) -> html.Div:
    commercial = clean_commercial_name(nome_conglomerado)
    location = f"{cidade}/{uf}" if cidade and uf else ""
    info_items = [
        ("Razão Social", nome_conglomerado),
        ("Segmento", segmento),
        ("CNPJ", cnpj),
        ("Localização", location or "N/D"),
    ]
    info_divs = [
        html.Div(
            [
                html.Span(label, style=_LABEL_STYLE),
                html.Div(value, style={"fontSize": "14px"}),
            ],
            style={"flex": "1", "minWidth": "120px"},
        )
        for label, value in info_items
    ]
    return html.Div(
        [
            html.H3(
                commercial,
                style={"margin": "0 0 8px 0", "fontSize": "22px"},
            ),
            html.Div(
                info_divs,
                style={
                    "display": "flex",
                    "gap": "24px",
                    "flexWrap": "wrap",
                    "marginTop": "8px",
                },
            ),
        ],
        style={
            "padding": "16px 20px",
            "background": "#f8f9fa",
            "borderRadius": "8px",
            "marginBottom": "24px",
            "borderLeft": "4px solid #1f77b4",
        },
    )


def _format_brl(value: float | None) -> str:
    """Format absolute value in BRL with proper scale."""
    if value is None:
        return "N/D"
    av = abs(value)
    if av >= 1_000_000_000:
        return f"R$ {value / 1_000_000_000:,.1f} bi"
    if av >= 1_000_000:
        return f"R$ {value / 1_000_000:,.1f} mi"
    if av >= 1_000:
        return f"R$ {value / 1_000:,.1f} mil"
    return f"R$ {value:,.0f}"


def _format_pct(value: float | None) -> str:
    """Format fraction as percentage (0.36 -> 36.01%)."""
    if value is None:
        return "N/D"
    return f"{value * 100:,.2f}%"


def _build_big_numbers(
    metrics: list[tuple[str, float | None, str]],
) -> html.Div:
    cards = []
    for label, value, fmt in metrics:
        display = _format_pct(value) if fmt == "pct" else _format_brl(value)
        is_negative = value is not None and value < 0
        color = "#c00" if is_negative else "#1f77b4"
        value_style = {
            "fontSize": "24px",
            "fontWeight": "bold",
            "color": color,
        }
        label_style = {
            "fontSize": "12px",
            "color": "#666",
            "marginTop": "4px",
        }
        card_style = {
            "textAlign": "center",
            "padding": "16px 12px",
            "background": "#fff",
            "border": "1px solid #e0e0e0",
            "borderRadius": "8px",
            "flex": "1",
            "minWidth": "140px",
        }
        cards.append(
            html.Div(
                [
                    html.Div(display, style=value_style),
                    html.Div(label, style=label_style),
                ],
                style=card_style,
            )
        )
    return html.Div(
        cards,
        style={
            "display": "flex",
            "gap": "12px",
            "marginBottom": "24px",
            "flexWrap": "wrap",
        },
    )


def _fmt_table_value(value: float | None, is_pct: bool) -> str:
    """Format a single cell value for report tables."""
    if value is None:
        return ""
    if is_pct:
        return f"{value * 100:.2f}%"
    return f"{value:,.0f}"


def _build_report_table(df: pl.DataFrame, title: str) -> html.Div:
    """Build a pivoted DataTable with proper formatting."""
    if df.is_empty():
        return html.Div()

    pivoted = df.pivot(
        on="ano_mes",
        index="nome_linha",
        values="valor_a",
        aggregate_function="first",
    )
    period_cols = sorted([c for c in pivoted.columns if c != "nome_linha"])
    pivoted = pivoted.select(["nome_linha", *period_cols])

    pdf = pivoted.to_pandas()

    # Convert period columns to object to allow string formatting
    for col in period_cols:
        pdf[col] = pdf[col].astype(object)

    for idx, row in pdf.iterrows():
        is_pct = _is_pct_indicator(str(row["nome_linha"]))
        for col in period_cols:
            pdf.at[idx, col] = _fmt_table_value(row[col], is_pct)

    columns = [{"name": "Indicador", "id": "nome_linha"}]
    for col in period_cols:
        columns.append({"name": str(col), "id": str(col)})

    cell_style = {
        "textAlign": "right",
        "padding": "6px 10px",
        "fontSize": "13px",
        "minWidth": "110px",
    }

    return html.Div(
        [
            html.H3(
                title,
                style={"marginTop": "24px", "marginBottom": "12px"},
            ),
            dash_table.DataTable(
                data=pdf.to_dict("records"),
                columns=columns,
                page_size=30,
                style_table={"overflowX": "auto"},
                style_cell=cell_style,
                style_header={
                    "fontWeight": "bold",
                    "textAlign": "center",
                },
                style_data_conditional=[
                    {
                        "if": {"column_id": "nome_linha"},
                        "textAlign": "left",
                        "fontWeight": "bold",
                        "minWidth": "280px",
                    },
                ],
                sort_action="native",
            ),
        ]
    )


def _build_cosif_dre_table(cosif_df: pl.DataFrame) -> html.Div:
    """Build a pivoted DataTable from COSIF 4010 DRE data."""
    pivoted = cosif_df.pivot(
        on="ano_mes",
        index=["conta", "nome_conta"],
        values="saldo",
        aggregate_function="first",
    )
    period_cols = sorted([c for c in pivoted.columns if c not in ("conta", "nome_conta")])
    pivoted = pivoted.select(["conta", "nome_conta", *period_cols]).sort("conta")

    pdf = pivoted.to_pandas()
    pdf["indicador"] = pdf["conta"] + " - " + pdf["nome_conta"]
    pdf = pdf.drop(columns=["conta", "nome_conta"])

    for col in period_cols:
        pdf[col] = pdf[col].apply(lambda v: f"{v:,.0f}" if v is not None and v == v else "")

    columns = [{"name": "Conta", "id": "indicador"}]
    for col in period_cols:
        columns.append({"name": str(col), "id": str(col)})

    return html.Div(
        [
            html.H3(
                "DRE Detalhada — Balancete 4010 (COSIF)",
                style={"marginTop": "24px", "marginBottom": "12px"},
            ),
            html.P(
                "Dados agregados do Balancete 4010 individual. "
                "Grupo 7 = Receitas, Grupo 8 = Despesas.",
                style={"color": "#666", "fontSize": "12px", "marginBottom": "8px"},
            ),
            dash_table.DataTable(
                data=pdf.to_dict("records"),
                columns=columns,
                page_size=40,
                style_table={"overflowX": "auto"},
                style_cell={
                    "textAlign": "right",
                    "padding": "6px 10px",
                    "fontSize": "13px",
                    "minWidth": "110px",
                },
                style_header={
                    "fontWeight": "bold",
                    "textAlign": "center",
                },
                style_data_conditional=[
                    {
                        "if": {"column_id": "indicador"},
                        "textAlign": "left",
                        "fontWeight": "bold",
                        "minWidth": "350px",
                    },
                ],
                sort_action="native",
            ),
        ]
    )


def _build_pivot_indicator_options(
    capital_df: pl.DataFrame,
    summary_df: pl.DataFrame,
    dre_df: pl.DataFrame,
) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    sources = [
        (capital_df, "Capital", "5"),
        (summary_df, "Resumo", "1"),
        (dre_df, "DRE", "4"),
    ]
    for df, label, report in sources:
        if not df.is_empty():
            indicators = df["nome_linha"].unique().sort().to_list()
            for ind in indicators:
                options.append(
                    {
                        "label": f"[{label}] {ind}",
                        "value": f"{report}|{ind}",
                    }
                )
    return options


def _build_pivot_analysis(
    df: pl.DataFrame,
    indicator: str,
    comparison: str,
) -> html.Div | dash_table.DataTable:
    """Build a year x quarter pivot table with optional YoY/QoQ."""
    filtered = df.filter(df["nome_linha"] == indicator)
    if filtered.is_empty():
        return html.P(
            "Nenhum dado para este indicador.",
            style={"color": "#888"},
        )

    is_pct = _is_pct_indicator(indicator)

    filtered = filtered.group_by("ano_mes").agg(pl.col("valor_a").first())

    # Convert pct fractions to actual percentage for display
    if is_pct and comparison == "abs":
        filtered = filtered.with_columns((pl.col("valor_a") * 100).alias("valor_a"))

    filtered = filtered.with_columns(
        (pl.col("ano_mes") // 100).alias("ano"),
        (pl.col("ano_mes") % 100).alias("mes"),
    ).sort(["ano", "mes"])

    quarter_cols = [3, 6, 9, 12]

    if comparison in ("yoy", "qoq"):
        rows = [
            {
                "ano": r["ano"],
                "mes": r["mes"],
                "valor": r["valor_a"],
            }
            for r in filtered.iter_rows(named=True)
        ]
        ts = pl.DataFrame(rows).sort(["ano", "mes"])

        if comparison == "qoq":
            ts = ts.with_columns(
                pl.when(pl.col("valor").shift(1).abs() > 0)
                .then(
                    (pl.col("valor") - pl.col("valor").shift(1))
                    / pl.col("valor").shift(1).abs()
                    * 100
                )
                .otherwise(None)
                .alias("valor")
            )

        pivoted = ts.pivot(
            on="mes",
            index="ano",
            values="valor",
            aggregate_function="first",
        ).sort("ano")

        if comparison == "yoy":
            for q in quarter_cols:
                col = q if q in pivoted.columns else str(q)
                if col not in pivoted.columns:
                    continue
                pivoted = pivoted.with_columns(
                    pl.when(pl.col(col).shift(1).abs() > 0)
                    .then((pl.col(col) - pl.col(col).shift(1)) / pl.col(col).shift(1).abs() * 100)
                    .otherwise(None)
                    .alias(col)
                )
    else:
        pivoted = filtered.pivot(
            on="mes",
            index="ano",
            values="valor_a",
            aggregate_function="first",
        ).sort("ano")

    for q in quarter_cols:
        if q not in pivoted.columns and str(q) not in pivoted.columns:
            pivoted = pivoted.with_columns(pl.lit(None).cast(pl.Float64).alias(str(q)))

    rename_map = {c: str(c) for c in pivoted.columns if isinstance(c, int)}
    if rename_map:
        pivoted = pivoted.rename(rename_map)

    pdf = pivoted.to_pandas()

    # Format values for display
    month_labels = {
        "3": "Mar",
        "6": "Jun",
        "9": "Set",
        "12": "Dez",
    }
    is_variation = comparison != "abs"
    suffix = " (%)" if is_variation else ""

    for q in quarter_cols:
        qstr = str(q)
        if qstr not in pdf.columns:
            continue
        if is_variation:
            pdf[qstr] = pdf[qstr].apply(lambda v: f"{v:+.1f}" if v is not None and v == v else "")
        elif is_pct:
            pdf[qstr] = pdf[qstr].apply(lambda v: f"{v:.2f}%" if v is not None and v == v else "")
        else:
            pdf[qstr] = pdf[qstr].apply(lambda v: f"{v:,.0f}" if v is not None and v == v else "")

    columns = [{"name": "Ano", "id": "ano"}]
    for q in quarter_cols:
        qstr = str(q)
        if qstr in pdf.columns:
            columns.append(
                {
                    "name": f"{month_labels[qstr]}{suffix}",
                    "id": qstr,
                }
            )

    return dash_table.DataTable(
        data=pdf.to_dict("records"),
        columns=columns,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "right",
            "padding": "6px 10px",
            "fontSize": "13px",
        },
        style_header={"fontWeight": "bold", "textAlign": "center"},
        style_data_conditional=[
            {
                "if": {"column_id": "ano"},
                "textAlign": "left",
                "fontWeight": "bold",
            },
        ],
    )


# ──────────────────────────────────────────────
# Layout
# ──────────────────────────────────────────────

_DD_LABEL = {"fontWeight": "bold", "marginRight": "8px"}

layout = html.Div(
    [
        html.H2("Análise Individual", style={"marginBottom": "16px"}),
        html.Div(
            [
                html.Label("Instituição:", style={"fontWeight": "bold"}),
                dcc.Dropdown(
                    id="individual-institution",
                    placeholder="Selecione uma instituição...",
                    style={"width": "400px"},
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        html.Div(id="individual-details-card"),
        html.Div(id="individual-big-numbers"),
        html.Div(id="individual-basileia-chart"),
        # Pivot analysis section
        html.Div(
            [
                html.Hr(style={"marginTop": "24px"}),
                html.H3(
                    "Análise Pivot",
                    style={"marginBottom": "12px"},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label("Indicador:", style=_DD_LABEL),
                                dcc.Dropdown(
                                    id="pivot-indicator",
                                    placeholder="Selecione...",
                                    style={"width": "400px"},
                                ),
                            ],
                            style={
                                "display": "inline-block",
                                "marginRight": "24px",
                                "verticalAlign": "top",
                            },
                        ),
                        html.Div(
                            [
                                html.Label("Comparação:", style=_DD_LABEL),
                                dcc.Dropdown(
                                    id="pivot-comparison",
                                    options=[
                                        {
                                            "label": "Valor absoluto",
                                            "value": "abs",
                                        },
                                        {
                                            "label": "YoY (ano a ano)",
                                            "value": "yoy",
                                        },
                                        {
                                            "label": "QoQ (tri a tri)",
                                            "value": "qoq",
                                        },
                                    ],
                                    value="abs",
                                    clearable=False,
                                    style={"width": "300px"},
                                ),
                            ],
                            style={
                                "display": "inline-block",
                                "verticalAlign": "top",
                            },
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                html.Div(id="pivot-table-container"),
            ],
            id="pivot-section",
            style={"display": "none"},
        ),
        html.Div(id="individual-report-tables"),
    ]
)


# ──────────────────────────────────────────────
# Callbacks
# ──────────────────────────────────────────────

_OptionsList = list[dict[str, str | int]]


@callback(
    Output("individual-institution", "options"),
    Input("institution-options-store", "data"),
)
def update_dropdown(
    options: _OptionsList | None,
) -> _OptionsList:
    """Populate institution dropdown from shared store."""
    return options or []


@callback(
    Output("individual-details-card", "children"),
    Output("individual-big-numbers", "children"),
    Output("individual-basileia-chart", "children"),
    Output("individual-report-tables", "children"),
    Output("pivot-indicator", "options"),
    Output("pivot-indicator", "value"),
    Output("pivot-section", "style"),
    Input("individual-institution", "value"),
)
def render_institution_content(
    cod_conglomerado: int | None,
) -> tuple[
    object,
    object,
    object,
    object,
    list[dict[str, str]],
    str | None,
    dict[str, str],
]:
    """Render all content when an institution is selected."""
    hidden: dict[str, str] = {"display": "none"}
    empty: list[dict[str, str]] = []

    if cod_conglomerado is None:
        msg = html.P(
            "Selecione uma instituição para visualizar.",
            style={"color": "#888", "fontStyle": "italic"},
        )
        return msg, "", "", "", empty, None, hidden

    # ── Institution details ──
    details_df = get_institution_details(con, cod_conglomerado)
    if not details_df.is_empty():
        row = details_df.row(0, named=True)
        details_card = _build_details_card(
            row["nome_conglomerado"],
            row["cnpj"],
            row["segmento"],
            row["cidade"],
            row["uf"],
        )
    else:
        details_card = html.Div()

    # ── Fetch all report data ──
    capital_df = get_capital_indicators(con, cod_conglomerado)
    summary_df = get_summary_indicators(con, cod_conglomerado)
    dre_df = get_dre_indicators(con, cod_conglomerado)

    # ── Big numbers (latest period) ──
    basileia_val = _extract_latest_value(capital_df, "(?i)basileia", exclude="(?i)amplo")
    lucro_val = _extract_latest_value(summary_df, "(?i)lucro l")
    metrics: list[tuple[str, float | None, str]] = [
        ("Lucro Líquido", lucro_val, "brl"),
        (
            "Patrimônio Líquido",
            _extract_latest_value(summary_df, "(?i)patrim.nio l"),
            "brl",
        ),
        ("Índice de Basileia", basileia_val, "pct"),
        (
            "Ativo Total",
            _extract_latest_value(summary_df, "(?i)ativo total"),
            "brl",
        ),
        (
            "Captações",
            _extract_latest_value(summary_df, "(?i)capta"),
            "brl",
        ),
        (
            "Carteira de Crédito",
            _extract_latest_value(summary_df, "(?i)carteira de cr"),
            "brl",
        ),
    ]
    big_numbers = _build_big_numbers(metrics)

    # ── Basileia bar chart (values * 100 for %) ──
    basileia_chart: object = html.Div()
    if not capital_df.is_empty():
        basileia = capital_df.filter(
            capital_df["nome_linha"].str.contains("(?i)basileia")
            & ~capital_df["nome_linha"].str.contains("(?i)amplo")
        )
        if not basileia.is_empty():
            basileia = basileia.group_by("ano_mes").agg(pl.col("valor_a").max())
            basileia = basileia.with_columns(
                pl.col("ano_mes").cast(pl.Utf8).alias("periodo"),
                (pl.col("valor_a") * 100).alias("valor_pct"),
            ).sort("ano_mes")

            fig = px.bar(
                basileia.to_pandas(),
                x="periodo",
                y="valor_pct",
                title="Índice de Basileia",
                labels={
                    "periodo": "Período",
                    "valor_pct": "%",
                },
            )
            fig.add_hline(
                y=10.5,
                line_dash="dash",
                line_color="red",
                annotation_text="Mínimo regulatório (10,5%)",
            )
            fig.update_layout(
                xaxis_title="Período (AAAAMM)",
                yaxis_title="Índice (%)",
                height=400,
            )
            basileia_chart = dcc.Graph(figure=fig)

    # ── Report tables ──
    tables: list[object] = []
    tables.append(
        _build_report_table(
            capital_df,
            "Indicadores de Capital (Relatório 5)",
        )
    )
    tables.append(_build_report_table(summary_df, "Resumo (Relatório 1)"))
    # COSIF 4010 detailed DRE (if available)
    cosif_dre = get_cosif_dre(con, cod_conglomerado)
    if not cosif_dre.is_empty():
        tables.append(_build_cosif_dre_table(cosif_dre))

    tables.append(
        html.Div(
            [
                _build_report_table(
                    dre_df,
                    "DRE Resumida — IF.data (Relatório 4)",
                ),
                html.P(
                    "Nota: A DRE do IF.data contém apenas linhas resumidas. "
                    "Receitas/despesas detalhadas estão no Balancete 4010 acima "
                    "(quando disponível).",
                    style={
                        "color": "#888",
                        "fontSize": "12px",
                        "marginTop": "4px",
                    },
                ),
            ]
        )
    )

    # Raw data table (keep existing)
    if not capital_df.is_empty():
        raw_pdf = capital_df.to_pandas()
        tables.append(
            html.Div(
                [
                    html.H3(
                        "Dados Brutos — Capital",
                        style={"marginTop": "24px"},
                    ),
                    dash_table.DataTable(
                        data=raw_pdf.to_dict("records"),
                        columns=[{"name": c, "id": c} for c in raw_pdf.columns],
                        page_size=20,
                        style_table={"overflowX": "auto"},
                        style_cell={
                            "textAlign": "left",
                            "padding": "8px",
                        },
                        style_header={"fontWeight": "bold"},
                        sort_action="native",
                        filter_action="native",
                    ),
                ]
            )
        )

    # ── Pivot indicator options ──
    pivot_options = _build_pivot_indicator_options(capital_df, summary_df, dre_df)
    default_ind = pivot_options[0]["value"] if pivot_options else None
    show = {"display": "block"} if pivot_options else hidden

    report_tables = html.Div(tables) if tables else html.Div()

    return (
        details_card,
        big_numbers,
        basileia_chart,
        report_tables,
        pivot_options,
        default_ind,
        show,
    )


@callback(
    Output("pivot-table-container", "children"),
    Input("pivot-indicator", "value"),
    Input("pivot-comparison", "value"),
    Input("individual-institution", "value"),
)
def render_pivot_table(
    indicator_encoded: str | None,
    comparison: str,
    cod_conglomerado: int | None,
) -> object:
    """Render pivot analysis table."""
    if not indicator_encoded or not cod_conglomerado:
        return html.P(
            "Selecione um indicador.",
            style={"color": "#888", "fontStyle": "italic"},
        )

    parts = indicator_encoded.split("|", 1)
    if len(parts) != 2:
        return html.P("Indicador inválido.", style={"color": "#c00"})
    report, indicator = parts

    query_map = {
        "5": get_capital_indicators,
        "1": get_summary_indicators,
        "4": get_dre_indicators,
    }
    query_fn = query_map.get(report)
    if not query_fn:
        return html.P("Relatório inválido.", style={"color": "#c00"})

    df = query_fn(con, cod_conglomerado)
    return _build_pivot_analysis(df, indicator, comparison)
