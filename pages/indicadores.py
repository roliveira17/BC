from __future__ import annotations

import dash
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html

from src.db import get_connection
from src.queries import (
    get_financial_ratios,
    get_market_share_pl,
    get_ratio_ranking,
)
from src.settings import Settings

dash.register_page(
    __name__, path="/indicadores", name="Indicadores Financeiros"
)

settings = Settings()
con = get_connection(settings.duckdb_path)

# ──────────────────────────────────────────────
# Ratio metadata: label, SQL column, unit, format hint
# ──────────────────────────────────────────────

RATIO_DEFS: list[dict[str, str]] = [
    {"label": "ROE", "col": "roe", "unit": "%", "group": "Rentabilidade"},
    {"label": "ROA", "col": "roa", "unit": "%", "group": "Rentabilidade"},
    {
        "label": "Loan-to-Deposit",
        "col": "loan_to_deposit",
        "unit": "%",
        "group": "Estrutura",
    },
    {
        "label": "Credit Intensity",
        "col": "credit_intensity",
        "unit": "%",
        "group": "Estrutura",
    },
    {
        "label": "Securities Share",
        "col": "securities_share",
        "unit": "%",
        "group": "Estrutura",
    },
    {
        "label": "Alavancagem",
        "col": "leverage",
        "unit": "x",
        "group": "Estrutura",
    },
    {
        "label": "Debt/Equity",
        "col": "debt_equity",
        "unit": "x",
        "group": "Estrutura",
    },
    {
        "label": "Funding Dependency",
        "col": "funding_dependency",
        "unit": "%",
        "group": "Estrutura",
    },
    {
        "label": "PR / Ativo",
        "col": "pr_coverage",
        "unit": "%",
        "group": "Capital",
    },
    {
        "label": "Basileia",
        "col": "basileia",
        "unit": "%",
        "group": "Capital",
    },
    {
        "label": "Capital Principal (CET1)",
        "col": "capital_principal",
        "unit": "%",
        "group": "Capital",
    },
    {
        "label": "Capital Nível I",
        "col": "capital_nivel1",
        "unit": "%",
        "group": "Capital",
    },
    {
        "label": "Capital Excess",
        "col": "capital_excess",
        "unit": "ppts",
        "group": "Capital",
    },
    {
        "label": "Razão de Alavancagem",
        "col": "razao_alavancagem",
        "unit": "%",
        "group": "Capital",
    },
    {
        "label": "Taxa Efetiva IR",
        "col": "tax_rate",
        "unit": "%",
        "group": "DRE",
    },
]

RATIO_COL_TO_LABEL = {r["col"]: r["label"] for r in RATIO_DEFS}
RATIO_COL_TO_UNIT = {r["col"]: r["unit"] for r in RATIO_DEFS}

# Big number cards to show
BIG_NUMBER_RATIOS = [
    "roe", "roa", "loan_to_deposit", "basileia",
    "capital_principal", "leverage",
]

SEGMENT_COLORS: dict[str, str] = {
    "S1": "#1f77b4",
    "S2": "#ff7f0e",
    "S3": "#2ca02c",
    "S4": "#d62728",
    "S5": "#9467bd",
    "": "#999999",
}


def _format_ratio_value(value: float | None, unit: str) -> str:
    if value is None:
        return "N/D"
    if unit == "x":
        return f"{value:.1f}x"
    if unit == "ppts":
        return f"{value:+.2f} ppts"
    return f"{value:.2f}%"


def _period_label(ano_mes: int) -> str:
    year = ano_mes // 100
    month = ano_mes % 100
    quarter_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
    q = quarter_map.get(month, f"M{month}")
    return f"{q}/{year}"


def _build_ratio_big_numbers(
    row: dict[str, object],
) -> html.Div:
    cards = []
    for col in BIG_NUMBER_RATIOS:
        value = row.get(col)
        unit = RATIO_COL_TO_UNIT[col]
        label = RATIO_COL_TO_LABEL[col]
        display = _format_ratio_value(value, unit)

        is_negative = value is not None and isinstance(value, int | float) and value < 0
        color = "#c00" if is_negative else "#1f77b4"

        cards.append(
            html.Div(
                [
                    html.Div(
                        display,
                        style={
                            "fontSize": "24px",
                            "fontWeight": "bold",
                            "color": color,
                        },
                    ),
                    html.Div(
                        label,
                        style={
                            "fontSize": "12px",
                            "color": "#666",
                            "marginTop": "4px",
                        },
                    ),
                ],
                style={
                    "textAlign": "center",
                    "padding": "16px 12px",
                    "background": "#fff",
                    "border": "1px solid #e0e0e0",
                    "borderRadius": "8px",
                    "flex": "1",
                    "minWidth": "140px",
                },
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


def _build_ratios_table(df: pl.DataFrame) -> html.Div:
    if df.is_empty():
        return html.Div()

    periods = sorted(df["ano_mes"].to_list())
    period_labels = {p: _period_label(p) for p in periods}

    rows = []
    for rd in RATIO_DEFS:
        col = rd["col"]
        unit = rd["unit"]
        row_data: dict[str, str] = {
            "indicador": f"{rd['label']} ({unit})",
            "grupo": rd["group"],
        }
        for p in periods:
            p_row = df.filter(pl.col("ano_mes") == p)
            if p_row.is_empty() or col not in p_row.columns:
                row_data[str(p)] = ""
            else:
                val = p_row[col][0]
                row_data[str(p)] = _format_ratio_value(val, unit)
        rows.append(row_data)

    columns = [
        {"name": "Grupo", "id": "grupo"},
        {"name": "Indicador", "id": "indicador"},
    ]
    for p in periods:
        columns.append({"name": period_labels[p], "id": str(p)})

    return html.Div(
        [
            html.H3(
                "Tabela de Indicadores",
                style={"marginTop": "24px", "marginBottom": "12px"},
            ),
            dash_table.DataTable(
                data=rows,
                columns=columns,
                page_size=20,
                style_table={"overflowX": "auto"},
                style_cell={
                    "textAlign": "right",
                    "padding": "6px 10px",
                    "fontSize": "13px",
                    "minWidth": "100px",
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
                        "minWidth": "200px",
                    },
                    {
                        "if": {"column_id": "grupo"},
                        "textAlign": "left",
                        "minWidth": "120px",
                        "color": "#666",
                    },
                ],
                sort_action="native",
                merge_duplicate_headers=True,
            ),
        ]
    )


# ──────────────────────────────────────────────
# Layout
# ──────────────────────────────────────────────

_DD_STYLE = {"width": "400px"}

layout = html.Div(
    [
        html.H2(
            "Indicadores Financeiros",
            style={"marginBottom": "16px"},
        ),
        html.P(
            "Ratios calculados a partir dos Reports 1, 4 e 5 do IF.data.",
            style={"color": "#666", "marginBottom": "16px"},
        ),
        # Institution selector
        html.Div(
            [
                html.Label(
                    "Instituição:",
                    style={"fontWeight": "bold"},
                ),
                dcc.Dropdown(
                    id="ind-institution",
                    placeholder="Selecione uma instituição...",
                    style=_DD_STYLE,
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        # Big numbers
        html.Div(id="ind-big-numbers"),
        # Ratios table
        html.Div(id="ind-ratios-table"),
        # Evolution chart section
        html.Div(
            [
                html.Hr(style={"marginTop": "24px"}),
                html.H3(
                    "Evolução Temporal",
                    style={"marginBottom": "12px"},
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "Indicador:",
                                    style={
                                        "fontWeight": "bold",
                                        "marginRight": "8px",
                                    },
                                ),
                                dcc.Dropdown(
                                    id="ind-chart-ratio",
                                    options=[
                                        {
                                            "label": r["label"],
                                            "value": r["col"],
                                        }
                                        for r in RATIO_DEFS
                                    ],
                                    value="roe",
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
                dcc.Graph(id="ind-evolution-chart"),
            ],
            id="ind-chart-section",
            style={"display": "none"},
        ),
        # Ranking section
        html.Div(
            [
                html.Hr(style={"marginTop": "24px"}),
                html.H3(
                    "Ranking por Indicador",
                    style={"marginBottom": "12px"},
                ),
                html.Div(
                    [
                        html.Label(
                            "Indicador:",
                            style={
                                "fontWeight": "bold",
                                "marginRight": "8px",
                            },
                        ),
                        dcc.Dropdown(
                            id="ind-ranking-ratio",
                            options=[
                                {
                                    "label": r["label"],
                                    "value": r["col"],
                                }
                                for r in RATIO_DEFS
                            ],
                            value="roe",
                            clearable=False,
                            style={"width": "300px"},
                        ),
                    ],
                    style={"marginBottom": "16px"},
                ),
                dcc.Graph(id="ind-ranking-chart"),
            ],
        ),
        # Market share section
        html.Div(
            [
                html.Hr(style={"marginTop": "24px"}),
                html.H3(
                    "Market Share por Patrimônio Líquido",
                    style={"marginBottom": "12px"},
                ),
                html.Div(id="ind-market-share"),
            ],
        ),
    ]
)


# ──────────────────────────────────────────────
# Callbacks
# ──────────────────────────────────────────────

_OptionsList = list[dict[str, str | int]]


@callback(
    Output("ind-institution", "options"),
    Input("institution-options-store", "data"),
)
def update_dropdown(options: _OptionsList | None) -> _OptionsList:
    return options or []


@callback(
    Output("ind-big-numbers", "children"),
    Output("ind-ratios-table", "children"),
    Output("ind-chart-section", "style"),
    Input("ind-institution", "value"),
)
def render_ratios(
    cod_conglomerado: int | None,
) -> tuple[object, object, dict[str, str]]:
    hidden: dict[str, str] = {"display": "none"}

    if cod_conglomerado is None:
        return (
            html.P(
                "Selecione uma instituição para visualizar.",
                style={"color": "#888", "fontStyle": "italic"},
            ),
            "",
            hidden,
        )

    df = get_financial_ratios(con, cod_conglomerado)
    if df.is_empty():
        return (
            html.P(
                "Nenhum dado disponível para esta instituição.",
                style={"color": "#888"},
            ),
            "",
            hidden,
        )

    # Big numbers from latest period
    latest = df.filter(pl.col("ano_mes") == df["ano_mes"].max())
    latest_row = latest.row(0, named=True)
    big_numbers = _build_ratio_big_numbers(latest_row)

    # Table with all ratios across periods
    ratios_table = _build_ratios_table(df)

    return big_numbers, ratios_table, {"display": "block"}


@callback(
    Output("ind-evolution-chart", "figure"),
    Input("ind-chart-ratio", "value"),
    Input("ind-institution", "value"),
)
def render_evolution_chart(
    ratio_col: str | None,
    cod_conglomerado: int | None,
) -> go.Figure:
    fig = go.Figure()
    if not ratio_col or not cod_conglomerado:
        return fig

    df = get_financial_ratios(con, cod_conglomerado)
    if df.is_empty() or ratio_col not in df.columns:
        return fig

    chart_df = df.select(["ano_mes", ratio_col]).drop_nulls()
    if chart_df.is_empty():
        return fig

    label = RATIO_COL_TO_LABEL.get(ratio_col, ratio_col)
    unit = RATIO_COL_TO_UNIT.get(ratio_col, "")

    chart_df = chart_df.with_columns(
        pl.col("ano_mes").cast(pl.Utf8).alias("periodo")
    ).sort("ano_mes")

    pdf = chart_df.to_pandas()

    fig = px.line(
        pdf,
        x="periodo",
        y=ratio_col,
        markers=True,
        title=f"Evolução — {label}",
        labels={
            "periodo": "Período",
            ratio_col: f"{label} ({unit})",
        },
    )

    # Reference lines for capital ratios
    if ratio_col == "basileia":
        fig.add_hline(
            y=10.5,
            line_dash="dash",
            line_color="red",
            annotation_text="Mínimo regulatório (10,5%)",
        )
    elif ratio_col == "capital_principal":
        fig.add_hline(
            y=4.5,
            line_dash="dash",
            line_color="red",
            annotation_text="Mínimo CET1 (4,5%)",
        )

    fig.update_layout(height=400, xaxis_title="Período (AAAAMM)")
    return fig


@callback(
    Output("ind-ranking-chart", "figure"),
    Input("ind-ranking-ratio", "value"),
)
def render_ranking_chart(ratio_col: str | None) -> go.Figure:
    fig = go.Figure()
    if not ratio_col:
        return fig

    df = get_ratio_ranking(con, ratio_col)
    if df.is_empty():
        return fig

    label = RATIO_COL_TO_LABEL.get(ratio_col, ratio_col)
    unit = RATIO_COL_TO_UNIT.get(ratio_col, "")

    # Filter out extreme outliers for better visualization
    if df.shape[0] > 20:
        df = df.head(20)

    # Clean names for display
    from pages.individual import clean_commercial_name

    pdf = df.to_pandas()
    pdf["nome_clean"] = pdf["nome_conglomerado"].apply(
        clean_commercial_name
    )

    # Reverse for horizontal bar (top at top)
    pdf = pdf.iloc[::-1]

    colors = pdf["segmento"].map(SEGMENT_COLORS).fillna("#999999")

    fig = go.Figure(
        go.Bar(
            x=pdf["valor"],
            y=pdf["nome_clean"],
            orientation="h",
            marker_color=colors,
            text=pdf["valor"].apply(
                lambda v: f"{v:.1f}" if v == v else ""
            ),
            textposition="outside",
        )
    )

    fig.update_layout(
        title=f"Top 20 — {label} ({unit})",
        xaxis_title=f"{label} ({unit})",
        yaxis_title="",
        height=max(400, len(pdf) * 28),
        margin={"l": 250},
    )

    return fig


@callback(
    Output("ind-market-share", "children"),
    Input("ind-institution", "value"),
)
def render_market_share(
    _cod_conglomerado: int | None,
) -> object:
    df = get_market_share_pl(con, top_n=20)
    if df.is_empty():
        return html.P(
            "Nenhum dado disponível.",
            style={"color": "#888"},
        )

    from pages.individual import clean_commercial_name

    pdf = df.to_pandas()
    pdf["nome_clean"] = pdf["nome_conglomerado"].apply(
        clean_commercial_name
    )

    # Treemap
    fig = px.treemap(
        pdf,
        path=["nome_clean"],
        values="pl_value",
        color="market_share_pct",
        color_continuous_scale="Blues",
        title="Market Share por Patrimônio Líquido (Top 20)",
        custom_data=["market_share_pct"],
    )
    fig.update_traces(
        texttemplate="%{label}<br>%{customdata[0]:.1f}%",
    )
    fig.update_layout(height=500)

    # Table
    table_data = []
    for i, row in pdf.iterrows():
        pl_val = row["pl_value"]
        if pl_val >= 1_000_000_000:
            pl_fmt = f"R$ {pl_val / 1e9:.1f} bi"
        elif pl_val >= 1_000_000:
            pl_fmt = f"R$ {pl_val / 1e6:.1f} mi"
        else:
            pl_fmt = f"R$ {pl_val:,.0f}"
        table_data.append({
            "rank": i + 1,
            "instituicao": row["nome_clean"],
            "segmento": row["segmento"],
            "pl": pl_fmt,
            "share": f"{row['market_share_pct']:.2f}%",
        })

    table = dash_table.DataTable(
        data=table_data,
        columns=[
            {"name": "#", "id": "rank"},
            {"name": "Instituição", "id": "instituicao"},
            {"name": "Segmento", "id": "segmento"},
            {"name": "Patrimônio Líquido", "id": "pl"},
            {"name": "Market Share", "id": "share"},
        ],
        page_size=20,
        style_table={"overflowX": "auto"},
        style_cell={
            "textAlign": "right",
            "padding": "6px 10px",
            "fontSize": "13px",
        },
        style_header={"fontWeight": "bold", "textAlign": "center"},
        style_data_conditional=[
            {
                "if": {"column_id": "instituicao"},
                "textAlign": "left",
                "fontWeight": "bold",
                "minWidth": "250px",
            },
            {
                "if": {"column_id": "rank"},
                "textAlign": "center",
                "width": "40px",
            },
        ],
        sort_action="native",
    )

    return html.Div(
        [
            dcc.Graph(figure=fig),
            html.Div(table, style={"marginTop": "16px"}),
        ]
    )
