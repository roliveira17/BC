from __future__ import annotations

import dash
import plotly.express as px
import polars as pl
from dash import Input, Output, callback, dash_table, dcc, html

from src.db import get_connection
from src.peers import PEER_GROUP_MAP
from src.queries import (
    BALANCETES_KPI_MAP,
    get_balancetes_kpi_trend,
    get_balancetes_multi_kpi,
    get_balancetes_ratio_trend,
    get_balancetes_trend,
    list_balancetes_periods,
)
from src.settings import Settings

dash.register_page(__name__, path="/balancetes", name="Balancetes 4040")

settings = Settings()
con = get_connection(settings.duckdb_path)

KPI_OPTIONS = [{"label": name, "value": code} for name, code in BALANCETES_KPI_MAP.items()]

RATIO_LABELS = {
    "ratio:roe": "ROE (Resultado / PL)",
    "ratio:roa": "ROA (Resultado / Ativo)",
    "ratio:alavancagem": "Alavancagem (Ativo / PL)",
}
RATIO_OPTIONS = [{"label": v, "value": k} for k, v in RATIO_LABELS.items()]

TREND_OPTIONS = KPI_OPTIONS + [{"label": "———", "value": "", "disabled": True}] + RATIO_OPTIONS

layout = html.Div(
    [
        html.H2("Top 50 Conglomerados — Patrimônio Líquido", style={"marginBottom": "16px"}),
        # Period selector
        html.Div(
            [
                html.Label("Período:", style={"fontWeight": "bold", "marginRight": "8px"}),
                dcc.Dropdown(
                    id="balancetes-period",
                    placeholder="Selecione um período...",
                    style={"width": "200px", "display": "inline-block"},
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        # Bar chart
        html.Div(id="balancetes-bar-chart"),
        # Multi-KPI data table
        html.Div(id="balancetes-table", style={"marginTop": "24px"}),
        # Trend section
        html.Hr(style={"marginTop": "32px"}),
        html.H3("Tendência por Instituição", style={"marginBottom": "16px"}),
        html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            "Instituição:",
                            style={"fontWeight": "bold", "marginRight": "8px"},
                        ),
                        dcc.Dropdown(
                            id="balancetes-institution",
                            placeholder="Selecione uma instituição do Top 50...",
                            style={"width": "400px", "display": "inline-block"},
                        ),
                    ],
                    style={"marginRight": "24px", "display": "inline-block"},
                ),
                html.Div(
                    [
                        html.Label(
                            "Indicador:",
                            style={"fontWeight": "bold", "marginRight": "8px"},
                        ),
                        dcc.Dropdown(
                            id="balancetes-kpi",
                            options=TREND_OPTIONS,
                            value=BALANCETES_KPI_MAP["Patrimônio Líquido"],
                            clearable=False,
                            style={"width": "300px", "display": "inline-block"},
                        ),
                    ],
                    style={"display": "inline-block"},
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        html.Div(id="balancetes-trend-chart"),
    ]
)


def _fmt_cod(cod: object) -> str:
    """Format cod_conglomerado as C00xxxxx or '-' if missing."""
    if cod is None or (isinstance(cod, float) and str(cod) == "nan"):
        return "-"
    return f"C{int(cod):07d}"


@callback(
    Output("balancetes-period", "options"),
    Output("balancetes-period", "value"),
    Input("balancetes-period", "id"),
)
def load_periods(_: str) -> tuple[list[dict[str, str | int]], int | None]:
    """Load available periods on page render."""
    periods = list_balancetes_periods(con)
    options = [{"label": str(p), "value": p} for p in periods]
    value = periods[0] if periods else None
    return options, value


@callback(
    Output("balancetes-bar-chart", "children"),
    Output("balancetes-table", "children"),
    Output("balancetes-institution", "options"),
    Input("balancetes-period", "value"),
    Input("peer-group-filter", "value"),
)
def render_top50(
    ano_mes: int | None,
    peer_group: str,
) -> tuple[object, object, list[dict[str, str]]]:
    """Render bar chart and multi-KPI data table for Top 50."""
    if ano_mes is None:
        empty_msg = html.P(
            "Nenhum dado de balancetes disponível. "
            "Execute 'python -m scripts.refresh_balancetes' para carregar dados.",
            style={"color": "#c00"},
        )
        return empty_msg, "", []

    df = get_balancetes_multi_kpi(con, ano_mes)
    if peer_group != "ALL":
        codes = PEER_GROUP_MAP.get(peer_group, [])
        if codes:
            df = df.filter(pl.col("cod_conglomerado").is_in(codes))
    if df.is_empty():
        empty_msg = html.P("Nenhum dado para o período selecionado.", style={"color": "#888"})
        return empty_msg, "", []

    pdf = df.to_pandas()

    # Format cod_conglomerado for display
    pdf["cod_display"] = pdf["cod_conglomerado"].apply(_fmt_cod)

    # Bar chart (by PL)
    fig = px.bar(
        pdf.sort_values("rank", ascending=False),
        x="patrimonio_liquido",
        y="nome_inst",
        orientation="h",
        title=f"Top 50 por Patrimônio Líquido — {ano_mes}",
        labels={
            "patrimonio_liquido": "PL (R$ mil)",
            "nome_inst": "Instituição",
        },
        color="patrimonio_liquido",
        color_continuous_scale="Blues",
    )
    fig.update_layout(
        height=max(600, len(pdf) * 22),
        yaxis={"categoryorder": "total ascending"},
    )
    bar_chart = dcc.Graph(figure=fig)

    # Compute derived ratios
    pdf["roe"] = pdf["resultado_liquido"] / pdf["patrimonio_liquido"]
    pdf["roa"] = pdf["resultado_liquido"] / pdf["ativo_total"]
    pdf["alavancagem"] = pdf["ativo_total"] / pdf["patrimonio_liquido"]

    # Multi-KPI data table
    saldo_cols = [
        "patrimonio_liquido", "ativo_total",
        "operacoes_credito", "depositos", "resultado_liquido",
    ]
    ratio_cols = ["roe", "roa", "alavancagem"]
    all_num_cols = saldo_cols + ratio_cols
    table_df = pdf[
        ["rank", "nome_inst", "cnpj8", "cod_display", "nome_conglomerado"] + all_num_cols
    ].copy()
    for col in saldo_cols:
        table_df[col] = table_df[col].round(2)
    for col in ratio_cols:
        table_df[col] = table_df[col].round(4)

    table = dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[
            {"name": "#", "id": "rank"},
            {"name": "Instituição", "id": "nome_inst"},
            {"name": "CNPJ8", "id": "cnpj8"},
            {"name": "Cód. Congl.", "id": "cod_display"},
            {"name": "Conglomerado", "id": "nome_conglomerado"},
            {"name": "PL (R$ mil)", "id": "patrimonio_liquido", "type": "numeric"},
            {"name": "Ativo Total", "id": "ativo_total", "type": "numeric"},
            {"name": "Op. Crédito", "id": "operacoes_credito", "type": "numeric"},
            {"name": "Depósitos", "id": "depositos", "type": "numeric"},
            {"name": "Resultado", "id": "resultado_liquido", "type": "numeric"},
            {"name": "ROE", "id": "roe", "type": "numeric"},
            {"name": "ROA", "id": "roa", "type": "numeric"},
            {"name": "Alav.", "id": "alavancagem", "type": "numeric"},
        ],
        page_size=50,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "8px"},
        style_header={"fontWeight": "bold"},
        sort_action="native",
        filter_action="native",
    )

    # Institution options for trend dropdown — include cod_conglomerado
    inst_options = [
        {
            "label": (
                f"{row['rank']}. {row['nome_inst']}"
                f" ({_fmt_cod(row['cod_conglomerado'])})"
            ),
            "value": row["cnpj8"],
        }
        for row in pdf.to_dict("records")
    ]

    return bar_chart, table, inst_options


@callback(
    Output("balancetes-trend-chart", "children"),
    Input("balancetes-institution", "value"),
    Input("balancetes-kpi", "value"),
)
def render_trend(cnpj8: str | None, kpi_conta: str | None) -> object:
    """Render KPI or ratio trend line chart for a selected institution."""
    if cnpj8 is None:
        return html.P(
            "Selecione uma instituição para ver a tendência.",
            style={"color": "#888", "fontStyle": "italic"},
        )
    if not kpi_conta:
        return html.P("Selecione um indicador.", style={"color": "#888"})

    # --- Derived ratios ---
    if kpi_conta.startswith("ratio:"):
        ratio_key = kpi_conta.split(":", 1)[1]  # roe | roa | alavancagem
        kpi_name = RATIO_LABELS.get(kpi_conta, ratio_key.upper())

        df = get_balancetes_ratio_trend(con, cnpj8)
        if df.is_empty() or ratio_key not in df.columns:
            return html.P("Nenhum dado de tendência disponível.", style={"color": "#888"})
        pdf = df.to_pandas()

        fig = px.line(
            pdf,
            x="ano_mes",
            y=ratio_key,
            title=f"Evolução — {kpi_name} — CNPJ8: {cnpj8}",
            labels={"ano_mes": "Período (AAAAMM)", ratio_key: kpi_name},
            markers=True,
        )
        fig.update_layout(height=400)
        return dcc.Graph(figure=fig)

    # --- COSIF account balances ---
    kpi_name = next(
        (name for name, code in BALANCETES_KPI_MAP.items() if code == kpi_conta),
        "KPI",
    )

    if kpi_conta == BALANCETES_KPI_MAP["Patrimônio Líquido"]:
        df = get_balancetes_trend(con, cnpj8)
        if df.is_empty():
            return html.P("Nenhum dado de tendência disponível.", style={"color": "#888"})
        pdf = df.to_pandas()
        y_col = "patrimonio_liquido"
    else:
        df = get_balancetes_kpi_trend(con, cnpj8, kpi_conta)
        if df.is_empty():
            return html.P("Nenhum dado de tendência disponível.", style={"color": "#888"})
        pdf = df.to_pandas()
        y_col = "valor"

    fig = px.line(
        pdf,
        x="ano_mes",
        y=y_col,
        title=f"Evolução — {kpi_name} — CNPJ8: {cnpj8}",
        labels={"ano_mes": "Período (AAAAMM)", y_col: "R$ mil"},
        markers=True,
    )
    fig.update_layout(height=400)
    return dcc.Graph(figure=fig)
