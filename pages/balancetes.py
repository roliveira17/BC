from __future__ import annotations

import dash
import plotly.express as px
from dash import Input, Output, callback, dash_table, dcc, html

from src.db import get_connection
from src.queries import get_balancetes_top50, get_balancetes_trend, list_balancetes_periods
from src.settings import Settings

dash.register_page(__name__, path="/balancetes", name="Balancetes 4040")

settings = Settings()
con = get_connection(settings.duckdb_path)

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
        # Data table
        html.Div(id="balancetes-table", style={"marginTop": "24px"}),
        # Trend section
        html.Hr(style={"marginTop": "32px"}),
        html.H3("Tendência de PL por Instituição", style={"marginBottom": "16px"}),
        html.Div(
            [
                html.Label(
                    "Instituição:", style={"fontWeight": "bold", "marginRight": "8px"}
                ),
                dcc.Dropdown(
                    id="balancetes-institution",
                    placeholder="Selecione uma instituição do Top 50...",
                    style={"width": "400px", "display": "inline-block"},
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        html.Div(id="balancetes-trend-chart"),
    ]
)


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
)
def render_top50(
    ano_mes: int | None,
) -> tuple[object, object, list[dict[str, str]]]:
    """Render bar chart and data table for Top 50."""
    if ano_mes is None:
        empty_msg = html.P(
            "Nenhum dado de balancetes disponível. "
            "Execute 'python -m scripts.refresh_balancetes' para carregar dados.",
            style={"color": "#c00"},
        )
        return empty_msg, "", []

    df = get_balancetes_top50(con, ano_mes)
    if df.is_empty():
        empty_msg = html.P("Nenhum dado para o período selecionado.", style={"color": "#888"})
        return empty_msg, "", []

    pdf = df.to_pandas()

    # Bar chart
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

    # Data table
    table_df = pdf[["rank", "nome_inst", "cnpj8", "nome_conglomerado", "patrimonio_liquido"]]
    table_df = table_df.copy()
    table_df["patrimonio_liquido"] = table_df["patrimonio_liquido"].round(2)
    table = dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[
            {"name": "#", "id": "rank"},
            {"name": "Instituição", "id": "nome_inst"},
            {"name": "CNPJ8", "id": "cnpj8"},
            {"name": "Conglomerado", "id": "nome_conglomerado"},
            {"name": "PL (R$ mil)", "id": "patrimonio_liquido", "type": "numeric"},
        ],
        page_size=50,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "8px"},
        style_header={"fontWeight": "bold"},
        sort_action="native",
        filter_action="native",
    )

    # Institution options for trend dropdown
    inst_options = [
        {"label": f"{row['rank']}. {row['nome_inst']}", "value": row["cnpj8"]}
        for row in pdf.to_dict("records")
    ]

    return bar_chart, table, inst_options


@callback(
    Output("balancetes-trend-chart", "children"),
    Input("balancetes-institution", "value"),
)
def render_trend(cnpj8: str | None) -> object:
    """Render PL trend line chart for a selected institution."""
    if cnpj8 is None:
        return html.P(
            "Selecione uma instituição para ver a tendência.",
            style={"color": "#888", "fontStyle": "italic"},
        )

    df = get_balancetes_trend(con, cnpj8)
    if df.is_empty():
        return html.P("Nenhum dado de tendência disponível.", style={"color": "#888"})

    pdf = df.to_pandas()
    fig = px.line(
        pdf,
        x="ano_mes",
        y="patrimonio_liquido",
        title=f"Evolução do Patrimônio Líquido — CNPJ8: {cnpj8}",
        labels={"ano_mes": "Período (AAAAMM)", "patrimonio_liquido": "PL (R$ mil)"},
        markers=True,
    )
    fig.update_layout(height=400)
    return dcc.Graph(figure=fig)
