from __future__ import annotations

import dash
import plotly.express as px
from dash import Input, Output, callback, dash_table, dcc, html

from src.db import get_connection
from src.queries import (
    compare_institutions,
    get_available_indicators,
    get_segment_ranking,
)
from src.settings import Settings

dash.register_page(__name__, path="/comparison", name="Comparação")

settings = Settings()
con = get_connection(settings.duckdb_path)

REPORTS = [
    {"label": "Resumo (Relatório 1)", "value": "1"},
    {"label": "DRE (Relatório 4)", "value": "4"},
    {"label": "Capital (Relatório 5)", "value": "5"},
]

layout = html.Div(
    [
        html.H2("Comparação entre Instituições", style={"marginBottom": "16px"}),
        # Controls
        html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            "Instituições (selecione 2-8):",
                            style={"fontWeight": "bold"},
                        ),
                        dcc.Dropdown(
                            id="comparison-institutions",
                            multi=True,
                            placeholder="Selecione instituições...",
                            style={"width": "100%"},
                        ),
                    ],
                    style={"flex": "2", "marginRight": "16px"},
                ),
                html.Div(
                    [
                        html.Label("Relatório:", style={"fontWeight": "bold"}),
                        dcc.Dropdown(
                            id="comparison-report",
                            options=REPORTS,
                            value="5",
                            clearable=False,
                            style={"width": "100%"},
                        ),
                    ],
                    style={"flex": "1", "marginRight": "16px"},
                ),
                html.Div(
                    [
                        html.Label("Indicador:", style={"fontWeight": "bold"}),
                        dcc.Dropdown(
                            id="comparison-indicator",
                            placeholder="Selecione um indicador...",
                            style={"width": "100%"},
                        ),
                    ],
                    style={"flex": "1"},
                ),
            ],
            style={"display": "flex", "marginBottom": "24px"},
        ),
        # Charts
        html.Div(id="comparison-charts"),
    ]
)


@callback(
    Output("comparison-institutions", "options"),
    Input("institution-options-store", "data"),
)
def update_institution_dropdown(
    options: list[dict[str, str | int]] | None,
) -> list[dict[str, str | int]]:
    """Populate institution dropdown from shared store."""
    return options or []


@callback(
    Output("comparison-indicator", "options"),
    Output("comparison-indicator", "value"),
    Input("comparison-report", "value"),
)
def update_indicator_options(
    relatorio: str,
) -> tuple[list[dict[str, str]], str | None]:
    """Update indicator dropdown when report changes."""
    indicators = get_available_indicators(con, relatorio)
    options = [{"label": i, "value": i} for i in indicators]
    # Auto-select first indicator
    value = indicators[0] if indicators else None
    return options, value


@callback(
    Output("comparison-charts", "children"),
    Input("comparison-institutions", "value"),
    Input("comparison-indicator", "value"),
    Input("comparison-report", "value"),
)
def render_comparison(
    cod_conglomerados: list[int] | None,
    indicator_name: str | None,
    relatorio: str,
) -> list[object] | str:
    """Render comparison charts."""
    if not cod_conglomerados or len(cod_conglomerados) < 2:
        return html.P(
            "Selecione pelo menos 2 instituições para comparar.",
            style={"color": "#888", "fontStyle": "italic"},
        )
    if not indicator_name:
        return html.P(
            "Selecione um indicador.",
            style={"color": "#888", "fontStyle": "italic"},
        )

    charts: list[object] = []

    # --- Multi-line comparison chart ---
    comp_df = compare_institutions(con, cod_conglomerados, indicator_name, relatorio)
    if not comp_df.is_empty():
        comp_pandas = comp_df.to_pandas()
        comp_pandas["ano_mes"] = comp_pandas["ano_mes"].astype(int)
        comp_pandas = comp_pandas.sort_values("ano_mes")

        meses = [
            "Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
            "Jul", "Ago", "Set", "Out", "Nov", "Dez",
        ]
        comp_pandas["periodo"] = comp_pandas["ano_mes"].apply(
            lambda v: f"{meses[(v % 100) - 1]}/{str(v // 100)[-2:]}"
        )
        periodos_ordenados = comp_pandas["periodo"].unique().tolist()

        fig_bars = px.bar(
            comp_pandas,
            x="periodo",
            y="valor_a",
            color="nome_conglomerado",
            title=f"{indicator_name} — Evolução Comparada",
            labels={
                "periodo": "Período",
                "valor_a": "Valor",
                "nome_conglomerado": "Instituição",
            },
            category_orders={"periodo": periodos_ordenados},
        )
        fig_bars.update_layout(height=450, barmode="group")
        charts.append(dcc.Graph(figure=fig_bars))

        # --- Bar chart: latest period ranking ---
        latest_period = comp_pandas["ano_mes"].max()
        latest = comp_pandas[comp_pandas["ano_mes"] == latest_period].sort_values(
            "valor_a", ascending=True
        )
        if not latest.empty:
            fig_bar = px.bar(
                latest,
                x="valor_a",
                y="nome_conglomerado",
                orientation="h",
                title=f"{indicator_name} — Ranking ({latest_period})",
                labels={
                    "valor_a": "Valor",
                    "nome_conglomerado": "Instituição",
                },
                color="valor_a",
                color_continuous_scale="Blues",
            )
            fig_bar.update_layout(height=max(300, len(latest) * 50))
            charts.append(dcc.Graph(figure=fig_bar))

    # --- Segment ranking (all institutions) ---
    ranking_df = get_segment_ranking(con, indicator_name, relatorio)
    if not ranking_df.is_empty():
        ranking_pandas = ranking_df.head(20).to_pandas()
        fig_ranking = px.bar(
            ranking_pandas,
            x="valor_a",
            y="nome_conglomerado",
            orientation="h",
            title=f"Top 20 — {indicator_name} (todas as instituições)",
            labels={
                "valor_a": "Valor",
                "nome_conglomerado": "Instituição",
            },
            color="segmento",
        )
        fig_ranking.update_layout(
            height=max(400, len(ranking_pandas) * 30),
            yaxis={"categoryorder": "total ascending"},
        )
        charts.append(dcc.Graph(figure=fig_ranking))

    if not charts:
        return html.P(
            "Nenhum dado encontrado. Execute 'python -m scripts.refresh' para carregar dados.",
            style={"color": "#c00"},
        )

    return charts
