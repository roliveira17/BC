from __future__ import annotations

import dash
import plotly.express as px
from dash import Input, Output, callback, dash_table, dcc, html

from src.db import get_connection
from src.queries import (
    get_capital_indicators,
    get_dre_indicators,
    get_summary_indicators,
)
from src.settings import Settings

dash.register_page(__name__, path="/", name="Análise Individual")

settings = Settings()
con = get_connection(settings.duckdb_path)

layout = html.Div(
    [
        html.H2("Análise Individual", style={"marginBottom": "16px"}),
        # Institution selector
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
        # Charts container
        html.Div(id="individual-charts"),
    ]
)


@callback(
    Output("individual-institution", "options"),
    Input("institution-options-store", "data"),
)
def update_dropdown(options: list[dict[str, str | int]] | None) -> list[dict[str, str | int]]:
    """Populate institution dropdown from shared store."""
    return options or []


@callback(
    Output("individual-charts", "children"),
    Input("individual-institution", "value"),
)
def render_charts(cod_conglomerado: int | None) -> list[object] | str:
    """Render all charts for the selected institution."""
    if cod_conglomerado is None:
        return html.P(
            "Selecione uma instituição para visualizar os indicadores.",
            style={"color": "#888", "fontStyle": "italic"},
        )

    charts: list[object] = []

    # --- Capital indicators (Report 5) ---
    capital_df = get_capital_indicators(con, cod_conglomerado)
    if not capital_df.is_empty():
        # Basel Index trend
        basileia = capital_df.filter(
            capital_df["nome_linha"].str.contains("(?i)basileia")
        )
        if not basileia.is_empty():
            fig_basileia = px.line(
                basileia.to_pandas(),
                x="ano_mes",
                y="valor_a",
                title="Índice de Basileia",
                labels={"ano_mes": "Período", "valor_a": "%"},
            )
            fig_basileia.add_hline(
                y=10.5,
                line_dash="dash",
                line_color="red",
                annotation_text="Mínimo regulatório (10,5%)",
            )
            fig_basileia.update_layout(
                xaxis_title="Período (AAAAMM)",
                yaxis_title="Índice (%)",
                height=400,
            )
            charts.append(dcc.Graph(figure=fig_basileia))

        capital_pivot = capital_df.to_pandas()

        # Multi-line chart for key capital indicators
        fig_capital = px.line(
            capital_pivot,
            x="ano_mes",
            y="valor_a",
            color="nome_linha",
            title="Indicadores de Capital (Relatório 5)",
            labels={
                "ano_mes": "Período",
                "valor_a": "Valor",
                "nome_linha": "Indicador",
            },
        )
        fig_capital.update_layout(height=450)
        charts.append(dcc.Graph(figure=fig_capital))

    # --- Summary indicators (Report 1) ---
    summary_df = get_summary_indicators(con, cod_conglomerado)
    if not summary_df.is_empty():
        fig_summary = px.line(
            summary_df.to_pandas(),
            x="ano_mes",
            y="valor_a",
            color="nome_linha",
            title="Resumo (Relatório 1)",
            labels={
                "ano_mes": "Período",
                "valor_a": "R$ mil",
                "nome_linha": "Indicador",
            },
        )
        fig_summary.update_layout(height=450)
        charts.append(dcc.Graph(figure=fig_summary))

    # --- DRE indicators (Report 4) ---
    dre_df = get_dre_indicators(con, cod_conglomerado)
    if not dre_df.is_empty():
        fig_dre = px.line(
            dre_df.to_pandas(),
            x="ano_mes",
            y="valor_a",
            color="nome_linha",
            title="DRE - Demonstração de Resultado (Relatório 4)",
            labels={
                "ano_mes": "Período",
                "valor_a": "R$ mil",
                "nome_linha": "Indicador",
            },
        )
        fig_dre.update_layout(height=450)
        charts.append(dcc.Graph(figure=fig_dre))

    # --- Raw data table ---
    all_data = get_capital_indicators(con, cod_conglomerado)
    if not all_data.is_empty():
        table_df = all_data.to_pandas()
        charts.append(
            html.Div(
                [
                    html.H3("Dados Brutos — Capital", style={"marginTop": "24px"}),
                    dash_table.DataTable(
                        data=table_df.to_dict("records"),
                        columns=[{"name": c, "id": c} for c in table_df.columns],
                        page_size=20,
                        style_table={"overflowX": "auto"},
                        style_cell={"textAlign": "left", "padding": "8px"},
                        style_header={"fontWeight": "bold"},
                        sort_action="native",
                        filter_action="native",
                    ),
                ]
            )
        )

    if not charts:
        return html.P(
            "Nenhum dado encontrado para esta instituição. "
            "Execute 'python -m scripts.refresh' para carregar dados.",
            style={"color": "#c00"},
        )

    return charts
