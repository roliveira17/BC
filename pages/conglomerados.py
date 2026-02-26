from __future__ import annotations

import dash
import plotly.express as px
from dash import Input, Output, callback, dash_table, dcc, html

from src.db import get_connection
from src.queries import (
    compare_institutions,
    compare_pl_trend,
    get_balancetes_trend,
    get_capital_indicators,
    get_summary_indicators,
    get_top50_enriched,
    list_balancetes_periods,
)
from src.settings import Settings

dash.register_page(__name__, path="/conglomerados", name="Conglomerados")

settings = Settings()
con = get_connection(settings.duckdb_path)

layout = html.Div(
    [
        # --- Section 1: Enriched Top 50 Table ---
        html.H2(
            "Conglomerados — Visão Unificada",
            style={"marginBottom": "16px"},
        ),
        html.Div(
            [
                html.Label(
                    "Período:", style={"fontWeight": "bold", "marginRight": "8px"}
                ),
                dcc.Dropdown(
                    id="cong-period",
                    placeholder="Selecione um período...",
                    style={"width": "200px", "display": "inline-block"},
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        html.Div(id="cong-table"),
        # Mapping store: cod_conglomerado → cnpj8
        dcc.Store(id="cong-mapping-store"),
        # --- Section 2: Conglomerate Detail ---
        html.Hr(style={"marginTop": "32px"}),
        html.H3("Detalhe do Conglomerado", style={"marginBottom": "16px"}),
        html.Div(
            [
                html.Label(
                    "Conglomerado:",
                    style={"fontWeight": "bold", "marginRight": "8px"},
                ),
                dcc.Dropdown(
                    id="cong-detail-select",
                    placeholder="Selecione um conglomerado...",
                    style={"width": "400px", "display": "inline-block"},
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        html.Div(id="cong-detail-charts"),
        # --- Section 3: Multi-KPI Comparison ---
        html.Hr(style={"marginTop": "32px"}),
        html.H3("Comparação Multi-KPI", style={"marginBottom": "16px"}),
        html.Div(
            [
                html.Label(
                    "Conglomerados (2-5):",
                    style={"fontWeight": "bold", "marginRight": "8px"},
                ),
                dcc.Dropdown(
                    id="cong-compare-select",
                    multi=True,
                    placeholder="Selecione 2 a 5 conglomerados...",
                    style={"width": "600px", "display": "inline-block"},
                ),
            ],
            style={"marginBottom": "24px"},
        ),
        html.Div(id="cong-compare-charts"),
    ]
)


# --- Callback 1: Load periods ---


@callback(
    Output("cong-period", "options"),
    Output("cong-period", "value"),
    Input("cong-period", "id"),
)
def load_periods(_: str) -> tuple[list[dict[str, str | int]], int | None]:
    periods = list_balancetes_periods(con)
    options = [{"label": str(p), "value": p} for p in periods]
    value = periods[0] if periods else None
    return options, value


# --- Callback 2: Render summary table + populate dropdowns + store ---


@callback(
    Output("cong-table", "children"),
    Output("cong-detail-select", "options"),
    Output("cong-compare-select", "options"),
    Output("cong-mapping-store", "data"),
    Input("cong-period", "value"),
)
def render_summary_table(
    ano_mes: int | None,
) -> tuple[object, list[dict[str, str | int]], list[dict[str, str | int]], dict[str, str]]:
    empty_mapping: dict[str, str] = {}
    if ano_mes is None:
        msg = html.P("Nenhum dado disponível.", style={"color": "#888"})
        return msg, [], [], empty_mapping

    df = get_top50_enriched(con, ano_mes)
    if df.is_empty():
        msg = html.P(
            "Nenhum dado para o período selecionado.", style={"color": "#888"}
        )
        return msg, [], [], empty_mapping

    pdf = df.to_pandas()

    # Format numeric columns
    for col in ["patrimonio_liquido", "ativo_total", "lucro_liquido"]:
        if col in pdf.columns:
            pdf[col] = pdf[col].round(2)
    if "basileia" in pdf.columns:
        pdf["basileia"] = pdf["basileia"].round(2)

    table = dash_table.DataTable(
        data=pdf.to_dict("records"),
        columns=[
            {"name": "#", "id": "rank"},
            {"name": "Instituição", "id": "nome_inst"},
            {"name": "CNPJ8", "id": "cnpj8"},
            {"name": "Conglomerado", "id": "nome_conglomerado"},
            {"name": "PL (R$ mil)", "id": "patrimonio_liquido", "type": "numeric"},
            {"name": "Basileia (%)", "id": "basileia", "type": "numeric"},
            {"name": "Ativo Total", "id": "ativo_total", "type": "numeric"},
            {"name": "Lucro Líquido", "id": "lucro_liquido", "type": "numeric"},
        ],
        page_size=50,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "8px"},
        style_header={"fontWeight": "bold"},
        sort_action="native",
        filter_action="native",
    )

    # Build dropdown options (only institutions with cod_conglomerado)
    with_bridge = pdf.dropna(subset=["cod_conglomerado"])
    detail_options = [
        {
            "label": f"{row['rank']}. {row['nome_inst']} ({row['nome_conglomerado']})",
            "value": str(int(row["cod_conglomerado"])),
        }
        for _, row in with_bridge.iterrows()
    ]

    # Mapping: cod_conglomerado (str) → cnpj8
    mapping = {
        str(int(row["cod_conglomerado"])): row["cnpj8"]
        for _, row in with_bridge.iterrows()
    }

    return table, detail_options, detail_options, mapping


# --- Callback 3: Render conglomerate detail charts ---


@callback(
    Output("cong-detail-charts", "children"),
    Input("cong-detail-select", "value"),
    Input("cong-mapping-store", "data"),
)
def render_detail(
    cod_cong_str: str | None,
    mapping: dict[str, str] | None,
) -> object:
    if not cod_cong_str or not mapping:
        return html.P(
            "Selecione um conglomerado para ver os detalhes.",
            style={"color": "#888", "fontStyle": "italic"},
        )

    cod_cong = int(cod_cong_str)
    cnpj8 = mapping.get(cod_cong_str)
    charts: list[object] = []

    # 1. PL mensal (balancetes trend)
    if cnpj8:
        pl_df = get_balancetes_trend(con, cnpj8)
        if not pl_df.is_empty():
            fig_pl = px.line(
                pl_df.to_pandas(),
                x="ano_mes",
                y="patrimonio_liquido",
                title="Patrimônio Líquido — Evolução Mensal",
                labels={"ano_mes": "Período", "patrimonio_liquido": "PL (R$ mil)"},
                markers=True,
            )
            fig_pl.update_layout(height=400)
            charts.append(dcc.Graph(figure=fig_pl))

    # 2. Basileia trimestral (report 5)
    capital_df = get_capital_indicators(con, cod_cong)
    if not capital_df.is_empty():
        basileia = capital_df.filter(
            capital_df["nome_linha"].str.contains("(?i)basileia")
        )
        if not basileia.is_empty():
            fig_bas = px.line(
                basileia.to_pandas(),
                x="ano_mes",
                y="valor_a",
                title="Índice de Basileia — Trimestral",
                labels={"ano_mes": "Período", "valor_a": "%"},
                markers=True,
            )
            fig_bas.add_hline(
                y=10.5,
                line_dash="dash",
                line_color="red",
                annotation_text="Mínimo regulatório (10,5%)",
            )
            fig_bas.update_layout(height=400)
            charts.append(dcc.Graph(figure=fig_bas))

    # 3. Ativo Total trimestral (report 1)
    summary_df = get_summary_indicators(con, cod_cong)
    if not summary_df.is_empty():
        ativo = summary_df.filter(
            summary_df["nome_linha"].str.contains("(?i)ativo total")
        )
        if not ativo.is_empty():
            fig_ativo = px.line(
                ativo.to_pandas(),
                x="ano_mes",
                y="valor_a",
                title="Ativo Total — Trimestral",
                labels={"ano_mes": "Período", "valor_a": "R$ mil"},
                markers=True,
            )
            fig_ativo.update_layout(height=400)
            charts.append(dcc.Graph(figure=fig_ativo))

    if not charts:
        return html.P(
            "Nenhum dado encontrado para este conglomerado.",
            style={"color": "#888"},
        )

    return html.Div(charts)


# --- Callback 4: Render comparison charts ---


@callback(
    Output("cong-compare-charts", "children"),
    Input("cong-compare-select", "value"),
    Input("cong-mapping-store", "data"),
)
def render_comparison(
    cod_cong_list: list[str] | None,
    mapping: dict[str, str] | None,
) -> object:
    if not cod_cong_list or len(cod_cong_list) < 2:
        return html.P(
            "Selecione pelo menos 2 conglomerados para comparar.",
            style={"color": "#888", "fontStyle": "italic"},
        )

    charts: list[object] = []

    # 1. PL trend (monthly, via compare_pl_trend)
    cnpj8_list = [
        mapping[c] for c in cod_cong_list if mapping and c in mapping
    ]
    if cnpj8_list:
        pl_df = compare_pl_trend(con, cnpj8_list)
        if not pl_df.is_empty():
            fig_pl = px.line(
                pl_df.to_pandas(),
                x="ano_mes",
                y="patrimonio_liquido",
                color="nome_inst",
                title="Patrimônio Líquido — Comparação Mensal",
                labels={
                    "ano_mes": "Período",
                    "patrimonio_liquido": "PL (R$ mil)",
                    "nome_inst": "Instituição",
                },
                markers=True,
            )
            fig_pl.update_layout(height=450)
            charts.append(dcc.Graph(figure=fig_pl))

    # 2. Basileia trend (quarterly, via compare_institutions)
    int_cods = [int(c) for c in cod_cong_list]
    basileia_df = compare_institutions(
        con, int_cods, "Indice de Basileia", "5"
    )
    if not basileia_df.is_empty():
        fig_bas = px.line(
            basileia_df.to_pandas(),
            x="ano_mes",
            y="valor_a",
            color="nome_conglomerado",
            title="Índice de Basileia — Comparação Trimestral",
            labels={
                "ano_mes": "Período",
                "valor_a": "%",
                "nome_conglomerado": "Conglomerado",
            },
            markers=True,
        )
        fig_bas.add_hline(
            y=10.5,
            line_dash="dash",
            line_color="red",
            annotation_text="Mínimo regulatório (10,5%)",
        )
        fig_bas.update_layout(height=450)
        charts.append(dcc.Graph(figure=fig_bas))

    if not charts:
        return html.P(
            "Nenhum dado de comparação encontrado.",
            style={"color": "#888"},
        )

    return html.Div(charts)
