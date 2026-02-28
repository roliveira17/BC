from __future__ import annotations

import dash
from dash import Dash, Input, Output, dcc, html

from src.db import get_connection
from src.log import configure_logging
from src.queries import list_institutions
from src.settings import Settings

configure_logging()
settings = Settings()

con = get_connection(settings.duckdb_path)

app = Dash(
    __name__,
    use_pages=True,
    suppress_callback_exceptions=True,
    title="BCB Prudential Dashboard",
)

SEGMENTS = [
    {"label": "Todos", "value": "ALL"},
    {"label": "S1 - Porte >= 10%", "value": "S1"},
    {"label": "S2 - Porte >= 1%", "value": "S2"},
    {"label": "S3 - Porte >= 0.1%", "value": "S3"},
    {"label": "S4 - Porte < 0.1%", "value": "S4"},
    {"label": "S5 - Não bancário", "value": "S5"},
]

PEER_GROUPS = [
    {"label": "Todos", "value": "ALL"},
    {"label": "Cora Peers (23)", "value": "CORA_PEERS"},
]

CORA_PEERS_CODES: list[int] = [
    1000085317,  # CORA SCFI
    1000084693,  # NU PAGAMENTOS
    1000080996,  # INTER
    1000084844,  # BANCO C6
    1000084686,  # STONE IP
    1000084813,  # PAGSEGURO
    1000084820,  # MERCADO PAGO IP
    1000087384,  # CONTA SIMPLES SCD
    1000085771,  # ASAAS IP
    1000087157,  # OMIE IP
    1000080336,  # BTG PACTUAL
    1000083694,  # AGIBANK
    1000080271,  # SOFISA
    1000080422,  # BS2
    1000081744,  # DAYCOVAL
    1000080178,  # BMG
    1000080123,  # MERCANTIL DO BRASIL
    1000080312,  # ABC-BRASIL
    1000080099,  # ITAU
    1000080075,  # BRADESCO
    1000080185,  # SANTANDER
    1000080329,  # BB
    1000084909,  # CREDITAS SCD
]

_PEER_GROUP_MAP: dict[str, list[int]] = {
    "CORA_PEERS": CORA_PEERS_CODES,
}

app.layout = html.Div(
    [
        # Header
        html.Div(
            [
                html.H1(
                    "Dashboard Prudencial BCB",
                    style={"margin": "0", "fontSize": "24px"},
                ),
                html.P(
                    "Indicadores de instituições financeiras — IF.data",
                    style={"margin": "0", "color": "#666", "fontSize": "14px"},
                ),
            ],
            style={"padding": "16px 24px", "borderBottom": "1px solid #ddd"},
        ),
        # Navigation + segment filter
        html.Div(
            [
                html.Nav(
                    [
                        dcc.Link(
                            "Análise Individual",
                            href="/",
                            style={
                                "marginRight": "24px",
                                "fontWeight": "bold",
                                "textDecoration": "none",
                            },
                        ),
                        dcc.Link(
                            "Comparação",
                            href="/comparison",
                            style={
                                "marginRight": "24px",
                                "fontWeight": "bold",
                                "textDecoration": "none",
                            },
                        ),
                        dcc.Link(
                            "Balancetes 4040",
                            href="/balancetes",
                            style={
                                "marginRight": "24px",
                                "fontWeight": "bold",
                                "textDecoration": "none",
                            },
                        ),
                        dcc.Link(
                            "Conglomerados",
                            href="/conglomerados",
                            style={
                                "marginRight": "24px",
                                "fontWeight": "bold",
                                "textDecoration": "none",
                            },
                        ),
                        dcc.Link(
                            "Indicadores",
                            href="/indicadores",
                            style={
                                "fontWeight": "bold",
                                "textDecoration": "none",
                            },
                        ),
                    ],
                    style={"display": "inline-block", "verticalAlign": "middle"},
                ),
                html.Div(
                    [
                        html.Label(
                            "Segmento: ",
                            style={"fontWeight": "bold", "marginRight": "8px"},
                        ),
                        dcc.Dropdown(
                            id="segment-filter",
                            options=SEGMENTS,
                            value="ALL",
                            clearable=False,
                            style={"width": "200px", "display": "inline-block"},
                        ),
                    ],
                    style={
                        "display": "inline-block",
                        "marginLeft": "48px",
                        "verticalAlign": "middle",
                    },
                ),
                html.Div(
                    [
                        html.Label(
                            "Grupo: ",
                            style={"fontWeight": "bold", "marginRight": "8px"},
                        ),
                        dcc.Dropdown(
                            id="peer-group-filter",
                            options=PEER_GROUPS,
                            value="ALL",
                            clearable=False,
                            style={"width": "200px", "display": "inline-block"},
                        ),
                    ],
                    style={
                        "display": "inline-block",
                        "marginLeft": "24px",
                        "verticalAlign": "middle",
                    },
                ),
            ],
            style={"padding": "12px 24px", "borderBottom": "1px solid #eee"},
        ),
        # Shared store for institution options
        dcc.Store(id="institution-options-store"),
        # Page content
        html.Div(
            dash.page_container,
            style={"padding": "24px"},
        ),
    ],
    style={"fontFamily": "Arial, sans-serif"},
)


@app.callback(
    Output("institution-options-store", "data"),
    Input("segment-filter", "value"),
    Input("peer-group-filter", "value"),
)
def update_institution_options(
    segmento: str,
    peer_group: str,
) -> list[dict[str, str | int]]:
    """Update institution options whenever segment or peer group changes."""
    codes = _PEER_GROUP_MAP.get(peer_group) if peer_group != "ALL" else None
    df = list_institutions(
        con,
        segmento=segmento if segmento != "ALL" else None,
        cod_conglomerados=codes,
    )
    if df.is_empty():
        return []
    return [
        {"label": f"{row[1]} (C{row[0]:07d})", "value": row[0]}
        for row in df.iter_rows()
    ]


if __name__ == "__main__":
    app.run(
        debug=settings.dash_debug,
        host=settings.dash_host,
        port=settings.dash_port,
    )
