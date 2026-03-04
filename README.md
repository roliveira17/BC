# BC Dashboard

Dashboard prudencial de análise de bancos brasileiros usando dados públicos do BCB (Banco Central do Brasil).

## Quick Start

```bash
git clone https://github.com/<your-user>/BC.git
cd BC
pip install -e .
python -m scripts.seed
python app.py
```

O comando `scripts.seed` baixa todos os dados das APIs públicas do BCB e popula o DuckDB local. Na primeira execução leva ~20-30 minutos.

### Opções do seed

```bash
python -m scripts.seed --skip-4010          # Pula balancetes individuais (~5-10 min)
python -m scripts.seed --quarters 8 --months 12   # Menos histórico (mais rápido)
python -m scripts.seed --force              # Re-baixa tudo, mesmo se já cacheado
```

### Scripts individuais

Cada fonte de dados pode ser atualizada separadamente:

```bash
python -m scripts.refresh                   # IF.data (cadastro + reports)
python -m scripts.refresh_balancetes        # Balancetes 4040
python -m scripts.refresh_4010 --all        # Balancetes 4010 (individual)
```

## Stack

- **Python 3.11+** — Dash, Plotly, Polars, DuckDB
- **Dados** — IF.data REST API (reports 1, 4, 5) + Balancetes COSIF 4010/4040
- **Banco local** — DuckDB (arquivo em `data/bc_dashboard.duckdb`, não commitado)
