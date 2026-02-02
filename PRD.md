# PRD — Dashboard Bacen: Conglomerados (4040) + Top 50 PL + IF.data

## 1. Objetivo

Dashboard operacional para análise dos **Top 50 conglomerados financeiros por Patrimônio Líquido** no Brasil, com dados do Banco Central (BCB). Combina duas fontes:

- **Balancetes 4040** (Consolidado Financeiro): dados contábeis mensais por conglomerado
- **IF.data** (Olinda/OData): indicadores trimestrais agregados (Resumo, DRE, Capital, Crédito)

---

## 2. Definição de Pronto

- [ ] Baixar ZIPs de balancetes do BCB (últimos 24 meses) automaticamente
- [ ] Extrair e transformar apenas registros Documento=4040 em Parquet particionado
- [ ] Calcular Top 50 conglomerados por Patrimônio Líquido (conta 6.0.0.00.00-2) por data-base
- [ ] Mapear CNPJ (4040) → CodConglomerado (IF.data) via IfDataCadastro
- [ ] Baixar indicadores IF.data trimestrais para os Top 50 (Resumo, DRE, Capital)
- [ ] Dashboard Streamlit interativo com: ranking, séries temporais, explorador de contas, indicadores IF.data
- [ ] Execução manual via scripts numerados (automação posterior)

---

## 3. Fontes de Dados

### 3.1 Balancetes — Transferência CSV (BCB)

**O que é:** Arquivo ZIP mensal contendo CSV com balancetes de todas as IFs. Inclui documentos 4010 (individual) e 4040 (consolidado conglomerado), contas COSIF até nível 3.

**URL (padrão documentado no Portal de Dados Abertos do BCB):**
```
https://www4.bcb.gov.br/fis/cosif/cont/balan/bancos/{AAAAMM}/BANCOS.ZIP
```

**Variante sem barra (documentada como recurso alternativo):**
```
https://www4.bcb.gov.br/fis/cosif/cont/balan/bancos/{AAAAMM}BANCOS.ZIP
```

> **Nota:** O domínio `www4.bcb.gov.br` pode estar em processo de migração. O download
> deve implementar fallback entre as duas variantes. Se ambas falharem, logar claramente
> para investigação manual.

**Colunas do CSV:**

| # | Coluna              | Tipo   | Descrição                                    |
|---|---------------------|--------|----------------------------------------------|
| 1 | Data                | str    | Data-base no formato AAAAMM                  |
| 2 | CNPJ                | str    | CNPJ da instituição (líder no caso do 4040)  |
| 3 | Nome da Instituição | str    | Nome conforme UNICAD                         |
| 4 | Atributo            | str    | Código tipo instituição (UNICAD)             |
| 5 | Documento           | str    | Código CADOC: "4010" ou "4040"               |
| 6 | Conta               | str    | Conta COSIF (10 dígitos, até nível 3)        |
| 7 | Nome da Conta       | str    | Descrição da conta no COSIF                  |
| 8 | Saldo               | str    | Saldo com separador decimal vírgula          |

**Formato:** separador `;`, encoding `latin-1`, decimal `,`.

**Periodicidade:** Mensal. Disponível 60 dias após a data-base (90 dias para dezembro).

**Filtro aplicado:** Somente `Documento = "4040"` (Consolidado Financeiro do Conglomerado).

**Identificação do conglomerado:** O campo CNPJ contém o CNPJ da **instituição líder** do conglomerado. Usamos os 8 primeiros dígitos (CNPJ raiz) como chave de agrupamento (`CNPJ8`).

### 3.2 IF.data — Indicadores Trimestrais (Olinda/OData)

**Base URL:**
```
https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/
```

**Autenticação:** Nenhuma (API pública).

**Endpoints:**

| Endpoint           | Parâmetros Obrigatórios                       | Descrição                              |
|--------------------|-----------------------------------------------|----------------------------------------|
| `ListaDeRelatorio` | Nenhum                                        | Catálogo de relatórios e colunas       |
| `IfDataCadastro`   | `AnoMes` (int, AAAAMM)                        | Cadastro de IFs e conglomerados        |
| `IfDataValores`    | `AnoMes`, `TipoInstituicao`, `Relatorio`      | Valores dos indicadores                |

**Campos retornados por IfDataCadastro:**

| Campo              | Descrição                                      |
|--------------------|-------------------------------------------------|
| CodConglomerado    | Código único do conglomerado (chave IF.data)    |
| NomeConglomerado   | Nome do conglomerado                            |
| CodInst            | Código da instituição individual                |
| NomeInst           | Nome da instituição individual                  |
| CNPJ               | CNPJ (14 dígitos) da instituição                |
| Segmento           | Segmento regulatório BCB                        |
| TipoInstituicao    | 1=Congl. Financeiro, 2=Congl. Prudencial, 3=Individual |
| Cidade             | Cidade sede                                     |
| UF                 | Estado                                          |

**Campos retornados por IfDataValores:**

| Campo              | Descrição                                      |
|--------------------|-------------------------------------------------|
| CodConglomerado    | Código do conglomerado                          |
| NomeConglomerado   | Nome do conglomerado                            |
| CodigoColuna       | Código da coluna no relatório                   |
| NomeColuna         | Nome da coluna (descrição legível)              |
| ValorA             | Valor do indicador                              |
| NomeLinha          | Nome da linha no relatório                      |
| Ordenacao          | Ordem de exibição                               |

**TipoInstituicao para conglomerados financeiros:** `1`

**Relatórios disponíveis (TipoInstituicao=1):**

| Código | Nome                       | Conteúdo                                   |
|--------|----------------------------|---------------------------------------------|
| `'1'`  | Resumo                     | Ativo Total, PL, Lucro, Captações, Crédito  |
| `'2'`  | Ativo                      | Detalhamento de ativos                       |
| `'3'`  | Passivo                    | Detalhamento de passivos                     |
| `'4'`  | Demonstração de Resultado  | Receitas, despesas, resultado                |
| `'5'`  | Informações de Capital     | Índice de Basileia, PR, RWA                  |

> **Nota:** Os códigos de relatório (`'1'`, `'2'`, etc.) devem ser confirmados via
> `ListaDeRelatorio` na primeira execução. O script armazena o mapeamento localmente.

**Periodicidade:** Trimestral (março, junho, setembro, dezembro).

### 3.3 Ponte entre Fontes: CNPJ → CodConglomerado

O CSV dos balancetes identifica conglomerados por **CNPJ do líder** (8 dígitos).
O IF.data identifica por **CodConglomerado**.

**Solução:** Usar `IfDataCadastro` para construir tabela de-para:

```
IfDataCadastro(AnoMes=AAAAMM) → {CNPJ[:8]: CodConglomerado}
```

Essa tabela é persistida em `data/curated/cadastro/cadastro_map.parquet` e atualizada
junto com os indicadores.

---

## 4. Stack

| Componente         | Tecnologia       | Justificativa                                 |
|--------------------|------------------|-----------------------------------------------|
| ETL                | Python + Pandas  | Flexível, ecossistema rico para dados tabulares|
| Armazenamento      | Parquet (zstd)   | Compacto, tipado, particionável               |
| Consulta no dash   | DuckDB           | Leitura Parquet nativa, SQL analítico rápido   |
| Dashboard          | Streamlit        | Rápido para MVPs, sem frontend separado        |
| Gráficos           | Plotly           | Interativo, boa integração com Streamlit       |
| Retry HTTP         | tenacity         | Backoff exponencial para APIs instáveis        |
| Config             | TOML             | Nativo do Python 3.11+                         |

**Python mínimo:** 3.11 (por causa de `tomllib` nativo).

---

## 5. Estrutura de Pastas

```
.
├── app.py                              # Dashboard Streamlit
├── requirements.txt
├── config/
│   ├── settings.toml                   # URLs, paths, parâmetros
│   └── kpi_map.json                    # Mapeamento conta COSIF → KPI
├── data/
│   ├── raw/                            # ZIPs baixados (bancos/<AAAAMM>/BANCOS.ZIP)
│   ├── staging/                        # CSVs extraídos
│   └── curated/
│       ├── bal_4040/                   # Parquet particionado por Data
│       ├── top50/                      # Ranking Top 50 por PL
│       ├── cadastro/                   # Mapeamento CNPJ ↔ CodConglomerado
│       └── ifdata/                     # Indicadores trimestrais
├── scripts/
│   ├── 01_download_balancetes.py       # Baixa ZIPs do BCB
│   ├── 02_build_bal_4040_parquet.py    # Extrai CSV → filtra 4040 → Parquet
│   ├── 03_build_top50_pl.py           # Calcula Top 50 por PL
│   └── 04_update_ifdata_indicators.py  # Baixa cadastro + indicadores IF.data
├── src/
│   ├── __init__.py
│   ├── bcb_balancetes.py              # Download e helpers de data
│   ├── transform_balancetes.py        # Leitura CSV, limpeza, filtro 4040
│   ├── kpis.py                        # Cálculo de KPIs via conta COSIF
│   ├── ifdata_client.py               # Cliente OData para IF.data
│   └── store.py                       # Escrita Parquet particionado
├── tests/
│   ├── test_transform.py              # Testes de parsing e limpeza
│   ├── test_kpis.py                   # Testes de cálculo de KPIs
│   └── test_ifdata_client.py          # Testes do cliente OData
└── .gitignore
```

---

## 6. Fluxo de Execução (Manual)

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1) Baixa ZIPs dos últimos 24 meses
python scripts/01_download_balancetes.py --months 24

# 2) Extrai CSVs, filtra Documento=4040, grava Parquet particionado por Data
python scripts/02_build_bal_4040_parquet.py

# 3) Calcula Top 50 por PL (conta 6.0.0.00.00-2) por data-base
python scripts/03_build_top50_pl.py

# 4) Baixa cadastro IF.data (CNPJ→CodConglomerado) + indicadores para Top 50
python scripts/04_update_ifdata_indicators.py --quarters 8

# 5) Sobe dashboard
streamlit run app.py
```

---

## 7. Detalhamento dos Componentes

### 7.1 Script 01 — Download de Balancetes

**Entrada:** `--months N` (padrão 24)

**Lógica:**
1. Gera lista de AAAAMM dos últimos N meses
2. Para cada mês, tenta download com URL padrão (com barra)
3. Se falhar (HTTP != 200), tenta variante sem barra
4. Implementa retry com backoff (tenacity, 3 tentativas)
5. Pula meses cujo ZIP já existe em `data/raw/` (incremental)
6. Loga claramente: sucesso, falha (mês pode não existir), skip (já baixado)

**Saída:** `data/raw/bancos/<AAAAMM>/BANCOS.ZIP`

### 7.2 Script 02 — Build Parquet 4040

**Entrada:** Todos os ZIPs em `data/raw/bancos/`

**Lógica:**
1. Lista ZIPs disponíveis
2. Verifica quais já foram processados (checando partições existentes em `data/curated/bal_4040/Data=AAAAMM/`)
3. Para ZIPs novos:
   a. Extrai primeiro CSV do ZIP para `data/staging/`
   b. Lê com `sep=";"`, `encoding="latin-1"`, todas as colunas como `str`
   c. Valida colunas esperadas
   d. Filtra `Documento == "4040"`
   e. Limpa Saldo (remove `.` milhares, troca `,` por `.`, converte para float)
   f. Extrai CNPJ8 (primeiros 8 dígitos)
4. Grava Parquet particionado por `Data` com compressão zstd

**Saída:** `data/curated/bal_4040/Data=AAAAMM/*.parquet`

### 7.3 Script 03 — Top 50 por PL

**Entrada:** Parquets em `data/curated/bal_4040/`

**Lógica:**
1. Lê Parquets via DuckDB (sem carregar tudo em memória)
2. Filtra pela conta PL usando `kpi_map.json` (conta exata `6.0.0.00.00-2`, fallback regex)
3. Para cada data-base, agrupa por CNPJ8, soma Saldo, rankeia
4. Marca Top N (padrão 50)

**Query DuckDB (evita memory blow-up):**
```sql
SELECT Data, CNPJ8, "Nome da Instituição" AS Nome, SUM(Saldo) AS valor
FROM read_parquet('data/curated/bal_4040/**/*.parquet')
WHERE Conta = '6.0.0.00.00-2'
GROUP BY Data, CNPJ8, "Nome da Instituição"
```

Depois rankeia em Python (ou SQL com `ROW_NUMBER() OVER`).

**Saída:** `data/curated/top50/top50_pl.parquet` e `.csv`

### 7.4 Script 04 — IF.data (Cadastro + Indicadores)

**Entrada:** `--quarters N` (padrão 8), `top50_pl.parquet`

**Lógica:**
1. Lê Top 50 da última data-base → lista de CNPJ8
2. Busca `ListaDeRelatorio` → armazena mapeamento código↔nome (log)
3. Gera lista de trimestres (últimos N): `[202312, 202403, 202406, ...]`
4. Para cada trimestre:
   a. Busca `IfDataCadastro(AnoMes=AAAAMM)` → constrói mapa `CNPJ[:8] → CodConglomerado`
   b. Filtra apenas CNPJ8 do Top 50 → lista de CodConglomerado
   c. Para cada relatório prioritário (`'1'` Resumo, `'4'` DRE, `'5'` Capital):
      - Busca `IfDataValores(AnoMes=AAAAMM, TipoInstituicao=1, Relatorio=X)`
      - Filtra por `CodConglomerado` dos Top 50 (via `$filter` OData ou pós-filtro)
      - Adiciona coluna `CNPJ8` (via mapa reverso) e `Relatorio`
5. Consolida e grava

**Saída:**
- `data/curated/cadastro/cadastro_map.parquet` (CNPJ8 ↔ CodConglomerado)
- `data/curated/ifdata/ifdata_valores.parquet` (indicadores consolidados)

### 7.5 Dashboard (app.py)

**Abas/Seções:**

1. **Ranking Top 50** — Tabela com ranking por PL na data-base selecionada
2. **Série Temporal** — Gráfico de linhas do KPI selecionado (PL, Ativo Total, Resultado) para conglomerados selecionados
3. **Explorador de Contas** — Filtragem por prefixo de conta COSIF, top N contas por saldo
4. **Indicadores IF.data** — Gráficos por relatório (Resumo, DRE, Capital) para os conglomerados selecionados

**Filtros globais:**
- Data inicial / Data final (AAAAMM)
- KPI principal (patrimonio_liquido, ativo_total, resultado)
- Seleção de conglomerados (multiselect, default = top 10 do Top 50 mais recente)

**Dados carregados via DuckDB** (leitura direta de Parquet, sem carregar tudo em Pandas).

---

## 8. Indicadores IF.data Recomendados (MVP)

### Relatório 1 — Resumo

| Indicador                                  | Relevância                              |
|--------------------------------------------|------------------------------------------|
| Ativo Total                                | Tamanho do conglomerado                  |
| Patrimônio Líquido                         | Base para ranking e solidez              |
| Lucro Líquido                              | Rentabilidade absoluta                   |
| Carteira de Crédito Classificada           | Exposição ao crédito                     |
| Captações                                  | Funding / base de depósitos              |
| Receitas de Intermediação Financeira       | Core revenue                             |
| Despesas de Intermediação Financeira       | Core cost                                |

### Relatório 4 — Demonstração de Resultado (DRE)

| Indicador                                  | Relevância                              |
|--------------------------------------------|------------------------------------------|
| Margem financeira (receita - despesa interm.)| Spread operacional                     |
| Provisão para Créditos de Liquidação Duvidosa| Qualidade de crédito                   |
| Resultado Operacional                      | Eficiência operacional                   |

### Relatório 5 — Informações de Capital

| Indicador                                  | Relevância                              |
|--------------------------------------------|------------------------------------------|
| Índice de Basileia                         | Adequação de capital (regulatório)       |
| Patrimônio de Referência (PR)              | Capital disponível                       |
| Ativos Ponderados pelo Risco (RWA)         | Base de risco                            |

> **Nota:** Os nomes exatos dos indicadores (`NomeLinha`/`NomeColuna`) serão confirmados
> na primeira execução do script 04, ao consultar `ListaDeRelatorio` e `IfDataValores`.
> O dashboard exibe os nomes tal como retornados pela API.

---

## 9. Configuração

### 9.1 config/settings.toml

```toml
[bcb]
# Padrão 1: com barra (CSV version)
bancos_zip_v1 = "https://www4.bcb.gov.br/fis/cosif/cont/balan/bancos/{yyyymm}/BANCOS.ZIP"
# Padrão 2: sem barra (legacy)
bancos_zip_v2 = "https://www4.bcb.gov.br/fis/cosif/cont/balan/bancos/{yyyymm}BANCOS.ZIP"
# Defasagem mínima (dias) após data-base para disponibilidade
defasagem_dias = 60
defasagem_dias_dez = 90

[paths]
raw = "data/raw"
staging = "data/staging"
curated_bal4040 = "data/curated/bal_4040"
curated_top50 = "data/curated/top50"
curated_cadastro = "data/curated/cadastro"
curated_ifdata = "data/curated/ifdata"

[top50]
n = 50

[ifdata]
base_odata = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"
timeout_sec = 60
tipo_instituicao = 1
# Relatórios prioritários: 1=Resumo, 4=DRE, 5=Capital
relatorios = ["1", "4", "5"]
```

### 9.2 config/kpi_map.json

```json
{
  "patrimonio_liquido": {
    "conta_exact": ["6.0.0.00.00-2"],
    "nome_regex": ["patrimonio\\s+liquido", "patrim[oô]nio\\s+l[ií]quido"]
  },
  "ativo_total": {
    "conta_exact": ["1.0.0.00.00-7"],
    "nome_regex": ["ativo\\s+total"]
  },
  "resultado": {
    "conta_exact": ["7.0.0.00.00-9"],
    "nome_regex": ["resultado\\s+l[ií]quido", "lucro\\s+l[ií]quido"]
  }
}
```

> **Nota sobre contas COSIF:** As contas de nível 1 no COSIF são:
> - `1.0.0.00.00-7` — Ativo Total
> - `4.0.0.00.00-8` — Circulante e Exigível a Longo Prazo
> - `6.0.0.00.00-2` — Patrimônio Líquido
> - `7.0.0.00.00-9` — Contas de Resultado Credoras
> - `8.0.0.00.00-6` — Contas de Resultado Devedoras
>
> A precisão dessas contas deve ser validada na primeira execução contra
> os dados reais. O fallback por regex existe para lidar com variações.

---

## 10. requirements.txt

```
pandas>=2.1,<3
pyarrow>=14,<18
duckdb>=0.10,<2
requests>=2.31,<3
streamlit>=1.30,<2
plotly>=5.18,<6
python-dateutil>=2.8,<3
tqdm>=4.66,<5
tenacity>=8.2,<9
```

> Ranges ao invés de versões fixas para compatibilidade. Lock com `pip freeze` no ambiente real.

---

## 11. Correções Aplicadas ao Plano Original

| # | Problema no plano original                                          | Correção                                                       |
|---|----------------------------------------------------------------------|----------------------------------------------------------------|
| 1 | IF.data chamado sem parâmetros obrigatórios (`AnoMes`, `TipoInstituicao`, `Relatorio`) | Chamadas com os 3 parâmetros obrigatórios                     |
| 2 | `guess_fields` desnecessário — campos da API são conhecidos          | Campos fixos: `CodConglomerado`, `NomeColuna`, `ValorA`, etc. |
| 3 | Sem mapeamento CNPJ ↔ CodConglomerado entre fontes                 | Usa `IfDataCadastro` para construir tabela de-para             |
| 4 | Memory blow-up: `fetchdf()` carrega tudo em Pandas via DuckDB       | SQL analítico direto no DuckDB, só resultado final vai para Pandas |
| 5 | Variável `df` colide com selectbox no app.py                        | Renomear variável do selectbox (`data_fim`)                    |
| 6 | Paths hardcoded no app.py                                           | Lê de `settings.toml`                                          |
| 7 | Sem incremental real (scripts 02/03 reprocessam tudo)               | Checagem de partições existentes antes de processar            |
| 8 | Sem retry no download de balancetes                                 | Tenacity com backoff + fallback para URL alternativa           |
| 9 | Sem `.gitignore`                                                     | Adicionado                                                     |
| 10| Sem logging                                                         | `logging` com níveis info/warning/error                        |
| 11| `tomllib` requer Python 3.11+ não documentado                      | Documentado na seção de Stack                                  |
| 12| Uma única URL de download sem fallback                              | Duas variantes de URL com fallback                             |
| 13| IF.data busca entity-by-entity (50 requests por trimestre×relatório)| Busca por relatório completo + pós-filtro por CodConglomerado  |
| 14| Sem testes                                                          | Pasta `tests/` com testes unitários para funções críticas      |

---

## 12. Riscos e Mitigações

| Risco                                           | Probabilidade | Impacto | Mitigação                                           |
|-------------------------------------------------|---------------|---------|------------------------------------------------------|
| URL `www4.bcb.gov.br` desativada/migrada         | Média         | Alto    | Duas variantes de URL; log claro; fallback manual     |
| IF.data indisponível / rate limit                | Baixa         | Médio   | Retry com backoff; cache local; busca por relatório inteiro (não por entidade) |
| Conta COSIF para PL muda entre períodos          | Baixa         | Alto    | `kpi_map.json` com fallback regex; validação na primeira execução |
| Volume de dados excede memória (24 meses)        | Média         | Alto    | SQL no DuckDB, não `fetchdf()` sem filtro             |
| Schema IF.data muda (novos campos/nomes)         | Baixa         | Médio   | Campos documentados na API são estáveis; log warnings para campos inesperados |
| Mapeamento CNPJ→CodConglomerado incompleto      | Média         | Médio   | Log de entidades sem match; dashboard mostra dados parciais com aviso |

---

## 13. Fora de Escopo (MVP)

- Automação via cron/scheduler (será fase 2)
- Deploy em cloud (roda local)
- Autenticação no dashboard
- Dados de cooperativas, consórcios, ou sociedades
- Documento 4010 (individual)
- Relatórios de crédito detalhados do SCR (IF.data relatórios 6+)
- Comparação com peers internacionais
- Exportação PDF/Excel do dashboard

---

## 14. Próximos Passos (Pós-MVP)

1. **Automação:** Cron job ou Airflow para execução periódica dos scripts
2. **Mais KPIs COSIF:** Carteira de Crédito, Depósitos, Índice de Eficiência calculado
3. **Mais relatórios IF.data:** Carteira de Crédito por risco, por indexador, por região
4. **Enriquecimento:** Código do conglomerado (`C00xxxxx`) no UI via IfDataCadastro
5. **Deploy:** Containerização (Docker) e deploy em servidor/cloud
6. **Histórico longo:** Ampliar de 24 para 60+ meses
