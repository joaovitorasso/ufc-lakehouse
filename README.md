# ufc-lakehouse

Pipeline estilo *Lakehouse* (RAW → Bronze → Silver → Gold) para coletar e modelar dados do UFC Stats (eventos, lutas e lutadores).

> Fonte: ufcstats.com (coleta via `requests` + `BeautifulSoup`).  
> Este projeto salva snapshots HTML em `data/raw/html` para permitir reprocessamento sem bater no site.

## Estrutura (resumo)

- **RAW**: índice de eventos (jsonl) + HTML de eventos/lutadores + metadados de execução
- **BRONZE**: tabelas semi-estruturadas (jsonl) para eventos, lutas e lutadores
- **SILVER**: dados curados (dedupe + chaves resolvidas)
- **GOLD**: marts prontos para BI (agregações)

## Como rodar

1) Crie um ambiente e instale dependências:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

2) Copie `.env.example` para `.env` e ajuste se quiser.

3) Execute via CLI:

```bash
# roda tudo (raw -> bronze -> silver -> gold)
python -m ufc_pipeline run all

# rodar por estágio
python -m ufc_pipeline run raw
python -m ufc_pipeline run bronze
python -m ufc_pipeline run silver
python -m ufc_pipeline run gold
```

## Saídas

- `data/raw/...` snapshots e metadados (run_id, dt)
- `data/bronze/...` jsonl de eventos/lutas/lutadores
- `data/silver/...` curado (jsonl/parquet)
- `data/gold/marts/...` marts (jsonl/parquet)
- `data/exports/...` exportações auxiliares (csv/parquet)

## Observações importantes

- O site pode mudar HTML. Por isso o RAW salva snapshots.
- Respeite o site: use as opções de `POLITE_DELAY_SECONDS` e `MAX_REQUESTS_PER_MINUTE` no `configs/settings.yaml`.
- Este repositório não inclui dados coletados por padrão. A pasta `data/` fica no `.gitignore`.
