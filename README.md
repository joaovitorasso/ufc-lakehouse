# ufc-lakehouse (Databricks Free Edition friendly)

Este projeto constrói um mini *lakehouse* de dados do **ufcstats.com** usando:

- **GitHub Actions** para **extrair** (porque o Databricks Free Edition pode bloquear DNS/egress)
- Upload do resultado (ZIP) para o **DBFS** via API
- **Databricks** para ingerir e transformar em **Delta** (Bronze/Silver/Gold)

## Arquitetura

1. GitHub Actions roda `tools/extract_landing.py`
2. Gera `out/dt=YYYY-MM-DD/ufc_landing.zip` com:
   - `events.json`
   - `fights.jsonl`
   - `fighters.jsonl`
3. Workflow faz upload para `dbfs:/tmp/ufc/landing/dt=YYYY-MM-DD/ufc_landing.zip`
4. Workflow chama `jobs/run-now` para executar um Job no Databricks que roda:
   - `notebooks/10_ingest_landing.py`
   - `notebooks/11_build_silver.py`
   - `notebooks/12_build_gold.py`

## Como rodar localmente (teste rápido)

```bash
pip install -r requirements.txt
export RUN_DATE=2026-02-06
export MAX_EVENTS=10
python tools/extract_landing.py
```

Você verá o ZIP em `out/dt=2026-02-06/ufc_landing.zip`.

## Configurar GitHub Actions

Crie **Repository Secrets**:

- `DATABRICKS_HOST` (sem https://)  
  Ex: `adb-1234567890123456.7.azuredatabricks.net`
- `DATABRICKS_TOKEN` (PAT do Databricks)
- `DATABRICKS_JOB_ID` (ID do Job que você criar no Databricks)
- `MAX_EVENTS` (opcional, ex: `20`)

O workflow está em `.github/workflows/pipeline.yml`.

## Configurar Databricks

1. Importe os notebooks:
   - `notebooks/10_ingest_landing.py`
   - `notebooks/11_build_silver.py`
   - `notebooks/12_build_gold.py`

2. Crie um **Job** com 3 tasks (em sequência):
   - Task 1: `10_ingest_landing.py` (Notebook params: `landing_zip`, `run_date`)
   - Task 2: `11_build_silver.py`
   - Task 3: `12_build_gold.py`

3. Pegue o **Job ID** e coloque no secret `DATABRICKS_JOB_ID`.

## Tabelas geradas

**Bronze**
- `bronze_ufc_events`
- `bronze_ufc_fights`
- `bronze_ufc_fighters`

**Silver**
- `silver_ufc_events`
- `silver_ufc_fights`
- `silver_ufc_fighters`
- `silver_ufc_fighter_fights`

**Gold**
- `gold_ufc_wins_by_fighter`
- `gold_ufc_fights_by_method`

## Observações

- Para manter o pipeline rápido e gentil com o site, usamos `MAX_EVENTS` (padrão 20).
- Se quiser histórico maior, aumente `MAX_EVENTS` — mas isso aumenta tempo de execução.
