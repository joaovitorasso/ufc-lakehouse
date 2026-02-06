# Databricks notebook source
import sys
from datetime import date

# 1) Ajuste esse caminho para a raiz do seu projeto no Workspace
PROJECT_ROOT = "/Workspace/Users/joaovitorasso@hotmail.com/ufc-lakehouse"
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 2) No Free Edition, use FileStore (mais fácil que /mnt)
BRONZE_ROOT = "dbfs:/FileStore/ufc/bronze"

# 3) Uma data de ingestão consistente
INGESTION_DATE = str(date.today())

print("PROJECT_ROOT:", PROJECT_ROOT)
print("BRONZE_ROOT:", BRONZE_ROOT)
print("INGESTION_DATE:", INGESTION_DATE)

# COMMAND ----------

from src.ufc.config import UFCConfig
from src.ufc.common import make_run_id
from src.ufc.pipelines.fighters_pipeline import run_fighters_pipeline

cfg = UFCConfig(bronze_root=BRONZE_ROOT)
run_id = make_run_id()

run_fighters_pipeline(
    spark,
    cfg,
    ingestion_date=INGESTION_DATE,
    run_id=run_id,
    only_new_fighters=True
)
print("fighters done. run_id:", run_id)