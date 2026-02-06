# Databricks notebook source
# UFC Lakehouse - Ingestão a partir do ZIP "landing" (enviado via GitHub Actions)
#
# Params:
#   landing_zip : caminho dbfs:/.../ufc_landing.zip
#   run_date    : YYYY-MM-DD (mesma partição usada no landing)

dbutils.widgets.text("landing_zip", "")
dbutils.widgets.text("run_date", "")

landing_zip = dbutils.widgets.get("landing_zip")
run_date = dbutils.widgets.get("run_date")

assert landing_zip, "Param landing_zip vazio"
assert run_date, "Param run_date vazio"

import os, zipfile
from pyspark.sql import functions as F

# extrai o ZIP para uma pasta em /dbfs (filesystem local do cluster)
extract_dir_local = f"/dbfs/tmp/ufc/landing_extract/dt={run_date}"
os.makedirs(extract_dir_local, exist_ok=True)

zip_local = landing_zip.replace("dbfs:", "/dbfs")

with zipfile.ZipFile(zip_local, "r") as z:
    z.extractall(extract_dir_local)

events_path   = f"dbfs:/tmp/ufc/landing_extract/dt={run_date}/events.json"
fights_path   = f"dbfs:/tmp/ufc/landing_extract/dt={run_date}/fights.jsonl"
fighters_path = f"dbfs:/tmp/ufc/landing_extract/dt={run_date}/fighters.jsonl"

# --- Bronze tables (append) ---
# Observação: Bronze é "raw-ish", mas já vem estruturado pelo parser.
# A partição é run_date.

events_df = spark.read.json(events_path).withColumn("run_date", F.lit(run_date))
events_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_events")

fights_df = spark.read.json(fights_path).withColumn("run_date", F.lit(run_date))
fights_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_fights")

fighters_df = spark.read.json(fighters_path).withColumn("run_date", F.lit(run_date))
fighters_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_fighters")

display(fights_df.limit(10))
