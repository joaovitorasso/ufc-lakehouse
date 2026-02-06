# Databricks notebook source
# UFC Lakehouse - Ingestão a partir do ZIP "landing" (enviado via GitHub Actions)
#
# Params:
#   landing_zip : caminho WSFS (ex: /Users/<email>/ufc-lakehouse/landing/dt=YYYY-MM-DD/ufc_landing.zip)
#   run_date    : YYYY-MM-DD (mesma partição usada no landing)

dbutils.widgets.text("landing_zip", "")
dbutils.widgets.text("run_date", "")

landing_zip = dbutils.widgets.get("landing_zip").strip()
run_date = dbutils.widgets.get("run_date").strip()

assert landing_zip, "Param landing_zip vazio"
assert run_date, "Param run_date vazio"

from pyspark.sql import functions as F
import zipfile

# Importante: no Free Edition o DBFS root está bloqueado, então usamos Workspace Files (WSFS)
# Exemplo esperado: /Users/<email>/ufc-lakehouse/landing/dt=YYYY-MM-DD/ufc_landing.zip
assert landing_zip.startswith("/Users/"), f"Esperava caminho WSFS /Users/... mas veio: {landing_zip}"

print(f"[ingest] landing_zip={landing_zip}")
print(f"[ingest] run_date={run_date}")

# 1) Ler arquivos dentro do ZIP (sem extrair em /dbfs)
with zipfile.ZipFile(landing_zip, "r") as z:
    # Se quiser debugar nomes existentes:
    # print(z.namelist())

    events_bytes = z.read("events.json")
    fights_bytes = z.read("fights.jsonl")
    fighters_bytes = z.read("fighters.jsonl")

events_str = events_bytes.decode("utf-8")

fights_lines = [l for l in fights_bytes.decode("utf-8").splitlines() if l.strip()]
fighters_lines = [l for l in fighters_bytes.decode("utf-8").splitlines() if l.strip()]

print(f"[ingest] fights_lines={len(fights_lines)} fighters_lines={len(fighters_lines)}")

# 2) Criar DataFrames Spark a partir de strings/linhas (ótimo para volume pequeno)
events_df = (
    spark.read.json(sc.parallelize([events_str]))
    .withColumn("run_date", F.lit(run_date))
)

fights_df = (
    spark.read.json(sc.parallelize(fights_lines))
    .withColumn("run_date", F.lit(run_date))
)

fighters_df = (
    spark.read.json(sc.parallelize(fighters_lines))
    .withColumn("run_date", F.lit(run_date))
)

# 3) Salvar Bronze (append) - mantendo partição por run_date
events_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_events")
fights_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_fights")
fighters_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_fighters")

display(fights_df.limit(10))
