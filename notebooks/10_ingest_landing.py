# UFC Lakehouse - Ingestão a partir do ZIP "landing" (enviado via GitHub Actions)
#
# Params:
#   landing_zip : workspace path (ex: /Users/<email>/ufc-lakehouse/landing/dt=YYYY-MM-DD/ufc_landing.zip)
#   run_date    : YYYY-MM-DD

dbutils.widgets.text("landing_zip", "")
dbutils.widgets.text("run_date", "")

landing_zip = dbutils.widgets.get("landing_zip").strip()
run_date = dbutils.widgets.get("run_date").strip()

assert landing_zip, "Param landing_zip vazio"
assert run_date, "Param run_date vazio"

from pyspark.sql import functions as F
import os, zipfile

# Garantir formato de workspace path
# Aceita: /Users/...  ou  /Workspace/Users/...
if landing_zip.startswith("/Workspace/"):
    ws_path = landing_zip
else:
    ws_path = "/Workspace" + landing_zip  # vira /Workspace/Users/...

print(f"[ingest] landing_zip(raw)={landing_zip}")
print(f"[ingest] ws_path={ws_path}")
print(f"[ingest] run_date={run_date}")

# Copiar para o disco local do driver (sempre acessível)
local_zip = f"file:/tmp/ufc_landing_{run_date}.zip"

# Workspace Files -> local
# origem precisa ser file:/Workspace/... (repare no prefixo file:)
src = "file:" + ws_path

print(f"[ingest] copying from {src} to {local_zip}")
dbutils.fs.cp(src, local_zip, True)

# Agora abrimos com zipfile usando path local do SO
local_zip_os = f"/tmp/ufc_landing_{run_date}.zip"
assert os.path.exists(local_zip_os), f"ZIP não encontrado em {local_zip_os}"

with zipfile.ZipFile(local_zip_os, "r") as z:
    # debug opcional
    # print(z.namelist())

    events_bytes = z.read("events.json")
    fights_bytes = z.read("fights.jsonl")
    fighters_bytes = z.read("fighters.jsonl")

events_str = events_bytes.decode("utf-8")
fights_lines = [l for l in fights_bytes.decode("utf-8").splitlines() if l.strip()]
fighters_lines = [l for l in fighters_bytes.decode("utf-8").splitlines() if l.strip()]

print(f"[ingest] fights_lines={len(fights_lines)} fighters_lines={len(fighters_lines)}")

# Criar DataFrames
events_df = spark.read.json(sc.parallelize([events_str])).withColumn("run_date", F.lit(run_date))
fights_df = spark.read.json(sc.parallelize(fights_lines)).withColumn("run_date", F.lit(run_date))
fighters_df = spark.read.json(sc.parallelize(fighters_lines)).withColumn("run_date", F.lit(run_date))

# Salvar Bronze
events_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_events")
fights_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_fights")
fighters_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("bronze_ufc_fighters")

display(fights_df.limit(10))
