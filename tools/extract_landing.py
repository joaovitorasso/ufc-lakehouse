# Params
dbutils.widgets.text("landing_dir", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("db_token", "")

landing_dir = dbutils.widgets.get("landing_dir").strip()
run_date = dbutils.widgets.get("run_date").strip()
db_token = dbutils.widgets.get("db_token").strip()

assert landing_dir, "Param landing_dir vazio"
assert run_date, "Param run_date vazio"
assert db_token, "Param db_token vazio"

from pyspark.sql import functions as F
import requests

print(f"[ingest] landing_dir={landing_dir}")
print(f"[ingest] run_date={run_date}")

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
host = "https://" + workspace_url
print(f"[ingest] host={host}")

headers = {"Authorization": f"Bearer {db_token}"}

def get_ws_file_bytes(path_users: str) -> bytes:
    assert path_users.startswith("/Users/"), f"Esperava /Users/... mas veio: {path_users}"
    url = f"{host}/api/2.0/fs/files{path_users}"
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        raise RuntimeError(f"Falha ao baixar {path_users}. status={r.status_code} body={r.text[:300]}")
    return r.content

events_bytes   = get_ws_file_bytes(f"{landing_dir}/events.json")
fights_bytes   = get_ws_file_bytes(f"{landing_dir}/fights.jsonl")
fighters_bytes = get_ws_file_bytes(f"{landing_dir}/fighters.jsonl")

events_str = events_bytes.decode("utf-8")
fights_lines = [l for l in fights_bytes.decode("utf-8").splitlines() if l.strip()]
fighters_lines = [l for l in fighters_bytes.decode("utf-8").splitlines() if l.strip()]

print(f"[ingest] downloaded: events_bytes={len(events_bytes)} fights_lines={len(fights_lines)} fighters_lines={len(fighters_lines)}")

events_df = spark.read.json(sc.parallelize([events_str])).withColumn("run_date", F.lit(run_date))
fights_df = spark.read.json(sc.parallelize(fights_lines)).withColumn("run_date", F.lit(run_date))
fighters_df = spark.read.json(sc.parallelize(fighters_lines)).withColumn("run_date", F.lit(run_date))

spark.sql("CREATE DATABASE IF NOT EXISTS ufc")
spark.sql("USE ufc")

events_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("ufc.bronze_ufc_events")
fights_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("ufc.bronze_ufc_fights")
fighters_df.write.mode("append").format("delta").partitionBy("run_date").saveAsTable("ufc.bronze_ufc_fighters")

print("[ingest] ✅ bronze tables updated")
display(fights_df.limit(10))
