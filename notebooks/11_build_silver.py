# Databricks notebook source
# UFC Lakehouse - Silver (normalização)

from pyspark.sql import functions as F

# Events (events.json contém meta + arrays completed/upcoming)
raw = spark.table("bronze_ufc_events")

completed = raw.select(F.explode("completed").alias("e"), "run_date")
upcoming  = raw.select(F.explode("upcoming").alias("e"), "run_date")

events = (
    completed.unionByName(upcoming)
    .select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.name").alias("name"),
        F.col("e.link").alias("event_url"),
        F.col("e.date").alias("event_date"),
        F.col("e.location").alias("location"),
        F.col("e.status").alias("status"),
        F.col("run_date")
    )
    .dropDuplicates(["event_id", "status"])
)

events.write.mode("overwrite").format("delta").saveAsTable("silver_ufc_events")

# Fights
fights_raw = spark.table("bronze_ufc_fights")

fights = (
    fights_raw
    .select(
        "fight_id", "fight_url", "event_id", "event_name",
        "method", "round", "time", "time_format", "referee",
        "fighters", "source", "run_id", "ingestion_ts", "run_date"
    )
    .dropDuplicates(["fight_id"])
)

fights.write.mode("overwrite").format("delta").saveAsTable("silver_ufc_fights")

# Fighters (bio)
fighters_raw = spark.table("bronze_ufc_fighters")

fighters = (
    fighters_raw
    .select(
        "fighter_id", "name", "link",
        F.col("bio.cartel").alias("cartel"),
        F.col("bio.height").alias("height"),
        F.col("bio.weight").alias("weight"),
        F.col("bio.reach").alias("reach"),
        F.col("bio.stance").alias("stance"),
        F.col("bio.dob").alias("dob"),
        "source", "run_id", "ingestion_ts", "run_date"
    )
    .dropDuplicates(["fighter_id"])
)

fighters.write.mode("overwrite").format("delta").saveAsTable("silver_ufc_fighters")

# Fighter fights history (explode)
fighter_fights = (
    fighters_raw
    .select("fighter_id", "name", F.explode("fights").alias("f"), "run_date")
    .select(
        "fighter_id",
        F.col("f.fight_id").alias("fight_id"),
        F.col("f.result").alias("result"),
        F.col("f.opponent").alias("opponent"),
        F.col("f.event_name").alias("event_name"),
        F.col("f.event_date").alias("event_date"),
        F.col("f.title_bout").alias("title_bout"),
        F.col("f.method_short").alias("method_short"),
        F.col("f.method_detail").alias("method_detail"),
        F.col("f.round").alias("round"),
        F.col("f.time").alias("time"),
        "run_date"
    )
)

fighter_fights.write.mode("overwrite").format("delta").saveAsTable("silver_ufc_fighter_fights")

display(events.limit(10))
