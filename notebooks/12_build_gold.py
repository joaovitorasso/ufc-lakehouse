# Databricks notebook source
# UFC Lakehouse - Gold (métricas / marts)

from pyspark.sql import functions as F

ff = spark.table("silver_ufc_fighter_fights")

wins = (
    ff.groupBy("fighter_id")
      .agg(
          F.sum(F.when(F.lower("result") == "win", 1).otherwise(0)).alias("wins"),
          F.count("*").alias("total_fights")
      )
      .orderBy(F.desc("wins"))
)

wins.write.mode("overwrite").format("delta").saveAsTable("gold_ufc_wins_by_fighter")

by_method = (
    ff.groupBy("method_short")
      .agg(F.count("*").alias("n"))
      .orderBy(F.desc("n"))
)

by_method.write.mode("overwrite").format("delta").saveAsTable("gold_ufc_fights_by_method")

display(wins.limit(20))
