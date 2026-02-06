from __future__ import annotations
from typing import List, Dict, Any, Optional

def write_delta_append(
    spark,
    rows: List[Dict[str, Any]],
    delta_path: str,
    partition_col: Optional[str] = "ingestion_date",
    coalesce_n: int = 1,
):
    """
    Escreve lista de dicts em Delta (append). Ideal para Bronze.
    - delta_path deve ser dbfs:/...
    - partition_col: particiona por ingestion_date (bom pra escala)
    """
    if not rows:
        return 0

    df = spark.createDataFrame(rows)

    writer = df.coalesce(coalesce_n).write.format("delta").mode("append")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.save(delta_path)
    return len(rows)