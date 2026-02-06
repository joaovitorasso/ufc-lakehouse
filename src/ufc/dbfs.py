from __future__ import annotations

def dbfs_to_local(dbfs_path: str) -> str:
    """
    Converte:
      dbfs:/mnt/x/y -> /dbfs/mnt/x/y
    """
    if not dbfs_path.startswith("dbfs:/"):
        raise ValueError(f"Esperava caminho dbfs:/..., veio: {dbfs_path}")
    return "/dbfs/" + dbfs_path[len("dbfs:/"):]