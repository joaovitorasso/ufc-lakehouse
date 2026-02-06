# Databricks notebook source
import requests

# COMMAND ----------

import requests

tests = [
    "https://pypi.org/simple/",
    "https://files.pythonhosted.org/",
    "https://raw.githubusercontent.com/",
]

for url in tests:
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        print(url, "->", r.status_code, "len:", len(r.text))
    except Exception as e:
        print(url, "-> FAIL:", type(e).__name__, e)
