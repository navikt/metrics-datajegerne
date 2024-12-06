import json
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_gbq
from google.cloud import bigquery

def run_etl_websak():
    # Leser inn
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseArkiv'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    # Må hente ut fra json-objektet
    for col in ["status", "version", "arkiveringDato", "websakNummer", "onlyActiveKrav", "etterlevelseDokumentasjonId"]:
        temp_list = []
        for value in df["data"].values:
            json_item = json.loads(value)["data"]
            temp_list.append(None if col not in json_item.keys() else json_item[col])
        df[col] = temp_list

    # Beholder de som kun er arkivert
    df = df[df["status"] == "ARKIVERT"]
    df = df[["time", "onlyActiveKrav", "etterlevelseDokumentasjonId", "websakNummer"]]

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")
    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "arkivering_websak"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    return None