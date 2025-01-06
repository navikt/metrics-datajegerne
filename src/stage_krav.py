import json
from datetime import datetime

import pandas as pd
import pandas_gbq

from google.cloud import bigquery

def run_etl_tema():
    # Kobling mellom krav og regelverk
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw_krav`", "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    # Pakker ut json
    df["regelverk"] = df["data"].apply(lambda x: [item["lov"] for item in json.loads(x)["regelverk"]])
    df["kravNummer"] = df["data"].apply(lambda x: json.loads(x)["kravNummer"])
    df = df.explode("regelverk")

    # duplikater fordi regelverk er på detaljert nivå
    df = df[["kravNummer", "regelverk"]].drop_duplicates().copy()

    # Må koble på tema og
    df_2 = pandas_gbq.read_gbq("SELECT distinct code as regelverk, tema, underavdeling FROM `teamdatajegerne-prod-c8b1.metrics.raw_tema`", "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    #Og merge
    df = df.merge(df_2, on="regelverk", how="left")

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "stage_krav"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None

