import json
from datetime import datetime

import pandas as pd

import pandas_gbq
from google.cloud import bigquery

def run_etl_duplicates():
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw_generic_storage` where type = 'Etterlevelse'", "teamdatajegerne-prod-c8b1")
    # Henter ut fra json
    for var in ["kravNummer", "kravVersjon", "etterlevelseDokumentasjonId"]:
        df[var] = df["data"].apply(lambda x: json.loads(x)[var])

    # Ser etter duplikater
    df_duplicates = df.groupby(["etterlevelseDokumentasjonId", "kravNummer", "kravVersjon"])["id"].count().reset_index()
    df_to_bq = df_duplicates[df_duplicates["id"] > 1].sort_values(by=["etterlevelseDokumentasjonId", "kravNummer"], ascending=False)

    # Skriver til BigQuery
    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "duplikater_etterlevelse"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df_to_bq, table_id, job_config=job_config)

    return None