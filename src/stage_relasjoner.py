import json

import pandas as pd
import numpy as np

import pandas_gbq
from google.cloud import bigquery


def run_etl_relasjoner():
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.landing_zone.etterlevelse_audit_version` where table_name ='DOCUMENT_RELATION'", "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    # Json-berry
    df["data"] = df["data"].apply(lambda x: json.loads(x))

    cols_to_keep = ["id", "toDocument", "fromDocument", "relationType"]
    for col in cols_to_keep:
        df[col] = df["data"].apply(lambda x: x[col])

    df = df[cols_to_keep]

    # Endrer navn på kolonner
    rename_dict = {
        "toDocument": "etterlevelseDokumentasjonIdTil",
        "fromDocument": "etterlevelseDokumentasjonIdFra"
    }
    df.rename(rename_dict, axis=1, inplace=True)

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "stage_relasjoner"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None