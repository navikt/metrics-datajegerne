import json
import uuid
from datetime import datetime

import pandas as pd
import pandas_gbq
from google.cloud import bigquery

def run_etl_tildelt_og_notater():
    # Leser data
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.landing_zone.etterlevelse_etterlevelse_metadata`", "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    df.rename({"krav_nummer": "kravNummer"}, axis=1, inplace=True)
    df.rename({"id": "table_id"}, axis=1, inplace=True)
    df.rename({"etterlevelse_dokumentasjon": "etterlevelseDokumentasjonId"}, axis=1, inplace=True)

    # Pakker ut json-blob
    for col in ["notater", "tildeltMed"]:
        df[col] = df["data"].apply(lambda x: json.loads(x)[col] if col in json.loads(x).keys() else None)


    # Beholder kun disse kolonnene
    df = df[["etterlevelseDokumentasjonId", "table_id", "notater", "tildeltMed", "kravNummer"]]

    # Kobler sammen med tema
    df_tema = pandas_gbq.read_gbq("SELECT distinct kravNummer, tema FROM `teamdatajegerne-prod-c8b1.landing_zone.etterlevelse_krav_tema`", "teamdatajegerne-prod-c8b1", progress_bar_type=None)
    df = df.merge(df_tema, on="kravNummer", how="outer")

    df = df.explode("tildeltMed")
    tildeltMedDict = {val: str(uuid.uuid4()) for val in df["tildeltMed"].unique()} # anonymiserer
    df.loc[df["tildeltMed"].notnull(), "tildeltMed"] = df.loc[df["tildeltMed"].notnull(), "tildeltMed"].map(tildeltMedDict)

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "notaterOgTildelt"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None

