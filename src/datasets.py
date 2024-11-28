import json

import pandas as pd
import numpy as np

import pandas_gbq
from google.cloud import bigquery


project = "teamdatajegerne-prod-c8b1"
dataset = "etterlevelse"

def run_etl_beskrivelser_datasett():

    df = pandas_gbq.read_gbq("SELECT etterlevelseDokumentasjonId, time, varslingsadresser, aktivRad FROM `teamdatajegerne-prod-c8b1.etterlevelse.stage_dokument`", "teamdatajegerne-prod-c8b1")

    # Må merke dokumenter som har varslingsadresse
    df["harVarslingsadresse"] = False
    df.loc[(~df["varslingsadresser"].isnull()) & (df["varslingsadresser"].apply(lambda x: len(x)) > 0), "harVarslingsadresse"] = True

    # Varslingsadresser er lagret som en list av dicts
    df = df.explode("varslingsadresser")

    # ...og denne dicten kan vi pakke ut i to kolonner: Type og adresse
    for col in ["type", "adresse"]:
        df[col] = df["varslingsadresser"].apply(lambda x: x[col] if pd.notnull(x) else None)

    # Tidsserien
    df_timeseries = df.groupby(["etterlevelseDokumentasjonId", "harVarslingsadresse"])["time"].apply(np.min).reset_index().query("harVarslingsadresse == True")
    df_timeseries = df_timeseries.sort_values(by="time")
    # Snapshot
    df_snapshot = df.query("aktivRad == True").groupby("type")["etterlevelseDokumentasjonId"].nunique().reset_index(name="antall")
    df_snapshot = df_snapshot.sort_values(by="antall")

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")

    table_dict = {"ds_varslinger_tid": df_timeseries,
                  "ds_varslinger_snapshot": df_snapshot}


    for table in table_dict:
        df = table_dict[table]
        table_id = f"{project}.{dataset}.{table}"
        job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None


def run_etl_beskrivelser_datasett():
