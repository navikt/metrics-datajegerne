import json
import uuid

import pandas as pd
import numpy as np

import pandas_gbq
from google.cloud import bigquery


def run_etl_behandlinger():
    sql = "SELECT * FROM `teamdatajegerne-prod-c8b1.behandlingskatalogen.audit_version_raw` where table_name = 'PROCESS' order by time desc"
    df = pandas_gbq.read_gbq(sql, "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    # Finner gjeldende observasjon
    df["maxTime"] = df.groupby("table_id")["time"].transform("max")
    df["aktivObservasjon"] = False
    df.loc[df["time"] == df["maxTime"], "aktivObservasjon"] = True

    # Enklere å jobbe med json-bloben
    df["data"] = df["data"].apply(lambda x: json.loads(x))

    # Graver ut av json-bloben
    # Først policies som blir en koblingstabell mellom behandlinger og policies
    df["policies"] = df["data"].apply(lambda x: x["policies"])

    # Databehandlere: Dette blir en koblingstabell mellom behandlinger og databehandlere
    df["dataProcessing"] = df["data"].apply(lambda x: x["data"]["dataProcessing"] if "dataProcessing" in x["data"] else None)

    # S


    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "behandlinger"
    table = "behandling_policy"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None