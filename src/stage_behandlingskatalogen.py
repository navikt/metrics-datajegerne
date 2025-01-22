import json
import uuid

import pandas as pd
import numpy as np

import pandas_gbq
from google.cloud import bigquery


def run_etl_behandlinger():
    sql = "SELECT * FROM `teamdatajegerne-prod-c8b1.behandlingskatalogen.audit_version_raw` where table_name = 'PROCESS' order by time desc"
    df = pandas_gbq.read_gbq(sql, "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    # Renamer litt
    df.rename({"table_id": "behandlingsId"}, axis=1, inplace=True)

    # Finner gjeldende observasjon
    df["maxTime"] = df.groupby("behandlingsId")["time"].transform("max")
    df["aktivObservasjon"] = False
    df.loc[df["time"] == df["maxTime"], "aktivObservasjon"] = True

    # Enklere å jobbe med json-bloben
    df["data"] = df["data"].apply(lambda x: json.loads(x))

    # Oppretter en dict som holder tabellene vi skal skrive til BigQuery
    df_dict = {}

    # Graver ut av json-bloben
    # Først policies som blir en koblingstabell mellom behandlinger og policies
    df["policies"] = df["data"].apply(lambda x: x["policies"])
    df_pol = df[["behandlingsId", "policies"]].explode("policies")
    df_pol["policyId"] = df_pol["policies"].explode().apply(lambda x: x["id"] if pd.notnull(x) and "id" in x else None)
    df_dict["stage_bridge_policy_behandling"] = df_pol

    # Databehandlere: Dette blir en koblingstabell mellom behandlinger og databehandlere
    df["dataProcessing"] = df["data"].apply(lambda x: x["data"]["dataProcessing"] if "dataProcessing" in x["data"] else None)
    df["processors"] = df["dataProcessing"].apply(lambda x: x["processors"] if x and "processors" in x else None)
    df_dp = df[["behandlingsId", "time", "aktivObservasjon", "processors"]]
    df_dp = df_dp.explode("processors") # <- Tabell klar til å skrives
    df_dict["stage_bridge_databehandler_behandling"] = df_dp

    # Vi trenger en egen tabell som viser behandlinsgrunnlag
    #df["legalBases"] = df["data"]


    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")

    project = "teamdatajegerne-prod-c8b1"
    dataset = "behandlinger"
    for table in df_dict:
        table_id = f"{project}.{dataset}.{table}"
        job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_dict[table], table_id, job_config=job_config)

    return None