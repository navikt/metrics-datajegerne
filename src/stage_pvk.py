from datetime import datetime
import json

import pandas as pd
import numpy as np

import pandas_gbq

from google.cloud import bigquery


def run_etl_pvk():
    # Leser data
    sql = "SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name in ('TILTAK', 'RISIKOSCENARIO', 'PVK_DOKUMENT')"
    df = pandas_gbq.read_gbq(sql, "teamdatajegerne-prod-c8b1", progress_bar_type=None)
    df["data"] = df["data"].apply(lambda x: json.loads(x))
    df["maxTime"] = df.groupby("table_id")["time"].transform("max")

    # Tiltak
    df_tiltak = df[df["table_name"] == "TILTAK"].copy()
    cols_to_keep = ["navn", "frist", "ansvarlig", "beskrivelse"]
    for col in cols_to_keep:
        df_tiltak[col] = df_tiltak["data"].apply(lambda x: x["tiltakData"][col])
    df_tiltak["pvkDokumentId"] = df_tiltak["data"].apply(lambda x: x["pvkDokumentId"])
    cols_to_keep.append("pvkDokumentId")
    cols_to_keep.append("time")
    cols_to_keep.append("table_id")

    df_tiltak["aktivRad"] = False
    df_tiltak.loc[df_tiltak["time"] == df_tiltak["maxTime"], "aktivRad"] = True

    cols_to_keep.append("aktivRad")

    df_tiltak = df_tiltak[cols_to_keep].copy()

    df_tiltak.rename({"table_id": "tiltakId"}, axis=1, inplace=True)

    # PVK-dokument
    df_pvk = df[df["table_name"] == "PVK_DOKUMENT"].copy()
    cols_to_keep = list(df_pvk["data"].values[0]["pvkDokumentData"].keys())
    for col in cols_to_keep:
        df_pvk[col] = df_pvk["data"].apply(lambda x: x["pvkDokumentData"][col])
    df_pvk["etterlevelseDokumentId"] = df_pvk["data"].apply(lambda x: x["etterlevelseDokumentId"])
    df_pvk["status"] = df_pvk["data"].apply(lambda x: x["status"])
    for col in ["etterlevelseDokumentId", "time", "table_id", "status", "aktivRad"]:
        cols_to_keep.append(col)

    df_pvk["aktivRad"] = False
    df_pvk.loc[df_pvk["time"] == df_pvk["maxTime"], "aktivRad"] = True

    df_pvk = df_pvk[cols_to_keep].copy()

    df_pvk.rename({"table_id": "pvkDokumentId"}, axis=1, inplace=True)

    # Risikoscenarioer
    df_risk = df[df["table_name"] == "RISIKOSCENARIO"].copy()

    cols_to_keep = list(df_risk["data"].values[0]["risikoscenarioData"].keys())
    for col in cols_to_keep:
        df_risk[col] = df_risk["data"].apply(lambda x: x["risikoscenarioData"][col])

    df_risk["pvkDokumentId"] = df_risk["data"].apply(lambda x: x["pvkDokumentId"])

    for col in ["pvkDokumentId", "time", "table_id", "aktivRad"]:
        cols_to_keep.append(col)

    df_risk["aktivRad"] = False
    df_risk.loc[df_risk["time"] == df_risk["maxTime"], "aktivRad"] = True

    df_risk = df_risk[cols_to_keep].copy()

    df_risk.rename({"table_id": "risikoscenarioId"}, axis=1, inplace=True)


    table_dict = {"stage_pvk_dokument": df_pvk,
                      "stage_risikoscenario": df_risk,
                      "stage_tiltak": df_tiltak}

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")
    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"

    for table in table_dict:
        table_id = f"{project}.{dataset}.{table}"
        job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(table_dict[table], table_id, job_config=job_config)