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
    df_pol = df[["behandlingsId", "policies", "time", "aktivObservasjon"]].explode("policies")
    df_pol["policyId"] = df_pol["policies"].apply(lambda x: x["id"] if pd.notnull(x) and "id" in x else None)
    df_dict["stage_bridge_policy_behandling"] = df_pol[["behandlingsId", "time", "aktivObservasjon", "policyId"]]

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


def run_etl_information_types():
    sql = "SELECT * FROM `teamdatajegerne-prod-c8b1.behandlingskatalogen.audit_version_raw` order by time desc"
    df = pandas_gbq.read_gbq(sql, "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    df_dict = {} # Skal ta vare på to tabeller denne gangen

    # Finner gjeldende observasjon
    df["maxTime"] = df.groupby("table_id")["time"].transform("max")
    df["aktivObservasjon"] = False
    df.loc[df["time"] == df["maxTime"], "aktivObservasjon"] = True
    df = df[df["aktivObservasjon"]].copy()

    # Enklere å jobbe med json-bloben
    df["data"] = df["data"].apply(lambda x: json.loads(x))

    # Først policy: Information types per behandling <- Trenger ikke å hente ut document faktisk fordi informationTypes er allerede der digg!
    df_pol = df[df["table_name"] == "POLICY"].copy()
    df_pol.rename({"table_id": "policyId"}, axis=1, inplace=True)
    df_pol["informationTypeId"] = df_pol["data"].apply(lambda x: x["informationTypeId"])
    df_pol["subjectCategories"] = df_pol["data"].apply(lambda x: x["data"]["subjectCategories"] if "data" in x else None)
    df_pol["documentIds"] = df_pol["data"].apply(lambda x: x["data"]["documentIds"] if "data" in x else None) # strukturendring langt tilbake i tid

    df_dict["stage_policy"] = df_pol

    # Så informationtypes (opplysningstyper)
    df_info = df[df["table_name"] == "INFORMATION_TYPE"].copy()
    df_info.rename({"table_id": "informationTypeId"}, axis=1, inplace=True)
    cols_to_unpack = ["name", "sources", "categories", "description", "sensitivity", "productTeams"]
    for col in cols_to_unpack:
        df_info[col] = df_info["data"].apply(lambda x: x["data"][col] if col in x["data"] else None)

    df_info = df_info[["informationTypeId"] + cols_to_unpack]
    df_dict["stage_informationType"] = df_info

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")

    project = "teamdatajegerne-prod-c8b1"
    dataset = "behandlinger"
    for table in df_dict:
        table_id = f"{project}.{dataset}.{table}"
        job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_dict[table], table_id, job_config=job_config)

    return None






