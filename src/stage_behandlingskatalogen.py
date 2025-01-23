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


    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")

    project = "teamdatajegerne-prod-c8b1"
    dataset = "behandlinger"
    for table in df_dict:
        table_id = f"{project}.{dataset}.{table}"
        job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_dict[table], table_id, job_config=job_config)

    return None


def run_etl_legal_bases():
    sql = "SELECT * FROM `teamdatajegerne-prod-c8b1.behandlingskatalogen.audit_version_raw` order by time desc"
    df_raw = pandas_gbq.read_gbq(sql, "teamdatajegerne-prod-c8b1", progress_bar_type=None)
    df_raw["data"] = df_raw["data"].apply(lambda x: json.loads(x))

    # Finner gjeldende observasjon
    df_raw["maxTime"] = df_raw.groupby("table_id")["time"].transform("max")
    df_raw["aktivObservasjon"] = False
    df_raw.loc[df_raw["time"] == df_raw["maxTime"], "aktivObservasjon"] = True

    # Filtrerer ned tabllen
    df = df_raw[df_raw["table_name"] == "PROCESS"].copy()

    # Renamer litt
    df.rename({"table_id": "behandlingsId"}, axis=1, inplace=True)


    # Graver ut behandlinsgrunnlagene
    df["legalBases"] = df["data"].apply(lambda x: x["data"]["legalBases"] if "legalBases" in x["data"] else None)

    df = df[["time", "aktivObservasjon", "behandlingsId", "action", "legalBases"]].copy()

    # PÅ noe om slettet?

    df = df.explode("legalBases")

    for col in ["gdpr", "description", "nationalLaw"]:
        df[col] = df["legalBases"].apply(lambda x: x[col] if pd.notnull(x) and col in x else None)
    df.rename({"description": "descriptionUserNationalLaw"}, axis=1, inplace=True)
    df.drop("legalBases", axis=1, inplace=True)

    # Kobler inn mer informasjon om behandlingsgrunnlagene: Fra codelist
    df_code = df_raw[df_raw["table_name"] == "CODELIST"].copy()
    print(len(df_code))
    cols_to_unpack = ["list", "code", "shortName", "description"]
    for col in cols_to_unpack:
        df_code[col] = df_code["data"].apply(lambda x: x[col])
    df_code = df_code[(df_code["list"].isin(["GDPR_ARTICLE", "NATIONAL_LAW"])) & (df_code["aktivObservasjon"])].copy()
    print(len(df_code))

    df_code = df_code[cols_to_unpack].copy()
    df_code.drop("list", axis=1, inplace=True)

    # Merger først inn info om GDPR
    print(df["gdpr"].unique())
    print(df_code["code"].unique())
    df = df.merge(df_code, how="left", left_on="gdpr", right_on="code")
    df.rename({"shortName": "shortNameGDPR", "description": "descriptionGDPR"}, axis=1, inplace=True)
    df.drop("code", axis=1, inplace=True)

    #...så nasjonalt lovverk
    df = df.merge(df_code, how="left", left_on="nationalLaw", right_on="code")
    df.rename({"shortName": "shortNameNationalLaw", "description": "descriptionNationalLaw"}, axis=1, inplace=True)
    df.drop("code", axis=1, inplace=True)

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")

    project = "teamdatajegerne-prod-c8b1"
    dataset = "behandlinger"
    table = "stage_behandlingsgrunnlag"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

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


def run_etl_dataprocessors():
    return None






