import json

import pandas as pd
import numpy as np

import pandas_gbq
from google.cloud import bigquery


def run_etl_etterlevelsebesvarelse():
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")
    # Konverterer jsonb til dict
    df["data"] = df["data"].apply(lambda x: json.loads(x))

    # Må ta vare på de observasjonene som er fra migrering som ble gjort august 2023
    df["version"] = df["data"].apply(lambda x: x["version"])
    df["first_version"] = df.groupby("table_id")["version"].transform(np.min)
    df["createdBy"] = df["data"].apply(lambda x: x["createdBy"])
    df = df[(df["createdBy"] == "MIGRATION(ADMIN)") & (df["version"] == df["first_version"])].copy()

    # Trenger mapping mellom behandlingsId og etterlevelsesId
    df["behandlingId"] = df["data"].apply(lambda x: x["data"]["behandlingIds"])
    df["etterlevelseId"] = df["data"].apply(lambda x: x["id"])
    df = df[["behandlingId", "etterlevelseId"]]
    df = df.explode("behandlingId")
    mapping_dict = {behandlingId: etterlevelseId for behandlingId, etterlevelseId in df.values}

    # Må koble med besvarelsene
    # Første steg er å erstatte behandlingsID bakover i tid
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where lower(table_name) = 'etterlevelse'", "teamdatajegerne-prod-c8b1")
    df["data"] = df["data"].apply(lambda x: json.loads(x))

    for col in ["behandlingId", "etterlevelseDokumentasjonId"]:
        list_values = []
        for val in df["data"].values:
            if col in val.keys():
                list_values.append(val[col])
            elif "data" in val.keys():
                if col in val["data"].keys():
                    list_values.append(val["data"][col])
                else:
                    list_values.append(None)
            else:
                list_values.append(None)
        df[col] = list_values

    df.loc[df["etterlevelseDokumentasjonId"].isnull(), "etterlevelseDokumentasjonId"] = df.loc[df["etterlevelseDokumentasjonId"].isnull(), "behandlingId"].map(mapping_dict)

    # Da har vi håndtert brudd i serien for identifikatoren på etterlevelsesdokumentet.
    # Nå må vi håndtere bruddet i hvordan besvarelser om suksesskriterier har blitt lagret
    for col in ["suksesskriterieBegrunnelser", "kravNummer", "kravVersjon"]:
        list_values = []
        for val in df["data"].values:
            match = False
            if "data" in val.keys():
                if col in val["data"].keys():
                    list_values.append(val["data"][col])
                    match = True
            if "etterlevelseData" in val.keys(): # Brudd i oktober 2024
                if col in val["etterlevelseData"].keys():
                    list_values.append(val["etterlevelseData"][col])
                    match = True
            if not match:
                if col in val.keys():
                    list_values.append(val[col])
                else:
                    list_values.append(None)

        df[col] = list_values

        # Flere suksesskriterier per krav
    df = df.explode("suksesskriterieBegrunnelser")
    for col in ["begrunnelse", "suksesskriterieId", "suksesskriterieStatus"]:
        list_values = []
        for val in df["suksesskriterieBegrunnelser"].values:
            if pd.isnull(val):
                list_values.append(None)
            elif col in val:
                list_values.append(val[col])
            elif col == "suksesskriterieStatus":
                if "oppfylt" and "ikkeRelevant" in val.keys():
                    if val["oppfylt"] == True:
                        list_values.append("OPPFYLT")
                    elif val["ikkeRelevant"] == True:
                        list_values.append("IKKE_RELEVANT")
                    else:
                        list_values.append("UNDER_ARBEID")
                elif "oppfylt" in val.keys():
                    if val["oppfylt"] == True:
                        list_values.append("OPPFYLT")
                    else:
                        list_values.append("UNDER_ARBEID")

        df[col] = list_values

    # Fjerner en del kolonner
    cols_to_keep = ["etterlevelseDokumentasjonId", "kravNummer", "kravVersjon", "time", "suksesskriterieId", "suksesskriterieStatus", "begrunnelse"]
    df = df[cols_to_keep].copy()

    # Avleder om et krav er oppfylt eller ikke
    df["kravOppfylt"] = df.groupby(["etterlevelseDokumentasjonId", "kravNummer", "time"])["suksesskriterieStatus"].transform(lambda x: True if all([True if item == "OPPFYLT" else False for item in x ]) else False)
    #...og om kravet er ferdig utfylt
    df["kravFerdigUtfylt"] = df.groupby(["etterlevelseDokumentasjonId", "kravNummer", "time"])["suksesskriterieStatus"].transform(lambda x: True if all([True if item in ["OPPFYLT", "IKKE_RELEVANT"] else False for item in x ]) else False)

    #...og til slutt finner vi ut av om raden er gjeldende observasjon
    df["kravSistOppdatert"] = df.groupby(["etterlevelseDokumentasjonId", "kravNummer"])["time"].transform(np.max)
    df["aktivRad"] = False
    df.loc[df["kravSistOppdatert"] == df["time"], "aktivRad"] = True
    df.drop("kravSistOppdatert", axis=1, inplace=True)

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "stage_besvarelser"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None