import json
from datetime import datetime

import pandas as pd
import pandas_gbq

from google.cloud import bigquery

def run_etl_spoersmaal_og_svar():
    # Leser audit-data
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.landing_zone.etterlevelse_tilbakemelding`", "teamdatajegerne-prod-c8b1", progress_bar_type=None)

    # Pakker ut json-blob
    for col in ["melder", "meldinger", "status"]:
        df[col] = df["data"].apply(lambda x: json.loads(x)[col] if col in json.loads(x).keys() else None)

    for col in ["tid", "rolle", "meldingNr", "innhold"]:
        temp_list = []
        for rows in df["data"].values:
            item = json.loads(rows)
            meldinger = item["meldinger"]
            temp_list.append([element[col] for element in meldinger])
        df[col] = temp_list

    # Ekspanderer listene i kolonnene slik at har en observasjon per element i listene
    df = df.explode(["tid", "rolle", "innhold", "meldingNr"])
    df = df[["krav_nummer", "created_date", "status", "id", "tid", "rolle", "innhold", "meldingNr"]]
    df.rename({"krav_nummer": "kravNummer"}, axis=1, inplace=True)

    # Markerer hvilke meldinger som er siste i rekka
    df["sist_aktivitet"] = df.groupby("id")["tid"].transform("max")
    df["siste_melding"] = False
    df.loc[df["sist_aktivitet"] == df["tid"], "siste_melding"] = True

    # Beregner hvor lang tid som er brukt fra spørsmålet stilles til det er besvart
    df["tid"] = pd.to_datetime(df["tid"])
    df["tid_brukt"] = df["tid"] - df["created_date"]

    # Må beregne tid_brukt der hvor spørsmålet fortsatt er ubesvart
    df.loc[df["status"] == "UBESVART", "tid_brukt"] = datetime.now() - df.loc[df["status"] == "UBESVART", "created_date"]

    # Vil bare ha dager
    df["tid_brukt"] = df["tid_brukt"].dt.days

    # Må trixe pga denne bugen: https://stackoverflow.com/questions/59682833/pyarrow-lib-arrowinvalid-casting-from-timestampns-to-timestampms-would-los
    for col in ["tid", "created_date"]:
        df[col] = df[col].dt.floor("s")


    # Filtrerer vekk observasjoner fra før juni 2023 en gang
    timestamp_status_obligatorisk = df[df["status"].isnull()]["created_date"].max() # Har bare status for spørsmål som er behandlet etter juni 2023 en gang
    df = df[df["created_date"] > timestamp_status_obligatorisk]

    # Kobler på tema
    # Henter inn mer info om kravene og
    df_tema = pandas_gbq.read_gbq("SELECT distinct kravNummer, tema FROM `teamdatajegerne-prod-c8b1.landing_zone.etterlevelse_krav_tema`", "teamdatajegerne-prod-c8b1", progress_bar_type=None)
    df = df.merge(df_tema, on="kravNummer", how="outer")

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "stage_meldinger"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None

