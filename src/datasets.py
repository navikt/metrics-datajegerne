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


def run_etl_datasett_beskrivelser():
    # Henter inn data
    df = pandas_gbq.read_gbq("SELECT etterlevelseDokumentasjonId, beskrivelse, time, aktivRad FROM `teamdatajegerne-prod-c8b1.etterlevelse.stage_dokument`", "teamdatajegerne-prod-c8b1")

    # Merker dokumenter basert på om de har en beskrivelse eller ikke
    df["harBeskrivelse"] = False
    df.loc[(~df["beskrivelse"].isnull()) & (df["beskrivelse"].str.len() > 0), "harBeskrivelse"] = True

    # Finner tidspunktet da dokumentet først tok i bruk prioritert kravliste
    df["minTid"] = df.groupby(["etterlevelseDokumentasjonId", "harBeskrivelse"])["time"].transform(np.min)

    # Beholder kun gjeldende observasjon og de som faktisk bruker featuren
    df = df.query("aktivRad == True and harBeskrivelse == True").copy()

    # Finner antall dokumenter som bruker featuren plottet over tid
    df.sort_values(by="minTid", ascending=True, inplace=True)
    df["antallDokumenterMedBeskrivelset"] = df["minTid"].rank()

    # Finner også antall prioriterte krav *i dag* for dokumenter som bruker denne featuren
    df["antallTegnBeskrivelse"] = df["beskrivelse"].str.len()

    # Skriver til BQ
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")

    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "ds_beskrivelser"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None


def run_etl_datasett_risikoeier():
    return None


def run_etl_datasett_prioritertlist():
    df = pandas_gbq.read_gbq("SELECT etterlevelseDokumentasjonId, prioritertKravNummer, time, aktivRad FROM `teamdatajegerne-prod-c8b1.etterlevelse.stage_dokument`", "teamdatajegerne-prod-c8b1")

    df["harPrioritertKravNummer"] = False
    df.loc[(~df["prioritertKravNummer"].isnull()) & (df["prioritertKravNummer"].apply(lambda x: len(x)) > 0), "harPrioritertKravNummer"] = True

    # Finner tidspunktet da dokumentet først tok i bruk prioritert kravliste
    df["minTid"] = df.groupby(["etterlevelseDokumentasjonId", "harPrioritertKravNummer"])["time"].transform(np.min)

    # Beholder kun gjeldende observasjon og de som faktisk bruker featuren
    df = df.query("aktivRad == True and harPrioritertKravNummer == True").copy()

    # Finner antall dokumenter som bruker featuren plottet over tid
    df.sort_values(by="minTid", ascending=True, inplace=True)
    df["antallDokumenterMedPrioritertKravlist"] = df["minTid"].rank()

    # Finner også antall prioriterte krav *i dag* for dokumenter som bruker denne featuren
    df["antallPrioriterteKrav"] = df["prioritertKravNummer"].apply(lambda x: len(x))

    # Skriver til BQ
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")

    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "ds_prioriterte_krav"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None

def run_etl_datasett_mordokumenter():
    return None
