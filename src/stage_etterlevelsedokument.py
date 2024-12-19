from datetime import datetime
import json

import pandas as pd
import numpy as np

import pandas_gbq

from google.cloud import bigquery

def run_etl_dokumenter():
    # Obs: Denne serien går bare tilbake til august 2023
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")
    # Konverterer stringen til json
    df["data"] = df["data"].apply(lambda x: json.loads(x))

    # Henter ut data fra json
    cols_to_keep = ["id", "title", "etterlevelseNummer", "teams", "irrelevansFor", "avdeling", "varslingsadresser", "behandlingIds", "beskrivelse", "prioritertKravNummer", "risikoeiere", "forGjenbruk", "gjenbrukBeskrivelse", "tilgjengeligForGjenbruk", "resources"]
    for col in cols_to_keep:
        if col == "id":
            df[col] = df["data"].apply(lambda x: x[col])
        else:
            df[col] = df["data"].apply(lambda x: x["data"][col] if col in x["data"] else None)

    # Beholder bare de vi har skikkelig lyst på
    cols_to_keep.append("time")
    df = df[cols_to_keep].copy()

    df.rename({"id": "etterlevelseDokumentasjonId"}, axis=1, inplace=True)

    # Finner hvilken rad som er den gjeldende
    df["dokumentSistOppdatert"] = df.groupby(["etterlevelseDokumentasjonId"])["time"].transform(np.max)
    df["aktivRad"] = False
    df.loc[df["dokumentSistOppdatert"] == df["time"], "aktivRad"] = True

    # Nærmer oss noe nå. Trenger også å finne ut hvilke krav som er relevante basert på dokumentegenskapene
    # Leser først inn krav-tabellen
    df_krav = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw_krav`", "teamdatajegerne-prod-c8b1") # Kun aktive krav -- utgåtte krav vises i etterlevelsesløsningen, men er ikke med her

    # Hello there, json as string
    df_krav["data"] = df_krav["data"].apply(lambda x: json.loads(x))

    # ...og pakker ut
    cols_to_keep = ["kravNummer", "relevansFor"]
    for col in cols_to_keep:
        df_krav[col] = df_krav["data"].apply(lambda x: x[col])

    # Trenger en rad per kategori som kravene er relevante for. De som er relevante for alle kategorier ligger inne med nan
    df_krav = df_krav[cols_to_keep].copy()
    df_krav = df_krav.explode("relevansFor")
    df_krav["help"] = 1 # Trenger denne for å kunne merge med dokumentegenskaper

    # ...henter opp igjen tabellen med dokumentegenskaper
    cols_to_keep = ["etterlevelseDokumentasjonId", "time", "irrelevansFor"]
    df_irrelevansFor = df[cols_to_keep].copy()
    df_irrelevansFor["help"] = 1 # Trenger denne for å merge med krav-tabellen, se over

    # Merger inn og ender opp med en tabell som er len(df_irrelevansFor) x len(df_krav)
    df_krav = df_irrelevansFor.merge(df_krav, on="help", how="left")

    #...og da kan vi filtrere
    # Fra dokumentegenskaper vet vi hvilke kategorier som er irrelevante (irrelevansFor).
    # Fra krav-tabellen vet vi hvilke kategorier kravene er relevante for.
    # De kravene som er relevante for en kategori og som eksisterer i listen over irrelevante kategorier knyttet til et deokument, merkes med True
    df_krav["drop_rad"] = df_krav.apply(lambda x: True if x["relevansFor"] in x["irrelevansFor"] else False, axis=1)

    # Og da er det jo greit!
    df_krav = df_krav[df_krav["drop_rad"] == False]
    # Vi må også fjerne duplikater siden krav kan være relevante for flere kategorier
    df_krav.drop_duplicates(subset=["etterlevelseDokumentasjonId", "time", "kravNummer"], inplace=True)

    # Så gjør vi det motsatte av pd.explode: Vi imploderer elementene inn i en liste. Går fra stor tabell til mindre tabell
    df_krav = df_krav.groupby(["etterlevelseDokumentasjonId", "time"])["kravNummer"].agg(lambda x: x.tolist()).reset_index(name="relevanteKrav")

    # Teller opp
    df_krav["antallKrav"] = df_krav["relevanteKrav"].apply(lambda x: len(x))

    #... og merger så vi har relevante krav og antall relevante krav som egenskaper knyttet til dokumentet, på lik linje med andre egenskaper
    df = df.merge(df_krav, on=["etterlevelseDokumentasjonId", "time"])

    # Merker også hvilke dokumenter som er slettet
    sql = "select * from `teamdatajegerne-prod-c8b1.metrics.raw_generic_storage` where type = 'EtterlevelseDokumentasjon'"
    df_gs = pandas_gbq.read_gbq(sql, "teamdatajegerne-prod-c8b1")
    etterlevelseDokumentasjonIdIkkeSlettet = df_gs["id"]
    df["slettet"] = False
    df.loc[~df["etterlevelseDokumentasjonId"].isin(etterlevelseDokumentasjonIdIkkeSlettet), "slettet"] = True

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "etterlevelse"
    table = "stage_dokument"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)