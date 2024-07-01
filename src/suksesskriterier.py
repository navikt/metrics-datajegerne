import json
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_gbq
from google.cloud import bigquery

def run_etl_suksesskriterier():
    # Starter med krav: Trenger dette for å beregne hva som er relevante krav
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw_krav`", "teamdatajegerne-prod-c8b1") # Kun aktive krav -- utgåtte krav vises i etterlevelsesløsningen, men er ikke med her

    # Graver ut Json og ekspanderer
    df["relevansFor"] = df["data"].apply(lambda x: json.loads(x)["relevansFor"])
    df["kravNummer"] = df["data"].apply(lambda x: json.loads(x)["kravNummer"])
    df["kravVersjon"] = df["data"].apply(lambda x: json.loads(x)["kravVersjon"])

    # Må hente ut data per suksesskriterium og
    list_id = []
    list_begrunnelser = []
    for i, rows in df.iterrows():
        items = json.loads(rows["data"])["suksesskriterier"]
        list_id.append([item["id"] for item in items])
        list_begrunnelser.append([item["behovForBegrunnelse"] for item in items])
    df["suksesskriterieId"] = list_id
    df["behovForBegrunnelse"] = list_begrunnelser

    df.drop("data", axis=1, inplace=True)
    df_krav = df.explode("relevansFor")
    df_krav.loc[df_krav["relevansFor"].isnull(), "relevansFor"] = "ALLE"
    df_krav = df_krav.explode(["suksesskriterieId", "behovForBegrunnelse"])
    df_krav.drop_duplicates(inplace=True)

    # Trenger å knytte krav til tema
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.krav_tema`", "teamdatajegerne-prod-c8b1")
    df_krav = df_krav.merge(df[["kravNummer", "tema"]].drop_duplicates(), on="kravNummer", how="left")
    df_krav["aktivVersjon"] = True

    # Da har vi krav på plass! Vi graver ut litt metadata om etterlevelse
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    # Må grave ut fra json-strukturen
    df["etterlevelseNummer"] = df["data"].apply(lambda x: json.loads(x)["data"]["etterlevelseNummer"])
    df["irrelevansFor"] = df["data"].apply(lambda x: json.loads(x)["data"]["irrelevansFor"])
    df["tittel"] = df["data"].apply(lambda x: json.loads(x)["data"]["title"])

    array_avdeling = []
    for i, rows in df.iterrows():
        item = json.loads(rows["data"])["data"]
        avdeling = item["avdeling"] if "avdeling" in item.keys() else None
        array_avdeling.append(avdeling)

    df["avdeling"] = array_avdeling

    # Beholder gjeldende observasjon
    df["sist_oppdatert"] = df.groupby("table_id")["time"].transform("max") # Beholder bare gjeldende observasjoner
    df = df[df["sist_oppdatert"] == df["time"]]

    # Beholder bare kolonner vi skal ha med videre
    df = df[["table_id", "irrelevansFor", "etterlevelseNummer", "tittel", "avdeling", "sist_oppdatert"]].copy()
    df.rename({"table_id": "etterlevelseDokumentasjonId"}, axis=1, inplace=True)

    # Merger informasjon om etterlevelsesdokumenter med informasjon om krav slik at vi får en oversikt over relevante krav per etterlevelsesdokument
    df["help"] = 1
    df_krav["help"] = 1
    df_merged = df.merge(df_krav, on="help", how="outer")

    # Her gjør vi filtreringen
    df_merged["drop_rad"] = df_merged.apply(lambda x: True if x["relevansFor"] in x["irrelevansFor"] else False, axis=1)
    df_relevante_krav = df_merged[df_merged["drop_rad"] == False].copy()
    df_relevante_krav.drop(["drop_rad", "help"], axis=1, inplace=True)


    # Og så henter vi ut besvarelsene på hvert enkelt suksesskriterie og. Må først gjøre en mapping fra behanldingsId til etterlevelsesDokumentasjonId for "gamle" dokumenter
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    df["created_by"] = df["data"].apply(lambda x: json.loads(x)["createdBy"]) # Trenger created by for å kunne identifisere da vi migrerte data i forbindelse med "arkiktekturskifte" i august 2023

    # må mappe behandlings-ider til etterlevelses-ider fordi det er brudd i serien august 2023.
    behandling_etterlevelse_mapping = {}
    ikke_match = []
    for i, rows in df[df["created_by"] == "MIGRATION(ADMIN)"].iterrows():
        item = json.loads(rows["data"])
        if len(item["data"]["behandlingIds"]) > 0:
            behandlingsId = item["data"]["behandlingIds"][0]
            behandling_etterlevelse_mapping[behandlingsId] = item["id"]
        else:
            ikke_match.append(item["id"])

    # Leser besvarelser på suksesskriterier her
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'Etterlevelse'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    # Graver ut fra json her
    df["kravNummer"] = df["data"].apply(lambda x: json.loads(x)["data"]["kravNummer"])
    df["kravVersjon"] = df["data"].apply(lambda x: json.loads(x)["data"]["kravVersjon"])

    # Og til de litt mer kronglete elementene i json
    def get_value_from_data_dict(x, key):
        item = json.loads(x)["data"]
        if key in item.keys():
            return item[key]
        else:
            return None

    for column in ["etterlevelseDokumentasjonId", "behandlingId", "suksesskriterieBegrunnelser"]:
        df[column] = df["data"].apply(lambda x: get_value_from_data_dict(x, column))

    # Trenger å mappe inn id-er for besvarelser gjort før august 2023 (der er etterlevelseDokumentasjonId null)
    df.loc[df["etterlevelseDokumentasjonId"].isnull(), "etterlevelseDokumentasjonId"] = df.loc[df["etterlevelseDokumentasjonId"].isnull(), "behandlingId"].map(behandling_etterlevelse_mapping)

    df = df[["etterlevelseDokumentasjonId", "time", "kravNummer", "kravVersjon", "suksesskriterieBegrunnelser"]].copy()

    # Og så må vi grave ut det vi kan fra suksesskriteriene... Brudd i serien her. Tidligere var oppfylt en boolean, og ja
    status_array = []
    id_array = []
    begrunnelse_array = []
    for besvarelse in df["suksesskriterieBegrunnelser"]:
        temp_status_array = []
        temp_id_array = [item["suksesskriterieId"] for item in besvarelse]
        temp_begrunnelse_array = [item["begrunnelse"] for item in besvarelse]
        for item in besvarelse:
            if "suksesskriterieStatus" in item.keys():
                temp_status_array.append(item["suksesskriterieStatus"])

            elif "oppfylt" in item.keys():
                if item["oppfylt"] == True:
                    temp_status_array.append("OPPFYLT")
                elif "underArbeid" in item.keys() and item["underArbeid"] == True:
                    temp_status_array.append("UNDER_ARBEID")
                elif "ikkeRelevant" in item.keys() and item["ikkeRelevant"] == True:
                    temp_status_array.append("IKKE_RELEVANT")
                else:
                    temp_status_array.append("IKKE_OPPFYLT")


        id_array.append(temp_id_array)
        begrunnelse_array.append(temp_begrunnelse_array)
        status_array.append(temp_status_array)

    df["suksesskriterieId"] = id_array
    df["statusKriterium"] = status_array
    df["begrunnelse"] = begrunnelse_array

    # Trenger å beholde gjeldende observasjon per suksesskriterium
    df["lastUpdated"] = df.groupby(["etterlevelseDokumentasjonId", "kravNummer", "kravVersjon"])["time"].transform(max)
    df = df[df["time"] == df["lastUpdated"]].copy()
    df.drop("suksesskriterieBegrunnelser", axis=1, inplace=True)

    # Avleder hva som er oppfylt og hva som er ferdig utfylt
    df["kravOppfylt"] = df["statusKriterium"].apply(lambda x: True if all([True if item in ["OPPFYLT", "IKKE_RELEVANT"] else False for item in x]) else False)
    df["kravFerdigUtfylt"] = df["statusKriterium"].apply(lambda x: True if all([True if item in ["OPPFYLT", "IKKE_RELEVANT", "IKKE_OPPFYLT"] else False for item in x]) else False)

    # Ekspanderer så vi har en linje per suksesskriterie
    df = df.explode(["suksesskriterieId", "statusKriterium", "begrunnelse"]).copy()

    # Merger besvarelsene med oversikt over relevante krav
    df_merged = df[["etterlevelseDokumentasjonId", "kravNummer", "kravVersjon", "kravOppfylt", "kravFerdigUtfylt", "suksesskriterieId", "statusKriterium", "begrunnelse", "lastUpdated"]].merge(df_relevante_krav[["etterlevelseDokumentasjonId", "kravNummer", "kravVersjon", "suksesskriterieId", "behovForBegrunnelse", "aktivVersjon"]].drop_duplicates(), on=["etterlevelseDokumentasjonId", "kravNummer", "kravVersjon", "suksesskriterieId"], how="outer")
    df_merged = df_merged.merge(df_relevante_krav[["etterlevelseDokumentasjonId", "etterlevelseNummer", "tittel", "avdeling"]].drop_duplicates(), on="etterlevelseDokumentasjonId", how="outer")
    df_merged = df_merged.merge(df_relevante_krav[["kravNummer", "tema"]].drop_duplicates(), on="kravNummer")

    # Er en del missing etter merge
    df_merged.loc[df_merged["kravOppfylt"].isnull(), "kravOppfylt"] = False
    df_merged.loc[df_merged["kravFerdigUtfylt"].isnull(), "kravFerdigUtfylt"] = False
    df_merged.loc[df_merged["aktivVersjon"].isnull(), "aktivVersjon"] = False



    # Teller antall tegn per besvarelse
    df_merged["begrunnelseAntallTegn"] = None
    df_merged.loc[df_merged["begrunnelse"].notnull(), "begrunnelseAntallTegn"] = df_merged.loc[df_merged["begrunnelse"].notnull(), "begrunnelse"].apply(lambda x: len(x))

    df_merged["version"] = datetime.now()

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "suksesskriterier"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df_merged, table_id, job_config=job_config)

    return None