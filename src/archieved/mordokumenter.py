import json
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_gbq

from google.cloud import bigquery

def run_etl_mordokumenter():
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")

    # Henter variabler fra json-blob
    variables = ["title", "etterlevelseNummer", "forGjenbruk", "gjenbrukBeskrivelse", "tilgjengeligForGjenbruk"]
    dict_var = {var: [] for var in variables}
    for i, rows in df.iterrows():
        data = json.loads(rows["data"])["data"]
        for col in variables:
            if col in data.keys():
                temp_var = data[col]
            else:
                temp_var = None
            dict_var[col].append(temp_var)
    for col in variables:
        df[col] = dict_var[col]

    # Tar vekk gamle observasjoner
    df = df[df["time"] > "2023-09-30"]

    # Beholder bare dokumenter som enten er tilrettelegges for eller har tidligere blitt tilrettelagt for gjenbruk
    etterlevelseNummerGjenbruk = df[df["forGjenbruk"] == True]["etterlevelseNummer"].unique()
    df = df[df["etterlevelseNummer"].isin(etterlevelseNummerGjenbruk)]

    # Og merker aktive observasjoner siden vi har full historikk
    df["aktiv"] = False
    df["timeMax"] = df.groupby("etterlevelseNummer")["time"].transform("max")
    df.loc[df["time"] == df["timeMax"], "aktiv"] = True

    #...og vi skal kun ha med enkelte variabler
    var_list = ["title", "etterlevelseNummer", "forGjenbruk", "gjenbrukBeskrivelse", "tilgjengeligForGjenbruk", "time", "aktiv"]
    df = df[var_list]

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "mordokumenter"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    return None


if __name__ == "__main__":
    run_etl_mordokumenter()

