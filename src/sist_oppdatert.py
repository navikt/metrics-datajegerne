import json
import datetime

import pandas as pd
import pandas_gbq
from google.cloud import bigquery

def run_etl_sist_oppdatert():
    # Leser fra audit version i etterlevelse
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'Etterlevelse'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    # Itererer over json-blob
    df_mother = pd.DataFrame()
    for i, rows in df.iterrows():
        data = rows["data"]
        data = json.loads(data)["data"]
        if "etterlevelseDokumentasjonId" in data.keys():
            etterlevelsesDokumentasjonId = data["etterlevelseDokumentasjonId"]
        elif "behandlingId" in data.keys():
            etterlevelsesDokumentasjonId = data["behandlingId"]
        else:
            print("Ingen id at all -- fuckin` hell mate!")

        timeStamp = rows["time"]
        df_temp = pd.DataFrame({"etterlevelseDokumentasjonId": [etterlevelsesDokumentasjonId],
                                "lastModified": timeStamp})
        df_mother = pd.concat([df_mother, df_temp])

    df_mother["lastModified"] = pd.to_datetime(df_mother["lastModified"])

    # Lager rangen så vi kan beregne metrikker per dato
    date_range = pd.date_range(df_mother["lastModified"].min(), datetime.datetime.today(), freq="d")

    # Beregner metrikker
    df_metrics = pd.DataFrame()
    for date in date_range:
        df_temp = df_mother[df_mother["lastModified"] <= date]
        df_temp = df_temp.groupby("etterlevelseDokumentasjonId")["lastModified"].max().reset_index()
        df_temp["deltaDager"] = (df_temp["lastModified"] - date).dt.days

        # Må finne siste års dato for å kunne beregne metrikker de siste tolv månedene
        date_last_year = date - datetime.timedelta(days=365)

        antall = df_temp["deltaDager"].count()
        median = abs(df_temp["deltaDager"].median())
        average = abs(df_temp["deltaDager"].mean())

        # Bare siste 12 måneder
        antallLastYear = abs(df_temp.loc[df_temp["lastModified"] > date_last_year, "deltaDager"].count())
        medianLastYear = abs(df_temp.loc[df_temp["lastModified"] > date_last_year, "deltaDager"].median())
        averageLastYear = abs(df_temp.loc[df_temp["lastModified"] > date_last_year, "deltaDager"].mean())



        df_temp = pd.DataFrame([{"dato": date,
                                 "antall": antall,
                                 "median": median,
                                 "average": average,
                                 "antallSiste12mnd": antallLastYear,
                                 "medianSiste12mnd": medianLastYear,
                                 "averageSiste12mnd": averageLastYear}])

        df_metrics = pd.concat([df_metrics, df_temp])

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "sist_oppdatert"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df_mother, table_id, job_config=job_config)

    return None
