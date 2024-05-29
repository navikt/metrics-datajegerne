import json
import pandas as pd
import pandas_gbq

from google.cloud import bigquery

def run_etl_prioriterte_krav():
    # Leser audit-data
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    # Pakker ut json-blob
    df_mother = pd.DataFrame()
    for i, rows in df.iterrows():
        timestamp = [rows["time"]]
        user = [rows["user"]]
        # for alt i json-bloben
        data = json.loads(rows["data"])["data"]
        teams = data["teams"] if "teams" in data.keys() else [None]
        etterlevelseDokumentId = [data["id"]]
        prioritertList = data["prioritertKravNummer"] if "prioritertKravNummer" in data.keys() else None

        df_temp = pd.DataFrame({"timestamp": timestamp,
                                "etterlevelseDokument": etterlevelseDokumentId,
                                "user": user})
        df_temp["prioritertList"] = [prioritertList]
        df_temp["teams"] = [teams]

        df_mother = pd.concat([df_mother, df_temp])

    # En rad per krav i prioritert liste
    df_mother = df_mother.explode("prioritertList")

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "prioriterte_krav"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df_mother, table_id, job_config=job_config)

    return None

