import json
from datetime import datetime

import pandas as pd
import pandas_gbq

from google.cloud import bigquery

def run_etl_alerts():
    # Leser audit-data
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.metrics.raw` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    # Pakker ut json-blob
    df_mother = pd.DataFrame()
    for i, rows in df.iterrows():
        data = rows["data"]
        data = json.loads(data)["data"]
        etterlevelsesDokumentasjonId = rows["table_id"]

        len_df = 1
        varsling = True if "varslingsadresser" in data.keys() and data["varslingsadresser"] is not None else False
        if varsling:
            varslingsadresser = data["varslingsadresser"]
            if varslingsadresser:
                varsel_type = [item["type"] for item in varslingsadresser]
                varsel_adr = [item["adresse"] for item in varslingsadresser]
                len_df = len(varsel_type)


        df_temp = pd.DataFrame({"etterlevelseDokumentasjonId": [etterlevelsesDokumentasjonId] * len_df})
        if varsling:
            df_temp["varsel_type"] = varsel_type
            df_temp["varsel_adr"] = varsel_adr

        df_mother = pd.concat([df_mother, df_temp])

    df_mother["version"] = datetime.now()

    df_mother.drop_duplicates(inplace=True)

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "varslingsadresser"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df_mother, table_id, job_config=job_config)

    return None

if __name__ == "__main__":
    run_etl_alerts()

