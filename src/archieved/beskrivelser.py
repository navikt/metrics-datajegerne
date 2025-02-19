import json
from datetime import datetime

import pandas as pd
import pandas_gbq

from google.cloud import bigquery

def run_etl_beskrivelser():
    # Leser audit-data
    df = pandas_gbq.read_gbq("SELECT * FROM `teamdatajegerne-prod-c8b1.landing_zone.etterlevelse_audit_version` where table_name = 'EtterlevelseDokumentasjon'", "teamdatajegerne-prod-c8b1")
    df.sort_values(by="time", ascending=False, inplace=True)

    # Pakker ut json-blob
    df_mother = pd.DataFrame()
    for i, rows in df.iterrows():
        data = rows["data"]
        data = json.loads(data)["data"]

        etterlevelsesDokumentasjonId = rows["table_id"]

        beskrivelse = True if "beskrivelse" in data.keys() else False
        if beskrivelse:
            beskrivelse_rapportert = data["beskrivelse"]
        else:
            beskrivelse_rapportert = None

        df_temp = pd.DataFrame({"etterlevelseDokumentasjonId": [etterlevelsesDokumentasjonId]})
        df_temp["beskrivelse"] = beskrivelse_rapportert

        df_mother = pd.concat([df_mother, df_temp])

    df_mother["version"] = datetime.now()

    # Skrive til BigQuery
    client = bigquery.Client(project="teamdatajegerne-prod-c8b1")


    project = "teamdatajegerne-prod-c8b1"
    dataset = "metrics"
    table = "beskrivelser"

    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.job.LoadJobConfig(write_disposition="WRITE_APPEND")
    job = client.load_table_from_dataframe(df_mother, table_id, job_config=job_config)

    return None

