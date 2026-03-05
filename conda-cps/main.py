import google.auth
from google.auth import impersonated_credentials
from google.cloud import bigquery


def main():
    source_creds, _ = google.auth.default()
    impersonated_creds = impersonated_credentials.Credentials(
        source_credentials=source_creds,
        target_principal='sa-cloudrun@conda-cps-dev.iam.gserviceaccount.com',
        target_scopes=[
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/drive',
        ]
    )

    client = bigquery.Client(credentials=impersonated_creds)
    query_job = client.query("SELECT 'Hello, World!' AS greeting")
    result = query_job.result()
    for row in result:
        print(row['greeting'])
