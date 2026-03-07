import argparse

import google.auth
from google.cloud import bigquery


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env", choices=["dev", "prod"], required=True,
        help="Deployment environment"
    )

    args = parser.parse_args()
    project_id = "conda-cps-dev" if args.env == "dev" else "conda-cps-prod"
    full_table_id = f"{project_id}.external_gsheet.run_rate"

    credentials, _ = google.auth.default(
        scopes=[
            'https://www.googleapis.com/auth/bigquery',
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/drive',
        ]
    )
    client = bigquery.Client(credentials=credentials)

    # Create external table
    client.delete_table(full_table_id, not_found_ok=True)
    bq_table = bigquery.Table(
        full_table_id,
        schema=[
            bigquery.SchemaField("test", "STRING"),
        ],
    )

    ext_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
    ext_config.source_uris = ["https://docs.google.com/spreadsheets/d/14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ"]
    ext_config.options.skip_leading_rows = 1
    ext_config.options.range = "Cost runrate!A2:AF71"
    bq_table.external_data_configuration = ext_config
    client.create_table(bq_table)
    print(f"External table {full_table_id} created successfully.")


if __name__ == "__main__":
    main()
