import json

import google.auth
from google.cloud import bigquery
from google.cloud.bigquery.external_config import ExternalSourceFormat


class GSheetTable:
    def __init__(
        self,
        sheet_id: str,
        tab_name: str,
        schema_path: str
    ):
        self.sheet_id = sheet_id
        self.tab_name = tab_name
        self.schema_path = schema_path
        self.url = f'https://docs.google.com/spreadsheets/d/{sheet_id}'

    def get_schema(self) -> list[dict]:
        with open(self.schema_path, 'r') as f:
            schema_str = f.read()
        return json.loads(schema_str)

    def _get_bq_external_config(self) -> bigquery.ExternalConfig:
        ext_config = bigquery.ExternalConfig(ExternalSourceFormat.GOOGLE_SHEETS)
        ext_config.source_uris = [self.url]
        ext_config.google_sheets_options.skip_leading_rows = 1
        ext_config.google_sheets_options.range = self.tab_name
        return ext_config

    def _get_bq_schema(self) -> list[bigquery.SchemaField]:
        return [
            bigquery.SchemaField(
                field.get('name'),
                field.get('type'),
            )
            for field in self.get_schema()
        ]

    def get_bq_table(
            self,
            full_table_id: str
    ) -> bigquery.Table:
        schema = self._get_bq_schema()
        external_config = self._get_bq_external_config()

        bq_table = bigquery.Table(full_table_id, schema=schema)
        bq_table.external_data_configuration = external_config
        return bq_table


class BigQuery:
    SCOPES = [
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive',
    ]

    def __init__(self):
        credentials, _ = google.auth.default(scopes=self.SCOPES)
        self.client = bigquery.Client(credentials=credentials)

    def create_bq_table_from_gsheet_table(
            self,
            gsheet_table: GSheetTable,
            full_table_id: str,
            recreate_if_exists: bool
    ) -> None:
        if recreate_if_exists:
            self.client.delete_table(full_table_id, not_found_ok=True)

        bq_table = gsheet_table.get_bq_table(full_table_id)
        bq_table = self.client.create_table(bq_table, exists_ok=True)
        print(f'Created {bq_table.full_table_id}')

    def create_bq_table(
            self,
            full_table_id: str,
            schema: list[bigquery.SchemaField],
            partition_field: str | None = None,
    ) -> None:
        bq_table = bigquery.Table(full_table_id, schema=schema)
        if partition_field:
            bq_table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
        self.client.create_table(bq_table, exists_ok=True)
        print(f'Created {full_table_id}')

    def insert_override(
            self,
            source_table_id: str,
            dest_table_id: str,
            partition_field: str,
    ) -> None:
        delete_query = f"""
            DELETE FROM `{dest_table_id}`
            WHERE {partition_field} IN (
                SELECT DISTINCT {partition_field} FROM `{source_table_id}`
            )
        """
        self.client.query(delete_query).result()

        insert_query = f"""
            INSERT INTO `{dest_table_id}`
            SELECT * FROM `{source_table_id}`
            WHERE date IS NOT NULL
        """
        self.client.query(insert_query).result()
        print(f'Ingested data from {source_table_id} into {dest_table_id}')

    def get_client(self) -> bigquery.Client:
        return self.client
