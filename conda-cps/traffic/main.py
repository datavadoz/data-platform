import argparse
from dataclasses import dataclass
import json
import os
import sys
import time

import polars as pl
import requests
from google.cloud import bigquery

from toolbox.bigquery import BigQuery, GSheetTable

CUR_DIR = os.path.dirname(os.path.abspath(__file__))

GSHEET_ID = "14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ"
GSHEET_TAB = "Cost runrate!Z105:AR131"
SCHEMA_PATH = os.path.join(CUR_DIR, "schemas", "traffic.json")


@dataclass
class LarkConfig:
    app_id: str
    app_secret: str
    receiver_ids: list[str]

    @classmethod
    def from_env(cls) -> "LarkConfig":
        secret = os.environ.get("LARK_SECRET", "")
        if not secret:
            raise ValueError("LARK_SECRET environment variable is not set")

        data = json.loads(secret)
        return cls(
            app_id=data["app_id"],
            app_secret=data["app_secret"],
            receiver_ids=data.get("receiver_ids", []),
        )


class LarkClient:
    BASE_URL = "https://open.larksuite.com/open-apis"

    def __init__(self, config: LarkConfig):
        self.config = config
        self.access_token = self._get_access_token()

    def _get_access_token(self) -> str:
        response = requests.post(
            f"{self.BASE_URL}/auth/v3/tenant_access_token/internal",
            headers={"Content-Type": "application/json"},
            json={"app_id": self.config.app_id, "app_secret": self.config.app_secret},
        )
        response.raise_for_status()
        return response.json()["tenant_access_token"]

    def send_message(self, receiver_id: str, message: str) -> None:
        response = requests.post(
            f"{self.BASE_URL}/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={
                "content": json.dumps({"text": message}),
                "msg_type": "text",
                "receive_id": receiver_id,
            },
        )
        print(
            f"Notification sent to {receiver_id}, "
            f"status: {response.status_code}, body: {response.text}"
        )

    def broadcast(self, message: str, delay: float = 3.0) -> None:
        for receiver_id in self.config.receiver_ids:
            self.send_message(receiver_id, message)
            time.sleep(delay)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run rate notification job")
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        required=True,
        help="Deployment environment",
    )
    return parser.parse_args()


def fetch_traffic_data(bq: BigQuery, full_table_id: str) -> pl.DataFrame:
    bq.create_bq_table_from_gsheet_table(
        gsheet_table=GSheetTable(
            sheet_id=GSHEET_ID,
            tab_name=GSHEET_TAB,
            schema_path=SCHEMA_PATH,
        ),
        full_table_id=full_table_id,
        recreate_if_exists=True,
    )

    query_job = bq.get_client().query(
        f"SELECT * FROM `{full_table_id}`;",
        job_config=bigquery.QueryJobConfig(dry_run=False, use_query_cache=False),
    )

    return pl.from_arrow(query_job.result().to_arrow())


def main() -> int:
    args = parse_args()
    project_id = "conda-cps-dev" if args.env == "dev" else "conda-cps-prod"
    full_table_id = f"{project_id}.external_gsheet.traffic"

    try:
        lark_config = LarkConfig.from_env()
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    lark_client = LarkClient(lark_config)
    bq = BigQuery()

    data = fetch_traffic_data(bq, full_table_id)
    for row in data.rows(named=True):
        print(row)
        # message = format_row_message(row)
        # lark_client.broadcast(message)

    return 0


if __name__ == "__main__":
    sys.exit(main())
