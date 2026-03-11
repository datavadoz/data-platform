import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any

import polars as pl
import requests
from google.cloud import bigquery

from toolbox.bigquery import BigQuery, GSheetTable

CUR_DIR = os.path.dirname(os.path.abspath(__file__))

GSHEET_ID = "14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ"
GSHEET_TAB = "Cost runrate!A2:AF71"
SCHEMA_PATH = os.path.join(CUR_DIR, "schemas", "run-rate.json")

NOTIFY_TEMPLATE = """=== RUN-RATE: *{dmc3}*
- Target DS: {target_sales} tỷ, % đạt: {sales} % MoM: {mom}
- Budget vs DS plan (Actual: {actual_budget} | Plan: {plan_budget}) | actual/plan: {actual_vs_plan_budget})
- FB Catalog % actual/plan: {actual_vs_plan_fb_catalog}
- FB % actual/plan: {actual_vs_plan_fb}
- GG % actual/plan: {actual_vs_plan_gg}
- TT % actual/plan: {actual_vs_plan_tt}
- Dynamic % actual/plan: {actual_vs_plan_dynamic}
- Criteo RE % actual/plan: {actual_vs_plan_criteo_re}
- Criteo NEW % actual/plan: {actual_vs_plan_criteo_new}
"""


def get_sql_statement(full_table_id: str) -> str:
    return f"""
    SELECT dmc3
      , ROUND(CAST(int64_field_2 AS FLOAT64) / 1000000000, 2)                               AS target_sales
      , ROUND(sales * 100, 2)                                                               AS sales
      , ROUND(mom * 100, 2)                                                                 AS mom
      , ROUND(plan_budget_perc * 100, 2)                                                    AS actual_budget_perc
      , ROUND(float_field_9 * 100, 2)                                                       AS plan_budget_perc
      , IF(plan_budget = 0, NULL, ROUND(actual_digital / plan_budget * 100, 2))             AS actual_vs_plan_budget
      , IF(plan_fb_catalog = 0, NULL, ROUND(actual_fb_catalog / plan_fb_catalog * 100, 2))  AS actual_vs_plan_fb_catalog
      , IF(plan_fb = 0, NULL, ROUND(actual_fb / plan_fb * 100, 2))                          AS actual_vs_plan_fb
      , IF(plan_gg = 0, NULL, ROUND(actual_gg / plan_gg * 100, 2))                          AS actual_vs_plan_gg
      , IF(plan_tt = 0, NULL, ROUND(actual_tt / plan_tt * 100, 2))                          AS actual_vs_plan_tt
      , IF(plan_new_camp_dynamic_search = 0, NULL, ROUND(actual_new_camp_dynamic_search / plan_new_camp_dynamic_search * 100, 2)) AS actual_vs_plan_dynamic
      , IF(plan_criteo_re = 0, NULL, ROUND(actual_criteo_re / plan_criteo_re * 100, 2))     AS actual_vs_plan_criteo_re
      , IF(plan_criteo_new = 0, NULL, ROUND(actual_criteo_new / plan_criteo_new * 100, 2))  AS actual_vs_plan_criteo_new
    FROM `{full_table_id}`
    ORDER BY int64_field_2 DESC
    LIMIT 20
    """


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


def format_percentage(value: Any) -> str:
    return f"{value}%" if value is not None else "N/A"


def format_value(value: Any) -> str:
    return str(value) if value is not None else "N/A"


def format_row_message(row: dict[str, Any]) -> str:
    return NOTIFY_TEMPLATE.format(
        dmc3=format_value(row["dmc3"]),
        target_sales=format_value(row["target_sales"]),
        sales=format_percentage(row["sales"]),
        mom=format_percentage(row["mom"]),
        actual_budget=format_percentage(row["actual_budget_perc"]),
        plan_budget=format_percentage(row["plan_budget_perc"]),
        actual_vs_plan_budget=format_percentage(row["actual_vs_plan_budget"]),
        actual_vs_plan_fb_catalog=format_percentage(row["actual_vs_plan_fb_catalog"]),
        actual_vs_plan_fb=format_percentage(row["actual_vs_plan_fb"]),
        actual_vs_plan_gg=format_percentage(row["actual_vs_plan_gg"]),
        actual_vs_plan_tt=format_percentage(row["actual_vs_plan_tt"]),
        actual_vs_plan_dynamic=format_percentage(row["actual_vs_plan_dynamic"]),
        actual_vs_plan_criteo_re=format_percentage(row["actual_vs_plan_criteo_re"]),
        actual_vs_plan_criteo_new=format_percentage(row["actual_vs_plan_criteo_new"]),
    )


def fetch_run_rate_data(bq: BigQuery, full_table_id: str) -> pl.DataFrame:
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
        get_sql_statement(full_table_id),
        job_config=bigquery.QueryJobConfig(dry_run=False, use_query_cache=False),
    )

    return pl.from_arrow(query_job.result().to_arrow())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run rate notification job")
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        required=True,
        help="Deployment environment",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_id = "conda-cps-dev" if args.env == "dev" else "conda-cps-prod"
    full_table_id = f"{project_id}.external_gsheet.run_rate"

    try:
        lark_config = LarkConfig.from_env()
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    lark_client = LarkClient(lark_config)
    bq = BigQuery()

    data = fetch_run_rate_data(bq, full_table_id)

    for row in data.rows(named=True):
        message = format_row_message(row)
        lark_client.broadcast(message)

    return 0


if __name__ == "__main__":
    sys.exit(main())
