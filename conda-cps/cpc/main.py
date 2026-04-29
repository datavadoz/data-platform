import argparse
from dataclasses import dataclass
from datetime import date
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
PLATFORMS = ["fb", "gg"]

CREATE_CPC_FB_PUBLISHED_TABLE_QUERY = """
CREATE OR REPLACE TABLE `{project_id}.history.cpc_fb_published`
PARTITION BY date
AS
WITH dmc3 AS (
  SELECT
    date,
    IFNULL(dmc3, 'KHÁC') AS dmc3,
    SUM(cost) AS cost,
    IF(
      SUM(landing_page_views) = 0,
      0,
      SUM(cost) / SUM(landing_page_views) / 23500
    ) AS cpc
  FROM `{project_id}.history.cpc_fb`
  GROUP BY date, dmc3
)
, total AS (
  SELECT
    date,
    'TOTAL' AS dmc3,
    SUM(cost) AS cost,
    IF(
      SUM(landing_page_views) = 0,
      0,
      SUM(cost) / SUM(landing_page_views) / 23500
    ) AS cpc
  FROM `{project_id}.history.cpc_fb`
  GROUP BY date
)
, distinct_dmc3 AS (
  SELECT DISTINCT IFNULL(dmc3, 'KHÁC') AS dmc3
  FROM `{project_id}.history.cpc_fb`
)
, date_series AS (
  SELECT DISTINCT date
  FROM `{project_id}.history.cpc_fb`
)
, all_date_dmc3_combinations AS (
  SELECT date, dmc3
  FROM distinct_dmc3
  CROSS JOIN date_series
)
, dmc3_and_total AS (
  SELECT
    T1.date AS date,
    T1.dmc3 AS dmc3,
    IFNULL(T2.cost, 0) AS cost,
    IFNULL(T2.cpc, 0) AS cpc,
  FROM all_date_dmc3_combinations T1
  LEFT JOIN dmc3 T2
    ON T1.date = T2.date
    AND T1.dmc3 = T2.dmc3

  UNION ALL

  SELECT *
  FROM total
)
, result_with_prev AS (
  SELECT *,
    IFNULL(LAG(cost) OVER (PARTITION BY dmc3 ORDER BY date), cost) AS prev_cost,
    IFNULL(LAG(cpc) OVER (PARTITION BY dmc3 ORDER BY date), cpc) AS prev_cpc,
  FROM dmc3_and_total
)

SELECT *,
  IF(prev_cost <> 0,
    (cost / prev_cost - 1) * 100,
    IF(cost = 0, 0, 100)
  ) cost_pct,
  IF(prev_cpc <> 0,
    (cpc / prev_cpc - 1) * 100,
    IF(cpc = 0, 0, 100)
  ) cpc_pct,
FROM result_with_prev
"""
CREATE_CPC_GG_PUBLISHED_TABLE_QUERY = """
CREATE OR REPLACE TABLE `{project_id}.history.cpc_gg_published`
PARTITION BY date
AS
WITH dmc3 AS (
  SELECT
    date,
    IFNULL(dmc3, 'KHÁC') AS dmc3,
    IFNULL(channel, 'KHÁC') AS channel,
    SUM(cost) AS cost,
    IF(
      SUM(clicks) = 0,
      0,
      SUM(cost) / SUM(clicks) / 23500
    ) AS cpc
  FROM `{project_id}.history.cpc_gg`
  GROUP BY date, dmc3, channel
)
, total AS (
  SELECT
    date,
    'TOTAL' AS dmc3,
    IFNULL(channel, 'KHÁC') AS channel,
    SUM(cost) AS cost,
    IF(
      SUM(clicks) = 0,
      0,
      SUM(cost) / SUM(clicks) / 23500
    ) AS cpc
  FROM `{project_id}.history.cpc_gg`
  GROUP BY date, channel
)
, distinct_dmc3 AS (
  SELECT DISTINCT IFNULL(dmc3, 'KHÁC') AS dmc3
  FROM `{project_id}.history.cpc_gg`
)
, distinct_channel AS (
  SELECT DISTINCT IFNULL(channel, 'KHÁC') AS channel
  FROM `{project_id}.history.cpc_gg`
)
, distinct_date AS (
  SELECT DISTINCT date,
  FROM `{project_id}.history.cpc_gg`
)
, all_dmc3_channel_per_date AS (
  SELECT DISTINCT date, dmc3, channel
  FROM distinct_date
  CROSS JOIN distinct_dmc3
  CROSS JOIN distinct_channel
)
, all_channel_per_date AS (
  SELECT date, channel
  FROM distinct_date
  CROSS JOIN distinct_channel
)
, union_result AS (
  SELECT
    T1.date AS date,
    T1.dmc3 AS dmc3,
    T1.channel AS channel,
    IFNULL(T2.cost, 0) AS cost,
    IFNULL(T2.cpc, 0) AS cpc,
  FROM all_dmc3_channel_per_date T1
  LEFT JOIN dmc3 T2
    ON T1.date = T2.date
    AND T1.dmc3 = T2.dmc3
    AND T1.channel = T2.channel

  UNION ALL

  SELECT
    T3.date AS date,
    'TOTAL' AS dmc3,
    T3.channel AS channel,
    IFNULL(T4.cost, 0) AS cost,
    IFNULL(T4.cpc, 0) AS cpc,
  FROM all_channel_per_date T3
  LEFT JOIN total T4
    ON T3.date = T4.date
    AND T3.channel = T4.channel
)
, union_result_with_prev AS (
  SELECT *,
    IFNULL(LAG(cost) OVER (PARTITION BY dmc3, channel ORDER BY date), cost) AS prev_cost,
    IFNULL(LAG(cpc) OVER (PARTITION BY dmc3, channel ORDER BY date), cpc) AS prev_cpc,
  FROM union_result
  ORDER BY date, dmc3, channel
)

SELECT *,
  IF(prev_cost <> 0,
    (cost / prev_cost - 1) * 100,
    IF(cost = 0, 0, 100)
  ) cost_pct,
  IF(prev_cpc <> 0,
    (cpc / prev_cpc - 1) * 100,
    IF(cpc = 0, 0, 100)
  ) cpc_pct,
FROM union_result_with_prev
"""
GET_LATEST_RESULT_QUERY = """
SELECT *
FROM `{published_table_id}`
WHERE (ABS(cost_pct) > 10 OR ABS(cpc_pct) > 10)
  AND date = (
    SELECT MAX(date)
    FROM `{published_table_id}`
  )
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

    def send_table(self, receiver_id: str, title: str, df: pl.DataFrame) -> None:
        columns = [
            {
                "name": col,
                "display_name": col,
                "data_type": "text",
                "width": "auto",
                "horizontal_align": "left",
            }
            for col in df.columns
        ]
        rows = [
            {col: str(val) for col, val in zip(df.columns, row)}
            for row in df.iter_rows()
        ]
        card = {
            "schema": "2.0",
            "header": {
                "title": {"tag": "plain_text", "content": title},
            },
            "body": {
                "elements": [
                    {
                        "tag": "table",
                        "page_size": 10,
                        "row_height": "low",
                        "header_style": {
                            "text_align": "left",
                            "bold": True,
                            "background_style": "grey",
                        },
                        "columns": columns,
                        "rows": rows,
                    }
                ]
            },
        }
        response = requests.post(
            f"{self.BASE_URL}/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={
                "content": json.dumps(card),
                "msg_type": "interactive",
                "receive_id": receiver_id,
            },
        )
        print(
            f"Notification sent to {receiver_id}, "
            f"status: {response.status_code}, body: {response.text}"
        )

    def broadcast_table(self, title: str, df: pl.DataFrame, delay: float = 3.0) -> None:
        for receiver_id in self.config.receiver_ids:
            self.send_table(receiver_id, title, df)
            time.sleep(delay)

    def send_message(self, receiver_id: str, message: str) -> None:
        response = requests.post(
            f"{self.BASE_URL}/im/v1/messages",
            params={"receive_id_type": "chat_id"},
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.access_token}",
            },
            json={
                "content": json.dumps({
                    "elements": [{
                        "tag": "markdown",
                        "content": message,
                    }]
                }),
                "msg_type": "interactive",
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


def df_to_markdown(df: pl.DataFrame) -> str:
    headers = df.columns
    header_row = "| " + " | ".join(headers) + " |"
    separator = "| " + " | ".join(["---"] * len(headers)) + " |"
    rows = [
        "| " + " | ".join(str(v) for v in row) + " |"
        for row in df.iter_rows()
    ]
    return "\n".join([header_row, separator] + rows)


def get_gsheet_table(platform: str) -> GSheetTable:
    if platform == "fb":
        tab_name = "FB_Day!A:J"
    elif platform == "gg":
        tab_name = "GG_ADs_Day!A:J"
    else:
        raise ValueError(f"Unsupported platform: {platform}")

    schema_file = os.path.join(CUR_DIR, "schemas", f"{platform}.json")

    return GSheetTable(
        sheet_id=GSHEET_ID,
        tab_name=tab_name,
        schema_path=schema_file,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CPC notification script")
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        required=True,
        help="Deployment environment",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    project_id = "conda-cps-dev" if args.env == "dev" else "conda-cps-prod"

    bq = BigQuery()
    lark_config = LarkConfig.from_env()
    lark_client = LarkClient(lark_config)

    for platform in PLATFORMS:
        gsheet_table = get_gsheet_table(platform)

        # Create external_gsheet table
        external_table_id = f"{project_id}.external_gsheet.cpc_{platform}"
        bq.create_bq_table_from_gsheet_table(
            gsheet_table=gsheet_table,
            full_table_id=external_table_id,
            recreate_if_exists=True,
        )

        # Create history table
        history_table_id = f"{project_id}.history.cpc_{platform}"
        schema = gsheet_table._get_bq_schema()
        bq.create_bq_table(
            full_table_id=history_table_id,
            schema=schema,
            partition_field="date",
        )

        # Ingest external data into history table
        bq.insert_override(
            source_table_id=external_table_id,
            dest_table_id=history_table_id,
            partition_field="date",
        )

        # Create the published table
        if platform == "fb":
            published_table_id = f"{project_id}.history.cpc_fb_published"
            query_template = CREATE_CPC_FB_PUBLISHED_TABLE_QUERY
        elif platform == "gg":
            published_table_id = f"{project_id}.history.cpc_gg_published"
            query_template = CREATE_CPC_GG_PUBLISHED_TABLE_QUERY
        else:
            raise ValueError(f"Unsupported platform: {platform}")

        bq.client.query(query_template.format(project_id=project_id)).result()
        print(f'Created {published_table_id}')

        # Get latest result
        latest_result = pl.from_arrow(
            bq.client.query(
                GET_LATEST_RESULT_QUERY.format(
                    published_table_id=published_table_id
                )
            ).result().to_arrow()
        )

        # Transform into display format
        display_result = latest_result.select([
            pl.col("dmc3"),
            pl.col("channel") if "channel" in latest_result.columns else pl.lit(None).alias("channel"),
            (
                pl.col("cost").round(2).cast(pl.Utf8)
                + pl.lit(" (")
                + pl.col("cost_pct").round(2).cast(pl.Utf8)
                + pl.lit("%)")
            ).alias("cost (D)"),
            pl.col("prev_cost").round(2).alias("prev_cost (D-1)"),
            (
                pl.col("cpc").round(2).cast(pl.Utf8)
                + pl.lit(" (")
                + pl.col("cpc_pct").round(2).cast(pl.Utf8)
                + pl.lit("%)")
            ).alias("cpc (D)"),
            pl.col("prev_cpc").round(2).alias("prev_cpc (D-1)"),
        ])

        if platform == "fb":
            display_result = display_result.drop("channel")

        display_result = display_result.sort(
            pl.col("dmc3").eq("TOTAL").cast(pl.Int8),
            descending=True,
        )

        print(f"Latest result for {platform}:\n{display_result}")

        if not display_result.is_empty():
            lark_client.broadcast_table(
                title=f"CPC Alert [{platform.upper()}]",
                df=display_result,
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
