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
                pl.col("cost").cast(pl.Utf8)
                + pl.format(" ({:.2f}%)", pl.col("cost_pct"))
            ).alias("cost (D)"),
            pl.col("prev_cost").alias("prev_cost (D-1)"),
            (
                pl.col("cpc").cast(pl.Utf8)
                + pl.format(" ({:.2f}%)", pl.col("cpc_pct"))
            ).alias("cpc (D)"),
            pl.col("prev_cpc").alias("prev_cpc (D-1)"),
        ])
        print(f"Latest result for {platform}:\n{display_result}")


    return 0


if __name__ == "__main__":
    sys.exit(main())
