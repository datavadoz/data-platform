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


def get_gsheet_table(platform: str) -> GSheetTable:
    if platform == "fb":
        tab_name = "FB_Day!A:G"
    elif platform == "gg":
        tab_name = "GG_ADs_Day!A:F"
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
        full_table_id = f"{project_id}.external_gsheet.cpc_{platform}"
        bq.create_bq_table_from_gsheet_table(
            gsheet_table=gsheet_table,
            full_table_id=full_table_id,
            recreate_if_exists=True,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
