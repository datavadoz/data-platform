import argparse
import os

from toolbox.bigquery import BigQuery, GSheetTable


cur_dir = os.path.dirname(os.path.abspath(__file__))
parser = argparse.ArgumentParser()
parser.add_argument(
    "--env", choices=["dev", "prod"], required=True,
    help="Deployment environment"
)

args = parser.parse_args()
project_id = "conda-cps-dev" if args.env == "dev" else "conda-cps-prod"

bq = BigQuery()
bq.create_bq_table_from_gsheet_table(
    gsheet_table=GSheetTable(
        sheet_id='14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
        tab_name='Cost runrate!A2:AF71',
        schema_path=os.path.join(cur_dir, 'schemas', 'run_rate.json')
    ),
    full_table_id=f"{project_id}.external_gsheet.run_rate",
    recreate_if_exists=True
)
