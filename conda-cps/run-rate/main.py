import argparse
import os
import requests
import time

from google.cloud import bigquery
import polars as pl
import json

from toolbox.bigquery import BigQuery, GSheetTable

cur_dir = os.path.dirname(os.path.abspath(__file__))
parser = argparse.ArgumentParser()
parser.add_argument(
    "--env", choices=["dev", "prod"], required=True,
    help="Deployment environment"
)
args = parser.parse_args()
project_id = "conda-cps-dev" if args.env == "dev" else "conda-cps-prod"
full_table_id = f"{project_id}.external_gsheet.run_rate"


SQL_STATEMENT = f"""
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

lark_secret = os.environ.get('LARK_SECRET', '')

if not lark_secret:
    print("LARK_SECRET is not set. Skipping notification.")
    exit(1)

lark_secret = json.loads(lark_secret)
app_id = lark_secret.get('app_id')
app_secret = lark_secret.get('app_secret')
receiver_ids = lark_secret.get('receiver_ids', [])

response = requests.post(
    'https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal',
    headers={"Content-Type": "application/json"},
    json={
        'app_id': app_id,
        'app_secret': app_secret,
    },
)

result = response.json()
access_token = result.get('tenant_access_token')


bq = BigQuery()
bq.create_bq_table_from_gsheet_table(
    gsheet_table=GSheetTable(
        sheet_id='14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
        tab_name='Cost runrate!A2:AF71',
        schema_path=os.path.join(cur_dir, 'schemas', 'run-rate.json')
    ),
    full_table_id=full_table_id,
    recreate_if_exists=True
)

query_job = bq.get_client().query(
    SQL_STATEMENT,
    job_config=bigquery.QueryJobConfig(
        dry_run=False,
        use_query_cache=False
    )
)

result = pl.from_arrow(query_job.result().to_arrow())

for row in result.rows(named=True):
    dmc3 = row['dmc3'] \
        if row['dmc3'] else 'N/A'
    target_sales = f"{row['target_sales']}" \
        if row['target_sales'] else 'N/A'
    sales = f"{row['sales']}%" \
        if row['sales'] else 'N/A'
    mom = f"{row['mom']}%" \
        if row['mom'] else 'N/A'
    actual_budget = f"{row['actual_budget_perc']}%" \
        if row['actual_budget_perc'] else 'N/A'
    plan_budget = f"{row['plan_budget_perc']}%" \
        if row['plan_budget_perc'] else 'N/A'
    actual_vs_plan_budget = f"{row['actual_vs_plan_budget']}%" \
        if row['actual_vs_plan_budget'] else 'N/A'
    actual_vs_plan_fb_catalog = f"{row['actual_vs_plan_fb_catalog']}%" \
        if row['actual_vs_plan_fb_catalog'] else 'N/A'
    actual_vs_plan_fb = f"{row['actual_vs_plan_fb']}%" \
        if row['actual_vs_plan_fb'] else 'N/A'
    actual_vs_plan_gg = f"{row['actual_vs_plan_gg']}%" \
        if row['actual_vs_plan_gg'] else 'N/A'
    actual_vs_plan_tt = f"{row['actual_vs_plan_tt']}%" \
        if row['actual_vs_plan_tt'] else 'N/A'
    actual_vs_plan_dynamic = f"{row['actual_vs_plan_dynamic']}%" \
        if row['actual_vs_plan_dynamic'] else 'N/A'
    actual_vs_plan_criteo_re = f"{row['actual_vs_plan_criteo_re']}%" \
        if row['actual_vs_plan_criteo_re'] else 'N/A'
    actual_vs_plan_criteo_new = f"{row['actual_vs_plan_criteo_new']}%" \
        if row['actual_vs_plan_criteo_new'] else 'N/A'

    msg = NOTIFY_TEMPLATE.format(
        dmc3=dmc3,
        sales=sales,
        mom=mom,
        actual_budget=actual_budget,
        plan_budget=plan_budget,
        actual_vs_plan_budget=actual_vs_plan_budget,
        actual_vs_plan_fb_catalog=actual_vs_plan_fb_catalog,
        actual_vs_plan_fb=actual_vs_plan_fb,
        actual_vs_plan_gg=actual_vs_plan_gg,
        actual_vs_plan_tt=actual_vs_plan_tt,
        actual_vs_plan_dynamic=actual_vs_plan_dynamic,
        actual_vs_plan_criteo_re=actual_vs_plan_criteo_re,
        actual_vs_plan_criteo_new=actual_vs_plan_criteo_new,
        target_sales=target_sales
    )

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}',
    }

    params = {
        'receive_id_type': 'chat_id',
    }

    for receiver_id in receiver_ids:
        json_data = {
            'content': json.dumps({'text': msg}),
            'msg_type': 'text',
            'receive_id': receiver_id
        }

        response = requests.post(
            'https://open.larksuite.com/open-apis/im/v1/messages',
            params=params, headers=headers, json=json_data
        )

        print(f"Notification sent to {receiver_id}, response status: {response.status_code}, response body: {response.text}")
        time.sleep(3)
