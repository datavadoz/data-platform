from toolbox.bigquery import BigQuery, GSheetTable


bq = BigQuery()
bq.create_bq_table_from_gsheet_table(
    gsheet_table=GSheetTable(
        sheet_id='14zV1me4r6dHQn6c7nBbpW549eumP9OdfVfUq3kH51uQ',
        tab_name='Cost runrate!A2:AF71',
        schema_name='run_rate.json'
    ),
    full_table_id='datavadoz-438714.cps_monitor_gsheet.cost_run_rate',
    recreate_if_exists=True
)