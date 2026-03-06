from google.cloud import bigquery


def main():
    client = bigquery.Client()
    query_job = client.query("SELECT 'Hello, World!' AS greeting")
    result = query_job.result()
    for row in result:
        print(row['greeting'])


main()
