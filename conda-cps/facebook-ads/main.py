import argparse

from toolbox.crawler import Crawler
from toolbox.facebook import FacebookAds

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Facebook Ads")
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
    full_table_id = f"{project_id}.ads.facebook"

    print('Crawling Facebook ads...')
    facebook = FacebookAds()
    facebook.download_raw_ads()
    facebook.convert_json_to_parquet()
    facebook.upload_parquet_to_bq(table_id=full_table_id)


if __name__ == '__main__':
    main()
