import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
import requests
from google.cloud import bigquery

from toolbox.bigquery import BigQuery
from toolbox.crawler import Crawler


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Content-Type': 'application/x-www-form-urlencoded',
    'X-FB-Friendly-Name': 'AdLibrarySearchPaginationQuery',
    'X-FB-LSD': 'AdGx2P7M3KI',
    'X-ASBD-ID': '359341',
    'Origin': 'https://www.facebook.com',
    'Sec-GPC': '1',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
}

DATA = {
    'av': '0',
    '__aaid': '0',
    '__user': '0',
    '__a': '1',
    '__req': '7',
    '__hs': '20448.HYP:comet_plat_default_pkg.2.1...0',
    'dpr': '1',
    '__ccg': 'EXCELLENT',
    '__rev': '1031475961',
    '__s': 'h9dzdt:9itwfq:urbk1e',
    '__hsi': '7588063125060013726',
    '__dyn': '7xeUmwlECdwn8K2Wmh0no6u5U4e1Fx-ewSAwHwNw9G2S2q0_EtxG4o0B-qbwgE1EEb87C1xwEwgo9oO0n24oaEd86a3a1YwBgao6C0Mo6i588Etw8WfK1LwPxe2GewbCXwJwmE2eUlwhE2Lw6OyES0gq0K-1LwqobU3Cwr86C1nwf6Eb87u1rwGwbu1ww',
    '__csr': 'ggq6DF8ynlPAGBKBJ6GFbEharkzueUKhemqt7ggm5JolAx7m3i2OcwMwVwVAxy3i69C6GwHwfm3S0CEtw8G1UK16yEeE520ALg4q0xU8XyE520H80Oy0Ao0a8E2Vw1EC06Ro0wC0LE02dkg03Plw0baa0m-01Pch80AK1hw19K',
    '__hsdp': 'l2IG49W40Jy2xECmQ98FksG8qiEVLEG8Gi-Fmu2uq8wEgdoa8kS5A2lsoIRCDyJ1W0Oaway2y5U1yE1u8C0x8dohw8S3m1Zwo8ho6a4FUR6oN0gU08CU1s808I8065C015cw0mnE',
    '__hblp': '00D2wbu05OE0Ni0Op82xw3A80kvwb-01m9w23U2Ew2JU2Vw2aU0kCw3080JS0g-07qU0dUE0lgw1Pu1qwvoW0Po0bNE0m4g33xS0U8',
    '__sjsp': 'l2IG49W40Jy2xECmQ98FksG8qiEVLEG8Gi-Fmu2uq8wEgdoa8kS5A2lsoLnpFUHguwcyE2EwExu0oG0ny9w8i3m4o2dwRwvo79woEiDzkpz413w0Ecw',
    '__comet_req': '94',
    'lsd': 'AdGx2P7M3KI',
    'jazoest': '2817',
    '__spin_r': '1031475961',
    '__spin_b': 'trunk',
    '__spin_t': '1766733621',
    '__jssesw': '1',
    'fb_api_caller_class': 'RelayModern',
    'fb_api_req_friendly_name': 'AdLibrarySearchPaginationQuery',
    'server_timestamps': 'true',
    'variables': None,
    'doc_id': '25464068859919530',
}

VARIABLES = {
    "activeStatus": "active",
    "adType": "ALL",
    "bylines": [],
    "collationToken": None,
    "contentLanguages": [],
    "countries": ["VN"],
    "cursor": None,
    "excludedIDs": None,
    "first": 10,
    "isTargetedCountry": False,
    "location": None,
    "mediaType": "all",
    "multiCountryFilterMode": None,
    "pageIDs": [],
    "potentialReachInput": None,
    "publisherPlatforms":[],
    "queryString":"",
    "regions": None,
    "searchType": "page",
    "sortData": None,
    "source": None,
    "startDate": None,
    "viewAllPageID": "114771895207322"
}

PAGE_IDS = {
    '114771895207322': 'CPS',
    # '110388245105469': 'CPS',
    # '111873897971603': 'CPS',
    # '340413785825514': 'CPS',
    # '124707154243128': 'DMX',
    # '332042093538446': 'FPT',
    # '133085353369973': 'CL',
    # '1557782724478208': 'SHOPEE'
}


class FacebookAds:
    def __init__(self):
        self.crawler = Crawler(proxy_list=["ScrapeDo"])
        self.bq = BigQuery()
        self.bq_client = self.bq.get_client()
        self.rd_challenge = self._get_rd_challenge("312304267031140")

    @staticmethod
    def _extract_url_from_body(body):
        urls = re.findall(r"(https?://[^\s]+)", body)
        return urls[0] if len(urls) != 0 else "N/A"

    @staticmethod
    def _get_rd_challenge(ad_id: str) -> str | None:
        response = requests.get(
            f"https://www.facebook.com/ads/library/?id={ad_id}",
            headers=HEADERS,
        )

        match = re.search(r"fetch\('(/__rd_verify_[^']+)'", response.text)
        if not match:
            return None

        rd_verify_path = match.group(1)
        verify_response = requests.head(
            f"https://www.facebook.com{rd_verify_path}",
            headers=HEADERS,
        )

        set_cookie = verify_response.headers.get("Set-Cookie", "")
        cookie_match = re.search(r"rd_challenge=([^;]+)", set_cookie)
        return cookie_match.group(1) if cookie_match else None

    def _parse_raw_data(self, raw_data):
        ads = []
        edges = raw_data["data"]["ad_library_main"]["search_results_connection"]["edges"]

        for edge in edges:
            ad = edge["node"]["collated_results"][0]
            page_id = ad["page_id"]
            ad_id = ad["ad_archive_id"]
            ad_body = ad["snapshot"].get("body")
            ad_body = ad_body.get("text") if ad_body else "N/A"
            product_link = ad["snapshot"]["link_url"]
            image_url = None

            if ad_body.replace(" ", "") == "{{product.brand}}":
                ad_body = ""

            if len(ad["snapshot"]["images"]) != 0:
                image_url = ad["snapshot"]["images"][0]["resized_image_url"]
            if not image_url and len(ad["snapshot"]["videos"]) != 0:
                image_url = ad["snapshot"]["videos"][0]["video_preview_image_url"]

            if len(ad["snapshot"]["cards"]) != 0:
                first_ad_card =  ad["snapshot"]["cards"][0]
                ad_body = first_ad_card["body"] if ad_body == "" else ad_body
                product_link = product_link if product_link else first_ad_card["link_url"]
                image_url = first_ad_card["resized_image_url"] or first_ad_card["video_preview_image_url"]

            if product_link is None:
                product_link = self._extract_url_from_body(ad_body)

            ad_data = {
                "page_id": page_id,
                "ad_id": ad_id,
                "ad_body": ad_body,
                "product_link": product_link,
                "start_date": datetime.fromtimestamp(int(ad["start_date"])).strftime("%Y-%m-%d"),
                "end_date": datetime.fromtimestamp(int(ad["end_date"])).strftime("%Y-%m-%d"),
                "platforms": ",".join(ad["publisher_platform"]),
                "image_url": image_url
            }
            ads.append(ad_data)

        return ads

    def download_raw_ads(self):
        if not self.rd_challenge:
            raise Exception("Failed to get rd_challenge cookie")

        for page_id in PAGE_IDS:
            raw_path = f"raw/facebook/{page_id}"
            shutil.rmtree(raw_path, ignore_errors=True)
            Path(raw_path).mkdir(parents=True, exist_ok=True)

            data = DATA
            variables = VARIABLES
            variables["cursor"] = None
            variables["viewAllPageID"] = page_id

            i = 1
            print(f"Fetching ads of {page_id}...")
            while True:
                data["variables"] = json.dumps(variables).replace(" ", "")
                response = self.crawler.post(
                    url="https://www.facebook.com/api/graphql/",
                    headers=HEADERS,
                    params=None,
                    data=data,
                    cookies={"rd_challenge": self.rd_challenge}
                )

                with open(f"{raw_path}/{page_id}.{i:05d}.json", "w") as f:
                    json.dump(response, f, indent=4)

                page_info = response["data"]["ad_library_main"]["search_results_connection"]["page_info"]
                if not page_info["has_next_page"]:
                    break

                i += 1
                variables["cursor"] = page_info["end_cursor"]

    def convert_json_to_parquet(self):
        raw_path = "raw/facebook"
        stage_path = "stage/facebook"

        shutil.rmtree(stage_path, ignore_errors=True)
        Path(stage_path).mkdir(parents=True, exist_ok=True)

        parsed_data = []
        for root, _, files in os.walk(raw_path):
            for file in files:
                with open(f"{root}/{file}") as f:
                    raw_data = json.load(f)
                    parsed_data.extend(self._parse_raw_data(raw_data))

        schema = {
            "page_id": pl.String,
            "ad_id": pl.String,
            "ad_body": pl.String,
            "product_link": pl.String,
            "start_date": pl.String,
            "end_date": pl.String,
            "platforms": pl.String,
            "image_url": pl.String,
        }

        stage_file_path = f"{stage_path}/staging.parquet"
        print(f"Writing stage file to {stage_file_path}")
        df = pl.DataFrame(parsed_data, schema=schema)
        df.write_parquet(stage_file_path)

    def upload_parquet_to_bq(self, table_id):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE",
        )

        stage_file_path = "stage/facebook/staging.parquet"

        with open(stage_file_path, "rb") as f:
            job = self.bq_client.load_table_from_file(
                f, table_id, job_config=job_config
            )

        job.result()
        print(f"Created {table_id} ({job.output_rows} rows)")
