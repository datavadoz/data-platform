import requests
import urllib3
import time
import random

from toolbox.proxy import ProxyPool


class Crawler:
    def __init__(self, proxy_list=None):
        self.proxy_pool = ProxyPool(proxy_list)
        self.current_proxy = self.proxy_pool.pull()
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def request(self, method, url, params, headers, data, cookies=None):
        while True:
            request = getattr(requests, method)
            if method is None:
                raise Exception(f"Unknown method {method}")

            proxy_provider = self.current_proxy.get('proxy_provider')
            proxy_value = self.current_proxy.get('proxy_value')
            print(f"{method.upper()}: URL {url} via {proxy_provider}")

            if proxy_provider == "Localhost":
                time_sleep = random.randint(1, 10)
                time.sleep(time_sleep)
                print(f"Sleeping in {time_sleep} seconds...")

            response = request(
                url=url,
                params=params,
                headers=headers,
                data=data,
                proxies=proxy_value,
                verify=False,
                cookies=cookies,
            )

            status_code = response.status_code

            if status_code != 200:
                print(f"{status_code}: {response.text}")
                print("Switch to other API key or proxy provider then retry this request")
                self.current_proxy = self.proxy_pool.pull()
                if self.current_proxy is None:
                    break
                continue

            data = response.json()
            errors = data.get("errors")
            if errors:
                print(f"Got error: {errors}")
                print("Switch to other API key or proxy provider then retry this request")
                self.current_proxy = self.proxy_pool.pull()
                if self.current_proxy is None:
                    break
                continue

            return data

    def get(self, url, params, headers, data, cookies=None):
        return self.request("get", url, params, headers, data, cookies)

    def post(self, url, params, headers, data, cookies=None):
        return self.request("post", url, params, headers, data, cookies)
