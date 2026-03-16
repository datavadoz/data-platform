import json
import os


class ProxyPool:
    def __init__(self, only_local=False):
        self.priority = ["Localhost"] if only_local else ["Localhost", "ScrapeDo", "ScraperApi"]
        self.proxies = self._build_proxies()

    @staticmethod
    def _build_proxies() -> dict:
        """
        Build a dictionary of proxies from environment variables.
        {
            "ScrapeDo": {
                "keys": ["key1", "key2"],
                "https": "https://{KEY}@proxy.scrape.do"
            },
            "ScraperApi": {
                "keys": ["key3"],
                "https": "https://scraperapi:{KEY}@proxy.scraperapi.com:8001"}
            }
        """
        proxies = {"Localhost": None}

        raw = os.environ.get("PROXY_KEYS", "")
        if raw:
            proxies.update(json.loads(raw))

        return proxies

    def pull(self):
        for proxy_provider in self.priority:
            if proxy_provider not in self.proxies.keys():
                continue

            if proxy_provider == "Localhost":
                self.proxies.pop(proxy_provider)
                return {
                    "proxy_provider": proxy_provider,
                    "proxy_value": None
                }

            proxy = self.proxies.get(proxy_provider)
            keys = proxy.get("keys")
            if len(keys) == 0:
                print(f"Reach limit of {proxy_provider} API keys")
                self.proxies.pop(proxy_provider)
                continue

            key = keys.pop()

            return {
                "proxy_provider": proxy_provider,
                "proxy_value": {"https": proxy.get("https").format(KEY=key)}
            }

        print("Out of proxy!")
        return None
