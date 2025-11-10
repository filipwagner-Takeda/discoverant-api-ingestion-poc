import requests
from requests.adapters import HTTPAdapter, Retry


class BaseApiClient:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))

    def get(self, url: str, headers=None, params=None, auth=None):
        response = self.session.get(url, headers=headers, params=params, auth=auth)
        response.raise_for_status()
        return response.json()

    def post(self, url: str, data=None, json=None, headers=None, auth=None):
        response = self.session.post(url, data=data, json=json, headers=headers, auth=auth)
        response.raise_for_status()
        return response.json()
