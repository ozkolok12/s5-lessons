from typing import List, Dict, Any
import requests
from datetime import datetime
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class CourierReader:
    BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"

    def __init__(self, nickname: str, cohort: str, api_key: str, limit: int = 50):
        self.headers = {
            "X-Nickname": nickname,
            "X-Cohort": cohort,
            "X-API-KEY": api_key,
        }
        self.limit = limit

        #  сессия +  back-off
        self.session = requests.Session()
        retries = Retry(
            total=5,              
            connect=3,
            read=3,
            backoff_factor=1.0,  
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def _get(self, path: str, params):
        # 5 сек на соединение, 60 сек на чтение
        resp = self.session.get(
            f"{self.BASE_URL}{path}",
            headers=self.headers,
            params=params,
            timeout=(5, 60),
        )
        resp.raise_for_status()
        return resp.json()

    def load_couriers(self, offset: int = 0) -> List[Dict[str, Any]]:
        return self._get(
            "/couriers",
            {
                "sort_field": "_id",
                "sort_direction": "asc",
                "limit": self.limit,
                "offset": offset
            }
        )

    def load_deliveries(self, offset: int = 0, from_date: str = None) -> List[Dict[str, Any]]:
        params = {
            "sort_field": "_id", "sort_direction": "asc",
            "limit": self.limit, "offset": offset
        }
        if from_date:
            params["from"] = from_date
        return self._get("/deliveries", params)
