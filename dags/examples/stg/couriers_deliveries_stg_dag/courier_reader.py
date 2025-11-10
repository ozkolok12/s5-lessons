from typing import List, Dict, Any
import requests
from datetime import datetime

class CourierReader:
    BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"

    def __init__(self, nickname: str, cohort: str, api_key: str, limit: int = 50):
        self.headers = {
            "X-Nickname": nickname,
            "X-Cohort": cohort,
            "X-API-KEY": api_key
        }
        self.limit = limit

    def _get(self, path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        r = requests.get(f"{self.BASE_URL}{path}", headers=self.headers, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

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

    def load_deliveries(self, offset: int = 0, days_back: int = 7) -> List[Dict[str, Any]]:
        from_date = (datetime.utcnow().date().fromordinal(
            datetime.utcnow().date().toordinal() - days_back
        )).strftime("%Y-%m-%d 00:00:00")

        return self._get(
            "/deliveries",
            {
                "sort_field": "_id",
                "sort_direction": "asc",
                "limit": self.limit,
                "offset": offset,
                "from": from_date
            }
        )
