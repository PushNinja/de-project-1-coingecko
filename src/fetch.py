import requests
import json
from datetime import datetime
import os
import logging
import time

# Einfaches Logging-Setup – später Airflow-kompatibel
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


def fetch_data(
    vs_currency: str = "usd",
    per_page: int = 100,
    page: int = 1,
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
) -> str:
    """
    Holt Marktdaten von der CoinGecko-API und speichert sie als JSON im raw/-Ordner.
    Gibt den Dateipfad zurück.
    """

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "per_page": per_page,
        "page": page,
        "order": "market_cap_desc",
    }

    os.makedirs("raw", exist_ok=True)

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            logger.info(
                "Fetching data from CoinGecko (attempt %s/%s, vs_currency=%s, per_page=%s, page=%s)",
                attempt,
                max_retries,
                vs_currency,
                per_page,
                page,
            )

            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 429:
                logger.warning(
                    "Rate limit hit (429). Waiting %s seconds...", backoff_seconds)
                time.sleep(backoff_seconds)
                continue

            if response.status_code != 200:
                logger.error("API request failed with status %s",
                             response.status_code)
                time.sleep(backoff_seconds)
                continue

            data = response.json()
            if not data:
                logger.warning("Empty response from API.")
                time.sleep(backoff_seconds)
                continue

            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"raw/coingecko_{vs_currency}_{timestamp}.json"

            with open(filename, "w") as f:
                json.dump(data, f, indent=4)

            logger.info("Saved raw data to %s", filename)
            return filename

        except requests.RequestException as e:
            logger.error("Request error: %s", e)
            time.sleep(backoff_seconds)

    raise RuntimeError("Failed to fetch data from CoinGecko after retries.")
