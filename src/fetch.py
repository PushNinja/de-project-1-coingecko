import requests
import json
from datetime import datetime
import os


def fetch_data():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd"}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"API Request failed: {response.status_code}")

    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    os.makedirs("raw", exist_ok=True)
    filename = f"raw/coingecko_{timestamp}.json"

    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

    return filename
