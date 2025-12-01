import pandas as pd
import json
import os


def transform_data(filepath):
    with open(filepath) as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    df_clean = df[[
        "id",
        "symbol",
        "name",
        "current_price",
        "high_24h",
        "low_24h",
        "market_cap",
        "total_volume"
    ]]

    os.makedirs("processed", exist_ok=True)

    csv_path = "processed/coins.csv"
    parquet_path = "processed/coins.parquet"

    df_clean.to_csv(csv_path, index=False)
    df_clean.to_parquet(parquet_path, index=False)

    return csv_path, parquet_path
