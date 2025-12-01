import pandas as pd
import json
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def transform_data(filepath: str):
    """
    Lädt eine RAW-JSON-Datei, wandelt sie in ein DataFrame um,
    prüft Schema und Typen und speichert ein sauberes CSV + Parquet.
    """

    logger.info("Loading raw data from %s", filepath)

    # JSON lesen
    with open(filepath) as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected list of dictionaries from API")

    df = pd.DataFrame(data)

    # == Schema definieren (DE-Standard) ==
    expected_columns = [
        "id",
        "symbol",
        "name",
        "current_price",
        "high_24h",
        "low_24h",
        "market_cap",
        "total_volume",
    ]

    # Fehlende Spalten finden
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        logger.warning("Missing expected columns: %s", missing)

    # Nur die gewünschten Spalten extrahieren (nur die, die vorhanden sind)
    df = df[[col for col in expected_columns if col in df.columns]]

    # == Typkonvertierungen (enforces schema) ==
    numeric_cols = [
        "current_price",
        "high_24h",
        "low_24h",
        "market_cap",
        "total_volume",
    ]

    for col in numeric_cols:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ID-/Symbol-Spalten zu String
    for col in ["id", "symbol", "name"]:
        if col in df:
            df[col] = df[col].astype("string")

    # Zeilen mit komplett fehlenden Kursdaten entfernen
    df = df.dropna(subset=["current_price"])

    # Ordner sicherstellen
    os.makedirs("processed", exist_ok=True)

    csv_path = "processed/coins.csv"
    parquet_path = "processed/coins.parquet"

    logger.info("Saving cleaned data to %s and %s", csv_path, parquet_path)

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, index=False)

    return csv_path, parquet_path
