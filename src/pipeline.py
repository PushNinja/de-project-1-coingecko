from fetch import fetch_data
from validate import validate_raw_json
from transform import transform_data
from utils import get_logger

logger = get_logger(__name__)


def run_pipeline(
    vs_currency: str = "usd",
    per_page: int = 100,
    page: int = 1,
):
    logger.info("Starting pipeline...")

    raw_file = fetch_data(
        vs_currency=vs_currency,
        per_page=per_page,
        page=page,
    )

    if not validate_raw_json(raw_file):
        raise RuntimeError("Validation failed. Aborting pipeline.")

    csv_path, parquet_path = transform_data(raw_file)

    logger.info("Pipeline complete.")
    return csv_path, parquet_path


if __name__ == "__main__":
    run_pipeline()
