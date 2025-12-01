import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def validate_raw_json(filepath: str) -> bool:
    """
    Validiert die RAW-JSON-Datei grob,
    bevor sie transformiert wird.
    """
    logger.info("Validating raw file: %s", filepath)

    try:
        with open(filepath) as f:
            data = json.load(f)
    except Exception as e:
        logger.error("Could not read JSON: %s", e)
        return False

    if not isinstance(data, list):
        logger.error("Expected a list of records, got %s", type(data))
        return False

    if len(data) == 0:
        logger.error("Empty API response.")
        return False

    logger.info("Raw JSON looks valid (%s records).", len(data))
    return True
