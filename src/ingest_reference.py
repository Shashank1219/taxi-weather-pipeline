from __future__ import annotations

import logging
from pathlib import Path

import requests
from dotenv import load_dotenv

from s3_utils import upload_file_if_missing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
LOCAL_PATH = Path("data/bronze/reference/taxi_zone_lookup.csv")
S3_BUCKET = "taxi-weather-raw"
S3_KEY = "bronze/reference/taxi_zone_lookup.csv"


def main() -> None:
    load_dotenv()
    LOCAL_PATH.parent.mkdir(parents=True, exist_ok=True)

    if not LOCAL_PATH.exists():
        resp = requests.get(ZONE_LOOKUP_URL, timeout=30)
        resp.raise_for_status()
        LOCAL_PATH.write_bytes(resp.content)
        logger.info("Downloaded zone lookup to %s", LOCAL_PATH)
    else:
        logger.info("Zone lookup already present locally")

    upload_file_if_missing(LOCAL_PATH, S3_BUCKET, S3_KEY)


if __name__ == "__main__":
    main()