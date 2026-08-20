"""Fetch hourly historical weather for NYC from the Open-Meteo archive API."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd
import requests

from config import PipelineConfig, expected_hourly_rows, load_config, month_date_bounds
from http_utils import with_retry
from s3_utils import upload_file_if_missing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_weather_data(config, force = False):
    target = config.weather.bronze_path
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force:
        logger.info("Weather file already present at %s, skipping fetch", target)
    else:
        start_date, end_date = month_date_bounds(config.months)
        expected_rows = expected_hourly_rows(start_date, end_date)

        params = {
            "latitude": config.weather.latitude,
            "longitude": config.weather.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(config.weather.hourly_vars),
            "timezone": config.weather.timezone,
        }

        logger.info("Fetching weather for %s to %s (covers all %d configured months)",
                    start_date, end_date, len(config.months))

        def _do_fetch():
            resp = requests.get(ARCHIVE_API_URL, params=params, timeout=config.http.timeout_seconds)
            resp.raise_for_status()
            return resp.json()

        payload = with_retry(
            _do_fetch,
            max_retries=config.http.max_retries,
            backoff_base_seconds=config.http.backoff_base_seconds,
            description="weather API fetch",
        )

        hourly = payload.get("hourly")
        if not hourly or "time" not in hourly:
            raise ValueError(f"Unexpected Open-Meteo response shape: keys={list(payload.keys())}")

        df = pd.DataFrame(hourly)
        df = df.rename(columns={"time": "weather_hour_local"})
        df["weather_hour_local"] = pd.to_datetime(df["weather_hour_local"])

        row_count = len(df)
        if row_count != expected_rows:
            logger.warning("Weather row count %d does not match expected %d for %s to %s",
                            row_count, expected_rows, start_date, end_date)
        else:
            logger.info("Weather row count matches expected: %d rows", row_count)

        df.to_parquet(target, index=False)
        logger.info("Wrote weather data (%s to %s) to %s", start_date, end_date, target)

    s3_key = config.s3.weather_key()
    upload_file_if_missing(target, config.s3.bucket, s3_key, force=force)

    return target


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch hourly NYC weather for configured months")
    parser.add_argument("--config", default="config/pipeline.yml")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    fetch_weather_data(cfg, force=args.force)