"""Download the NYC TLC Yellow Taxi trip file into the bronze layer."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pyarrow.parquet as pq
import requests

from config import PipelineConfig, load_config
from http_utils import with_retry

from s3_utils import upload_file_if_missing

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def download_taxi_month(config: PipelineConfig, month: str, force: bool = False) -> Path:
    """Download a single month's taxi file. Idempotent per-month, both locally and on S3."""
    target = config.taxi.bronze_path_for_month(month)
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force:
        logger.info("Taxi file for %s already present at %s, skipping download", month, target)
    else:
        url = config.taxi.url_for_month(month)
        logger.info("Downloading taxi data for %s from %s", month, url)

        def _do_download():
            with requests.get(url, stream=True, timeout=config.http.timeout_seconds) as resp:
                resp.raise_for_status()
                tmp_path = target.with_suffix(".tmp")
                with tmp_path.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                tmp_path.rename(target)
            return target

        with_retry(
            _do_download,
            max_retries=config.http.max_retries,
            backoff_base_seconds=config.http.backoff_base_seconds,
            description=f"taxi data download ({month})",
        )

        row_count = pq.ParquetFile(target).metadata.num_rows
        logger.info("Downloaded %s: %s rows at %s", month, f"{row_count:,}", target)

    s3_key = config.s3.taxi_key_for_month(month)
    upload_file_if_missing(target, config.s3.bucket, s3_key, force=force)

    return target


def download_all_months(config: PipelineConfig, force: bool = False) -> list[Path]:
    # Download every month in config.months. Each month fails/succeeds independently
    paths = []
    failures = []
    for month in config.months:
        try:
            paths.append(download_taxi_month(config, month, force=force))
        except Exception as exc:
            logger.error("Failed to download %s: %s", month, exc)
            failures.append(month)

    if failures:
        raise RuntimeError(f"Failed to download {len(failures)}/{len(config.months)} months: {failures}")

    logger.info("Downloaded all %d configured months", len(paths))
    return paths


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download NYC TLC taxi trip data for configured months")
    parser.add_argument("--config", default="config/pipeline.yml")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    download_all_months(cfg, force=args.force)