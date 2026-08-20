"""Unit tests for the ingestion scripts."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
import http_utils

import ingest_taxi
import ingest_weather
from config import DQConfig, HttpConfig, PipelineConfig, S3Config, TaxiConfig, WeatherConfig


def make_config(tmp_path, months: list[str] | None = None):
    return PipelineConfig(
        months=months or ["2026-01"],
        taxi=TaxiConfig(
            url_template="https://example.com/taxi_{month}.parquet",
            bronze_dir=tmp_path / "bronze" / "taxi",
        ),
        weather=WeatherConfig(
            latitude=40.7128,
            longitude=-74.0060,
            timezone="America/New_York",
            hourly_vars=["temperature_2m", "precipitation"],
            bronze_path=tmp_path / "bronze" / "weather.parquet",
        ),
        dq=DQConfig(weather_match_rate_threshold=0.95, passenger_count_min=1, passenger_count_max=8),
        s3=S3Config(bucket="test-bucket", taxi_prefix="bronze/taxi", weather_prefix="bronze/weather"),
        http=HttpConfig(timeout_seconds=5, max_retries=2, backoff_base_seconds=1),
    )


def mock_s3_upload(monkeypatch, module):
    """Replace the real S3 upload call with a no-op mock.

    Without this, every ingestion test would hit get_s3_client(), which
    checks os.environ for real AWS credentials and raises EnvironmentError
    in a clean test environment.
    """
    fake_upload = MagicMock(return_value="s3://fake-bucket/fake-key")
    monkeypatch.setattr(module, "upload_file_if_missing", fake_upload)
    return fake_upload


class TestDownloadTaxiMonth:
    def test_skips_download_if_file_already_exists(self, tmp_path, monkeypatch):
        config = make_config(tmp_path)
        mock_s3_upload(monkeypatch, ingest_taxi)

        target = config.taxi.bronze_path_for_month("2026-01")
        target.parent.mkdir(parents=True)
        target.write_bytes(b"already here")

        def fail_if_called(*args, **kwargs):
            raise AssertionError("requests.get should not be called when file already exists")

        monkeypatch.setattr(ingest_taxi.requests, "get", fail_if_called)

        result = ingest_taxi.download_taxi_month(config, "2026-01")

        assert result == target
        assert target.read_bytes() == b"already here"

    def test_downloads_and_logs_row_count_when_missing(self, tmp_path, monkeypatch):
        config = make_config(tmp_path)
        mock_s3_upload(monkeypatch, ingest_taxi)

        fake_response = MagicMock()
        fake_response.raise_for_status = MagicMock()
        fake_response.iter_content = MagicMock(return_value=[b"parquet-bytes"])
        fake_response.__enter__ = MagicMock(return_value=fake_response)
        fake_response.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(ingest_taxi.requests, "get", MagicMock(return_value=fake_response))

        fake_parquet_file = MagicMock()
        fake_parquet_file.metadata.num_rows = 12345
        monkeypatch.setattr(ingest_taxi.pq, "ParquetFile", MagicMock(return_value=fake_parquet_file))

        result = ingest_taxi.download_taxi_month(config, "2026-01")

        assert result == config.taxi.bronze_path_for_month("2026-01")
        assert result.exists()
        assert result.read_bytes() == b"parquet-bytes"

    def test_uploads_to_s3_after_download(self, tmp_path, monkeypatch):
        """The S3 upload should be attempted with the right bucket/key, whether
        or not the local download itself was skipped."""
        config = make_config(tmp_path)
        fake_upload = mock_s3_upload(monkeypatch, ingest_taxi)

        target = config.taxi.bronze_path_for_month("2026-01")
        target.parent.mkdir(parents=True)
        target.write_bytes(b"already here")  # local skip path

        def fail_if_called(*args, **kwargs):
            raise AssertionError("should not download when file exists locally")

        monkeypatch.setattr(ingest_taxi.requests, "get", fail_if_called)

        ingest_taxi.download_taxi_month(config, "2026-01")

        fake_upload.assert_called_once_with(
            target, "test-bucket", "bronze/taxi/yellow_tripdata_2026-01.parquet", force=False
        )

    def test_force_redownloads_even_if_file_exists(self, tmp_path, monkeypatch):
        config = make_config(tmp_path)
        mock_s3_upload(monkeypatch, ingest_taxi)

        target = config.taxi.bronze_path_for_month("2026-01")
        target.parent.mkdir(parents=True)
        target.write_bytes(b"stale data")

        fake_response = MagicMock()
        fake_response.raise_for_status = MagicMock()
        fake_response.iter_content = MagicMock(return_value=[b"fresh data"])
        fake_response.__enter__ = MagicMock(return_value=fake_response)
        fake_response.__exit__ = MagicMock(return_value=False)
        monkeypatch.setattr(ingest_taxi.requests, "get", MagicMock(return_value=fake_response))

        fake_parquet_file = MagicMock()
        fake_parquet_file.metadata.num_rows = 1
        monkeypatch.setattr(ingest_taxi.pq, "ParquetFile", MagicMock(return_value=fake_parquet_file))

        ingest_taxi.download_taxi_month(config, "2026-01", force=True)

        assert target.read_bytes() == b"fresh data"


class TestDownloadAllMonths:
    def test_downloads_every_configured_month(self, tmp_path, monkeypatch):
        config = make_config(tmp_path, months=["2026-01", "2026-02", "2026-03"])
        mock_s3_upload(monkeypatch, ingest_taxi)

        # pre-create all three files so every month hits the "already exists" skip path
        for month in config.months:
            p = config.taxi.bronze_path_for_month(month)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(b"data")

        monkeypatch.setattr(
            ingest_taxi.requests, "get",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("no download expected")),
        )

        results = ingest_taxi.download_all_months(config)

        assert len(results) == 3
        assert all(p.exists() for p in results)

    def test_raises_with_all_failed_months_listed_if_any_fail(self, tmp_path, monkeypatch):
        config = make_config(tmp_path, months=["2026-01", "2026-02"])
        mock_s3_upload(monkeypatch, ingest_taxi)

        def always_fails(*args, **kwargs):
            raise ingest_taxi.requests.exceptions.ConnectionError("simulated network failure")

        monkeypatch.setattr(ingest_taxi.requests, "get", always_fails)
        monkeypatch.setattr(http_utils.time, "sleep", lambda *_: None)

        with pytest.raises(RuntimeError, match="2026-01.*2026-02|2026-02.*2026-01"):
            ingest_taxi.download_all_months(config)


class TestFetchWeatherData:
    def test_skips_fetch_if_file_already_exists(self, tmp_path, monkeypatch):
        config = make_config(tmp_path)
        mock_s3_upload(monkeypatch, ingest_weather)

        config.weather.bronze_path.parent.mkdir(parents=True)
        config.weather.bronze_path.write_bytes(b"already here")

        def fail_if_called(*args, **kwargs):
            raise AssertionError("requests.get should not be called when file already exists")

        monkeypatch.setattr(ingest_weather.requests, "get", fail_if_called)

        result = ingest_weather.fetch_weather_data(config)

        assert result == config.weather.bronze_path

    def test_parses_full_month_and_row_count_matches(self, tmp_path, monkeypatch):
        """A real single month (Jan 2026 = 744 hours) end to end, exercising the
        actual month_date_bounds / expected_hourly_rows logic, not a mock of it."""
        config = make_config(tmp_path, months=["2026-01"])
        mock_s3_upload(monkeypatch, ingest_weather)

        hours = pd.date_range(start="2026-01-01", periods=744, freq="h")
        fake_payload = {
            "hourly": {
                "time": [h.strftime("%Y-%m-%dT%H:%M") for h in hours],
                "temperature_2m": [1.0] * 744,
            }
        }
        fake_response = MagicMock()
        fake_response.raise_for_status = MagicMock()
        fake_response.json = MagicMock(return_value=fake_payload)
        monkeypatch.setattr(ingest_weather.requests, "get", MagicMock(return_value=fake_response))

        result = ingest_weather.fetch_weather_data(config)

        df = pd.read_parquet(result)
        assert len(df) == 744
        assert "weather_hour_local" in df.columns

    def test_warns_on_row_count_mismatch(self, tmp_path, monkeypatch, caplog):
        config = make_config(tmp_path, months=["2026-01"])  # expects 744 hours
        mock_s3_upload(monkeypatch, ingest_weather)

        fake_payload = {"hourly": {"time": ["2026-01-01T00:00", "2026-01-01T01:00"], "temperature_2m": [1.0, 2.0]}}
        fake_response = MagicMock()
        fake_response.raise_for_status = MagicMock()
        fake_response.json = MagicMock(return_value=fake_payload)
        monkeypatch.setattr(ingest_weather.requests, "get", MagicMock(return_value=fake_response))

        with caplog.at_level("WARNING"):
            ingest_weather.fetch_weather_data(config)

        assert any("does not match expected" in record.message for record in caplog.records)

    def test_raises_on_unexpected_response_shape(self, tmp_path, monkeypatch):
        config = make_config(tmp_path)
        mock_s3_upload(monkeypatch, ingest_weather)

        fake_response = MagicMock()
        fake_response.raise_for_status = MagicMock()
        fake_response.json = MagicMock(return_value={"error": "something went wrong"})
        monkeypatch.setattr(ingest_weather.requests, "get", MagicMock(return_value=fake_response))

        with pytest.raises(ValueError, match="Unexpected Open-Meteo response shape"):
            ingest_weather.fetch_weather_data(config)