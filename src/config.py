"""Typed configuration loading."""
from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: int = 30
    max_retries: int = 3
    backoff_base_seconds: int = 2


@dataclass(frozen=True)
class TaxiConfig:
    url_template: str
    bronze_dir: Path

    def url_for_month(self, month):
        return self.url_template.format(month=month)

    def bronze_path_for_month(self, month):
        return self.bronze_dir / f"yellow_tripdata_{month}.parquet"


@dataclass(frozen=True)
class WeatherConfig:
    latitude: float
    longitude: float
    timezone: str
    hourly_vars: list[str]
    bronze_path: Path


@dataclass(frozen=True)
class DQConfig:
    weather_match_rate_threshold: float
    passenger_count_min: int
    passenger_count_max: int


@dataclass(frozen=True)
class S3Config:
    bucket: str
    taxi_prefix: str
    weather_prefix: str

    def taxi_key_for_month(self, month):
        return f"{self.taxi_prefix}/yellow_tripdata_{month}.parquet"

    def weather_key(self):
        return f"{self.weather_prefix}/weather_hourly.parquet"


@dataclass(frozen=True)
class PipelineConfig:
    months: list[str]
    taxi: TaxiConfig
    weather: WeatherConfig
    dq: DQConfig
    s3: S3Config
    http: HttpConfig = field(default_factory=HttpConfig)


def month_date_bounds(months):
    first_year, first_month = map(int, months[0].split("-"))
    last_year, last_month = map(int, months[-1].split("-"))
    last_day = calendar.monthrange(last_year, last_month)[1]
    start_date = f"{first_year:04d}-{first_month:02d}-01"
    end_date = f"{last_year:04d}-{last_month:02d}-{last_day:02d}"
    return start_date, end_date


def expected_hourly_rows(start_date, end_date):
    # Expected weather row count for a date range, at hourly resolution
    from datetime import date
    d0 = date.fromisoformat(start_date)
    d1 = date.fromisoformat(end_date)
    num_days = (d1 - d0).days + 1
    return num_days * 24


def load_config(path: str | Path = "config/pipeline.yaml"):
    # Load .env (for AWS credentials) and pipeline.yaml (for everything else)
    load_dotenv()

    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path.resolve()}")

    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return PipelineConfig(
        months=raw["months"],
        taxi=TaxiConfig(
            url_template=raw["taxi"]["url_template"],
            bronze_dir=Path(raw["taxi"]["bronze_dir"]),
        ),
        weather=WeatherConfig(
            latitude=raw["weather"]["latitude"],
            longitude=raw["weather"]["longitude"],
            timezone=raw["weather"]["timezone"],
            hourly_vars=raw["weather"]["hourly_vars"],
            bronze_path=Path(raw["weather"]["bronze_path"]),
        ),
        dq=DQConfig(
            weather_match_rate_threshold=raw["dq"]["weather_match_rate_threshold"],
            passenger_count_min=raw["dq"]["passenger_count_min"],
            passenger_count_max=raw["dq"]["passenger_count_max"],
        ),
        s3=S3Config(
            bucket=raw["s3"]["bucket"],
            taxi_prefix=raw["s3"]["taxi_prefix"],
            weather_prefix=raw["s3"]["weather_prefix"],
        ),
        http=HttpConfig(**raw.get("http", {})),
    )