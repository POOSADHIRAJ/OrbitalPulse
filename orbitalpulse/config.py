"""Configuration loading. Paths are relative to the project root (never hardcoded).

Defaults live in ``config/``. User edits made through the API are written to
``<data_dir>/config/`` as overrides, so the shipped defaults stay untouched.
Set ORBITALPULSE_DATA_DIR to relocate all runtime data (DB, raw archive, exports).
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, model_validator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_DIR = PROJECT_ROOT / "config"


def data_dir() -> Path:
    path = Path(os.environ.get("ORBITALPULSE_DATA_DIR", PROJECT_ROOT / "data"))
    path.mkdir(parents=True, exist_ok=True)
    return path


class SensorSpec(BaseModel):
    sensor_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_.\-]+$")
    display_name: str
    unit: str = Field(min_length=1)
    description: str = ""
    data_type: str = "float"
    expected_interval_s: float | None = Field(default=None, gt=0)
    validation_min: float | None = None
    validation_max: float | None = None
    operating_min: float | None = None
    operating_max: float | None = None
    enabled: bool = True
    synthetic_profile: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _bounds(self) -> "SensorSpec":
        for lo, hi in (("validation_min", "validation_max"), ("operating_min", "operating_max")):
            a, b = getattr(self, lo), getattr(self, hi)
            if a is not None and b is not None and a > b:
                raise ValueError(f"{lo} must be <= {hi}")
        return self


class SensorConfig(BaseModel):
    version: str = "sensors-1"
    sensors: list[SensorSpec]

    @model_validator(mode="after")
    def _unique(self) -> "SensorConfig":
        ids = [s.sensor_id for s in self.sensors]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate sensor_id in sensor configuration")
        return self

    def by_id(self) -> dict[str, SensorSpec]:
        return {s.sensor_id: s for s in self.sensors}


class Settings(BaseModel):
    version: str = "settings-1"
    limits: dict[str, Any]
    kafka: dict[str, Any]
    quality: dict[str, float]
    anomaly: dict[str, float]
    expected_phase_order: list[str]


def _read(name: str) -> dict:
    override = data_dir() / "config" / name
    path = override if override.is_file() else DEFAULT_CONFIG_DIR / name
    with path.open("r", encoding="utf-8-sig") as fh:
        return json.load(fh)


def load_sensor_config() -> SensorConfig:
    return SensorConfig.model_validate(_read("sensors.json"))


def load_settings() -> Settings:
    return Settings.model_validate(_read("settings.json"))


def save_override(name: str, payload: dict) -> None:
    folder = data_dir() / "config"
    folder.mkdir(parents=True, exist_ok=True)
    tmp = folder / (name + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(folder / name)


def config_fingerprint(sensors: SensorConfig, settings: Settings) -> str:
    """Stable hash of the effective configuration; stored with every finding."""
    blob = json.dumps(
        {"s": sensors.model_dump(), "t": settings.model_dump()}, sort_keys=True, default=str
    )
    return hashlib.sha256(blob.encode()).hexdigest()[:12]
