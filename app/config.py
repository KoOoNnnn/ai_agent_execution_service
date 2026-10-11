from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import AmqpDsn, Field, PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    database_url: PostgresDsn
    rabbitmq_url: AmqpDsn
    rabbitmq_queue: str = "agent.runs"
    broker_timeout_seconds: float = Field(default=5, gt=0)
    worker_prefetch: int = Field(default=1, ge=1)
    worker_concurrency: int = Field(default=1, ge=1)
    lease_ttl_seconds: float = Field(default=30, gt=0)
    heartbeat_interval_seconds: float = Field(default=10, gt=0)
    decision_timeout_seconds: float = Field(default=10, gt=0)
    decision_max_attempts: int = Field(default=3, ge=1, le=10)
    decision_retry_base_seconds: float = Field(default=1, ge=0)
    decision_retry_jitter_seconds: float = Field(default=0.2, ge=0)
    run_timeout_seconds: float = Field(default=300, gt=0)

    @model_validator(mode="after")
    def validate_lease_timing(self) -> Self:
        if self.heartbeat_interval_seconds * 3 > self.lease_ttl_seconds:
            raise ValueError("Heartbeat interval must be at most one third of the lease TTL")
        return self

    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
