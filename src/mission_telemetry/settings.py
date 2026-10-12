import re
from functools import lru_cache

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_env: str = "development"
    demo_mode: bool = False
    jwt_secret: SecretStr = SecretStr("replace-this-development-secret-with-at-least-32-characters")
    token_ttl_minutes: int = 30
    mission_username: str = "mission-operator"
    mission_password: SecretStr = SecretStr("change-this-local-development-password")
    scientist_username: str = "scientist"
    scientist_password: SecretStr = SecretStr("change-this-scientist-development-password")
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "telemetry.v1"
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
    mongodb_uri: str = "mongodb://localhost:27017/?directConnection=true"
    mongodb_database: str = "mission_control"
    redis_url: str = "redis://localhost:6379/0"
    cassandra_hosts: str = "localhost"
    cassandra_port: int = 9042
    cassandra_keyspace: str = "mission_telemetry"
    cassandra_username: str | None = None
    cassandra_password: SecretStr | None = None
    cassandra_ca_file: str | None = None
    cassandra_schema_bootstrap: bool = True
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_username: str = "neo4j"
    neo4j_password: SecretStr = SecretStr("local-neo4j-password")
    neo4j_schema_bootstrap: bool = True
    rate_limit_per_minute: int = 120
    max_history_days: int = 31

    @field_validator("token_ttl_minutes", "rate_limit_per_minute", "max_history_days")
    @classmethod
    def validate_positive_integer(cls, value: int) -> int:
        if value < 1:
            raise ValueError("Value must be greater than zero")
        return value

    @field_validator("cassandra_keyspace")
    @classmethod
    def validate_cassandra_keyspace(cls, value: str) -> str:
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,47}", value) is None:
            raise ValueError("CASSANDRA_KEYSPACE must be a valid CQL identifier")
        return value

    @field_validator(
        "kafka_sasl_username", "cassandra_username", "cassandra_ca_file", mode="before"
    )
    @classmethod
    def normalize_optional_string(cls, value: str | None) -> str | None:
        return value.strip() or None if isinstance(value, str) else value

    @field_validator("kafka_sasl_password", "cassandra_password", mode="before")
    @classmethod
    def normalize_optional_secret(cls, value: str | SecretStr | None) -> str | SecretStr | None:
        return value.strip() or None if isinstance(value, str) else value

    @model_validator(mode="after")
    def enforce_production_secrets(self) -> "Settings":
        if self.mission_username == self.scientist_username:
            raise ValueError("MISSION_USERNAME and SCIENTIST_USERNAME must be different")
        if self.demo_mode and self.app_env.lower() != "demo":
            raise ValueError("DEMO_MODE requires APP_ENV=demo")
        kafka_password = (
            self.kafka_sasl_password.get_secret_value() if self.kafka_sasl_password else None
        )
        cassandra_password = (
            self.cassandra_password.get_secret_value() if self.cassandra_password else None
        )
        if bool(self.kafka_sasl_username) != bool(kafka_password):
            raise ValueError("Configure both KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD")
        if bool(self.cassandra_username) != bool(cassandra_password):
            raise ValueError("Configure both CASSANDRA_USERNAME and CASSANDRA_PASSWORD")
        if self.app_env.lower() == "production":
            if len(self.jwt_secret.get_secret_value()) < 32:
                raise ValueError("Production JWT_SECRET must contain at least 32 characters")
            if self.mission_password.get_secret_value() == "change-this-local-development-password":
                raise ValueError("Set a unique MISSION_PASSWORD before production")
            if (
                self.scientist_password.get_secret_value()
                == "change-this-scientist-development-password"
            ):
                raise ValueError("Set a unique SCIENTIST_PASSWORD before production")
            if self.neo4j_password.get_secret_value() == "local-neo4j-password":
                raise ValueError("Set a unique NEO4J_PASSWORD before production")
            if not self.kafka_sasl_username:
                raise ValueError("Configure Kafka SASL credentials before production")
            if not self.cassandra_username:
                raise ValueError("Configure Cassandra credentials before production")
            if not self.mongodb_uri.startswith("mongodb+srv://") and "tls=true" not in (
                self.mongodb_uri.lower()
            ):
                raise ValueError("MongoDB must use a TLS-enabled connection URI in production")
            if not self.redis_url.startswith("rediss://"):
                raise ValueError("Redis must use a rediss:// connection URL in production")
            if not self.neo4j_uri.startswith(("neo4j+s://", "bolt+s://")):
                raise ValueError("Neo4j must use a TLS-enabled connection URI in production")
            if self.cassandra_schema_bootstrap or self.neo4j_schema_bootstrap:
                raise ValueError("Disable database schema bootstrapping in production")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
