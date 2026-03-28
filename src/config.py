from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


ROOT_DIR = Path(__file__).resolve().parents[1]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    bench_source_path: Path | None = Field(default=None, alias="BENCH_SOURCE_PATH")
    data_dir: Path = Field(default=ROOT_DIR / "data", alias="DATA_DIR")
    results_dir: Path = Field(default=ROOT_DIR / "results", alias="RESULTS_DIR")

    postgres_host: str = Field(default="localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(default=55432, alias="POSTGRES_PORT")
    postgres_db: str = Field(default="avalanche_benchmark", alias="POSTGRES_DB")
    postgres_user: str = Field(default="benchmark", alias="POSTGRES_USER")
    postgres_password: str = Field(default="benchmark", alias="POSTGRES_PASSWORD")

    neo4j_uri: str = Field(default="bolt://localhost:7687", alias="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field(default="benchmark-password", alias="NEO4J_PASSWORD")
    neo4j_database: str = Field(default="neo4j", alias="NEO4J_DATABASE")
    neo4j_import_subdir: str = Field(default="", alias="NEO4J_IMPORT_SUBDIR")

    warmup_runs: int = Field(default=5, alias="WARMUP_RUNS")
    repeat_runs: int = Field(default=30, alias="REPEAT_RUNS")
    row_limit: int | None = Field(default=None, alias="ROW_LIMIT")
    workload_seed: int = Field(default=42, alias="WORKLOAD_SEED")
    result_limit: int = Field(default=100, alias="RESULT_LIMIT")
    benchmark_mode: str = Field(default="warm", alias="BENCHMARK_MODE")
    timezone: str = Field(default="UTC", alias="TIMEZONE")

    @property
    def raw_data_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def postgres_data_dir(self) -> Path:
        return self.data_dir / "postgres"

    @property
    def neo4j_data_dir(self) -> Path:
        return self.data_dir / "neo4j"

    @property
    def workloads_dir(self) -> Path:
        return self.data_dir / "workloads"

    @property
    def raw_results_dir(self) -> Path:
        return self.results_dir / "raw"

    @property
    def summary_results_dir(self) -> Path:
        return self.results_dir / "summary"

    @property
    def figures_dir(self) -> Path:
        return self.results_dir / "figures"

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} password={self.postgres_password}"
        )

    def neo4j_csv_url(self, filename: str) -> str:
        subdir = self.neo4j_import_subdir.strip().strip("/")
        if subdir:
            return f"file:///{subdir}/{filename}"
        return f"file:///{filename}"

    def ensure_directories(self) -> None:
        for path in (
            self.data_dir,
            self.raw_data_dir,
            self.postgres_data_dir,
            self.neo4j_data_dir,
            self.workloads_dir,
            self.results_dir,
            self.raw_results_dir,
            self.summary_results_dir,
            self.figures_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


def get_settings(**overrides: object) -> Settings:
    settings = Settings(**overrides)
    settings.ensure_directories()
    return settings
