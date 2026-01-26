from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str = "postgresql+psycopg://app:app@localhost:5432/app"
    app_secret_key: str = "dev-secret-change-me"
    admin_login: str = "admin"
    admin_password: str = "admin12345"
    admin_full_name: str = "Default Admin"
    max_upload_mb: int = 50
    index_kb_template_path: str = "/mnt/docs/Эталон_ИндексКБ_шаб.xlsx"
    ui_timezone: str = "Europe/Moscow"  # e.g. Europe/Moscow


settings = Settings()
