"""Centralised application settings.

Defines the AppConfig Pydantic model and instantiates a config object
from environment variables. Import config wherever configuration values
are needed.

Classes:
    AppConfig: Pydantic settings model for application configuration.

Variables:
    config: Global application configuration instance.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, List, Optional

from loguru import logger
from pydantic import Field, ValidationError, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["AppConfig", "config"]


class AppConfig(BaseSettings):  # noqa: C901 – long but mostly field declarations
    """Application configuration loaded from environment variables (.env).
    
    All settings are loaded from environment variables with validation.
    Required settings will cause validation errors if missing or invalid.
    Optional settings have sensible defaults.
    
    Attributes:
        deluge_host: Hostname of the Deluge RPC server.
        deluge_port: Port number of the Deluge RPC server.
        deluge_username: Username for Deluge RPC authentication.
        deluge_password: Password for Deluge RPC authentication.
        deluge_torrent_base_remote_folder: Base folder for torrents on the Deluge server.
        local_torrent_base_local_folder: Local base folder for torrent files.
        local_media_base_local_folder: Local base folder for media files.
        output_file: Path where output reports will be saved.
        sqlite_cache_path: Path to the SQLite database used for caching.
        cache_save_interval: How frequently to save cache during operations.
        min_file_size_mb: Minimum file size to process (in MB).
        deletion_consecutive_scans_threshold: Number of consecutive scans before deletion.
        deletion_days_threshold: Number of days before deletion is considered.
        smtp_host: SMTP server hostname.
        smtp_port: SMTP server port.
        smtp_username: SMTP authentication username.
        smtp_password: SMTP authentication password.
        smtp_from_addr: Email address to send from.
        smtp_to_addrs: List of email addresses to send reports to.
        smtp_use_ssl: Whether to use SSL for SMTP connections.
        telegram_bot_token: Telegram bot API token.
        telegram_chat_id: Telegram chat ID for notifications.
        deluge_autoremove_label: Label used by Deluge autoremove plugin.
        extensions_blacklist: List of file extensions to ignore.
        local_subfolders_blacklist: List of subfolders to ignore.
    """

    # Required Deluge connection fields
    deluge_host: str = Field(alias="DELUGE_HOST")
    deluge_port: int = Field(alias="DELUGE_PORT")
    deluge_username: str = Field(alias="DELUGE_USERNAME")
    deluge_password: str = Field(alias="DELUGE_PASSWORD")

    # Base folders
    deluge_torrent_base_remote_folder: str = Field(alias="DELUGE_TORRENT_BASE_REMOTE_FOLDER")
    local_torrent_base_local_folder: Path = Field(alias="LOCAL_TORRENT_BASE_LOCAL_FOLDER")
    local_media_base_local_folder: Path = Field(alias="LOCAL_MEDIA_BASE_LOCAL_FOLDER")

    # Output paths / files
    output_file: Path = Field(alias="OUTPUT_FILE")
    sqlite_cache_path: Path = Field(alias="APP_SQLITE_CACHE_PATH")

    # Optional thresholds and behaviour
    cache_save_interval: int = Field(default=25, alias="CACHE_SAVE_INTERVAL")
    min_file_size_mb: int = Field(default=10, alias="MIN_FILE_SIZE_MB")
    deletion_consecutive_scans_threshold: int = Field(default=7, alias="DELETION_CONSECUTIVE_SCANS_THRESHOLD")
    deletion_days_threshold: int = Field(default=7, alias="DELETION_DAYS_THRESHOLD")
    relabel_action_delay_days: int = Field(default=7, alias="RELABEL_ACTION_DELAY_DAYS")

    # SMTP / e-mail
    smtp_host: Optional[str] = Field(default=None, alias="SMTP_HOST")
    smtp_port: int = Field(default=465, alias="SMTP_PORT")
    smtp_username: Optional[str] = Field(default=None, alias="SMTP_USERNAME")
    smtp_password: Optional[str] = Field(default=None, alias="SMTP_PASSWORD")
    smtp_from_addr: Optional[str] = Field(default=None, alias="SMTP_FROM_ADDR")
    raw_smtp_to_str: Optional[str] = Field(default=None, alias="SMTP_TO_ADDRS")
    smtp_to_addrs: List[str] = Field(default_factory=list, alias="_INTERNAL_SMTP_TO_LIST")
    smtp_use_ssl: bool = Field(default=True, alias="SMTP_USE_SSL")

    # Telegram
    telegram_bot_token: Optional[str] = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: Optional[str] = Field(default=None, alias="TELEGRAM_CHAT_ID")

    # Misc
    deluge_autoremove_label: str = Field(default="othercat", alias="DELUGE_AUTOREMOVE_LABEL")

    # Shadow fields for list parsing
    raw_extensions_blacklist_str: Optional[str] = Field(default=None, alias="EXTENSIONS_BLACKLIST")
    raw_local_subfolders_blacklist_str: Optional[str] = Field(default=None, alias="LOCAL_SUBFOLDERS_BLACKLIST")
    extensions_blacklist: List[str] = Field(default_factory=list, alias="_DONT_USE_EXT_LIST")
    local_subfolders_blacklist: List[str] = Field(default_factory=list, alias="_DONT_USE_SUB_LIST")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ---------------------------------------------------------------------
    # Validators
    # ---------------------------------------------------------------------

    @field_validator(
        "local_torrent_base_local_folder",
        "local_media_base_local_folder",
        mode="before",
    )
    def _validate_directory_path(cls, v: Any, info: ValidationInfo) -> Path:
        """Validate that the path exists, is a directory, and is readable.
        
        Args:
            v: The path value to validate.
            info: Validation context information.
            
        Returns:
            Path object if validation succeeds.
            
        Raises:
            ValueError: If the path doesn't exist, isn't a directory, or isn't readable.
        """
        path_obj = Path(v)
        if not path_obj.exists():
            raise ValueError(f"Path for {info.field_name} does not exist: {v}")
        if not path_obj.is_dir():
            raise ValueError(f"Path for {info.field_name} is not a directory: {v}")
        if not os.access(path_obj, os.R_OK):
            raise ValueError(f"Cannot read directory for {info.field_name}: {v}")
        return path_obj

    @field_validator("output_file", mode="before")
    def _validate_output_file_parent(cls, v: Any, info: ValidationInfo) -> Path:
        path_obj = Path(v)
        parent_dir = path_obj.parent
        if not parent_dir.exists():
            raise ValueError(f"Parent directory for {info.field_name} does not exist: {parent_dir}")
        if not parent_dir.is_dir():
            raise ValueError(f"Parent path for {info.field_name} is not a directory: {parent_dir}")
        if not os.access(parent_dir, os.W_OK):
            raise ValueError(f"Parent directory for {info.field_name} is not writable: {parent_dir}")
        return path_obj

    @field_validator("sqlite_cache_path", mode="before")
    def _validate_sqlite_cache_path(cls, v: Any, info: ValidationInfo) -> Path:
        path_obj = Path(v)
        parent_dir = path_obj.parent
        if not parent_dir.exists():
            raise ValueError(f"Parent directory for {info.field_name} does not exist: {parent_dir}")
        if not parent_dir.is_dir():
            raise ValueError(f"Parent path for {info.field_name} is not a directory: {parent_dir}")
        if not os.access(parent_dir, os.W_OK):
            raise ValueError(f"Parent directory for {info.field_name} is not writable: {parent_dir}")
        return path_obj

    @model_validator(mode="after")
    def _populate_parsed_lists(self) -> "AppConfig":
        default_ext_str = ".nfo,.srt,.jpg,.sfv,.txt,.png,.sub,.torrent,.plexmatch,.m3u,.json,.webp,.jpeg,.obj,.ini,.dtshd,.invalid"
        default_sub_str = "music,ebooks,courses"

        effective_ext_str = self.raw_extensions_blacklist_str or default_ext_str
        self.extensions_blacklist = [item.strip().lower() for item in effective_ext_str.split(",") if item.strip()]

        effective_sub_str = self.raw_local_subfolders_blacklist_str or default_sub_str
        self.local_subfolders_blacklist = [item.strip() for item in effective_sub_str.split(",") if item.strip()]

        # SMTP recipients
        if self.raw_smtp_to_str:
            self.smtp_to_addrs = [addr.strip() for addr in self.raw_smtp_to_str.split(",") if addr.strip()]
        return self


# -------------------------------------------------------------------------
# Instantiate config
# -------------------------------------------------------------------------
try:
    config = AppConfig()
    logger.info("Configuration loaded successfully from environment.")
except ValidationError as exc:  # noqa: BLE001
    logger.error("!!! Configuration Error !!!")
    for detail in exc.errors():
        variable_name = detail.get("loc", ["Unknown"])[0]
        message = detail.get("msg", "Unknown error")
        logger.error("  %s: %s", variable_name, message)
    sys.exit(1)
