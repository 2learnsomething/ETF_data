"""AmazingData 管道配置管理

从 YAML 配置文件 + .env 环境变量加载配置。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


# 默认配置（可被 YAML 文件覆盖）
DEFAULT_CONFIG: dict[str, Any] = {
    "amazingdata": {
        "username": "",
        "password": "",
        "host": "120.86.124.106",
        "port": 8600,
    },
    "storage": {
        "root": "/mnt/etf_data",
    },
    "pipeline": {
        "batch_size_etf_kline": 50,
        "batch_size_etf_min1": 10,
        "batch_size_a_stock": 100,
        "batch_size_financial": 500,
        "cache_dir": "/tmp/ad_cache",
        "retry_times": 3,
        "retry_delay": 5,
    },
    "fallback": {
        "enabled": True,
        "prefer_efinance": True,
        "prefer_baostock": True,
        "prefer_akshare": True,
    },
    "notify": {
        "serverchan_enabled": True,
        "qq_enabled_for_critical": True,
    },
}


class Config:
    """配置管理器，单例模式"""

    _instance: Config | None = None

    def __new__(cls) -> Config:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._data: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        """按优先级加载配置：默认值 ← YAML ← 环境变量"""
        self._data = dict(DEFAULT_CONFIG)

        # 项目根目录：ETF_DATA_CONFIG_DIR 环境变量 > dev 模式
        cfg_dir = os.environ.get("ETF_DATA_CONFIG_DIR")
        if cfg_dir:
            root = Path(cfg_dir).parent
        else:
            root = Path(__file__).parent.parent.parent

        # 加载 YAML 配置
        yaml_path = root / "config" / "amazingdata_pipeline.yaml"
        if yaml_path.exists():
            with open(yaml_path) as f:
                overrides = yaml.safe_load(f) or {}
            self._deep_merge(self._data, overrides)

        # .env 文件（从项目根或 hermes home）
        self._try_load_dotenv(root)
        hermes_env = Path(os.environ.get("HERMES_HOME", "~/.hermes")).expanduser() / ".env"
        if hermes_env != root / ".env":
            self._try_load_dotenv(hermes_env.parent)

        # 环境变量覆盖（优先于 YAML）
        if os.environ.get("AMAZINGDATA_USERNAME"):
            self._data["amazingdata"]["username"] = os.environ["AMAZINGDATA_USERNAME"]
        if os.environ.get("AMAZINGDATA_PASSWORD"):
            self._data["amazingdata"]["password"] = os.environ["AMAZINGDATA_PASSWORD"]
        if os.environ.get("AMAZINGDATA_HOST"):
            self._data["amazingdata"]["host"] = os.environ["AMAZINGDATA_HOST"]
        if os.environ.get("AMAZINGDATA_PORT"):
            self._data["amazingdata"]["port"] = int(os.environ["AMAZINGDATA_PORT"])
        if os.environ.get("ETF_DATA_ROOT"):
            self._data["storage"]["root"] = os.environ["ETF_DATA_ROOT"]

    @staticmethod
    def _try_load_dotenv(parent_dir: Path) -> None:
        """尝试加载 .env 文件"""
        env_path = parent_dir / ".env"
        if not env_path.exists():
            return
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            if key.strip() and val.strip() and not os.environ.get(key.strip()):
                os.environ[key.strip()] = val.strip()

    @staticmethod
    def _deep_merge(base: dict, override: dict) -> None:
        """递归合并字典"""
        for k, v in override.items():
            if k in base and isinstance(base[k], dict) and isinstance(v, dict):
                Config._deep_merge(base[k], v)
            else:
                base[k] = v

    def get(self, key: str, default: Any = None) -> Any:
        """获取配置值，支持点号分隔的路径，如 'amazingdata.host'"""
        parts = key.split(".")
        val = self._data
        for p in parts:
            if isinstance(val, dict):
                val = val.get(p)
            else:
                return default
        return val if val is not None else default

    @property
    def storage_root(self) -> str:
        return str(self.get("storage.root", "/mnt/etf_data"))

    @property
    def amazingdata_username(self) -> str:
        return str(self.get("amazingdata.username", ""))

    @property
    def amazingdata_password(self) -> str:
        return str(self.get("amazingdata.password", ""))

    @property
    def cache_dir(self) -> str:
        path = str(self.get("pipeline.cache_dir", "/tmp/ad_cache"))
        Path(path).mkdir(parents=True, exist_ok=True)
        return path

    @property
    def amazingdata_host(self) -> str:
        return str(self.get("amazingdata.host", "120.86.124.106"))

    @property
    def amazingdata_port(self) -> int:
        return int(self.get("amazingdata.port", 8600))
