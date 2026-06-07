"""
ETF_data 统一配置加载器
用法:
    from etf_data.config_helper import init, get_config, get_db_conn_str, get_tushare_api
    init()
    db_cfg = get_config("tushare_db")
    conn_str = get_db_conn_str("tushare")
    api = get_tushare_api()
"""
import os
import re
from pathlib import Path

import yaml

_CONFIG_DIR = Path(__file__).parent.parent.parent / "config"
_config_cache: dict = {}


def _load_env() -> dict:
    """从 .env 加载环境变量（已存在的不过载）"""
    env_file = _CONFIG_DIR.parent / ".env"
    if not env_file.exists():
        return {}
    env_vars = {}
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key not in os.environ:
                os.environ[key] = val
            env_vars[key] = val
    return env_vars


def _interpolate_env(value):
    """替换字符串中的 ${VAR} 占位符"""
    if not isinstance(value, str):
        return value

    def _replace(match):
        var_name = match.group(1)
        return os.environ.get(var_name, match.group(0))

    return re.sub(r"\$\{(\w+)\}", _replace, value)


def _interpolate_dict(obj):
    """递归替换字典中所有 ${VAR} 占位符"""
    if isinstance(obj, dict):
        return {k: _interpolate_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_interpolate_dict(i) for i in obj]
    if isinstance(obj, str):
        return _interpolate_env(obj)
    return obj


def init():
    """加载所有配置，返回配置字典"""
    global _config_cache
    _load_env()

    config_files = ["etf_data_config.yaml"]
    merged = {}

    for cf in config_files:
        path = _CONFIG_DIR / cf
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f)
            if data:
                merged.update(data)

    _config_cache = _interpolate_dict(merged)
    return _config_cache


def get_config(section: str | None = None):
    """
    获取配置段或全部配置。
    
    section: "database.tushare" | "tushare_api" 等
    """
    if not _config_cache:
        init()
    if section is None:
        return _config_cache
    parts = section.split(".")
    val = _config_cache
    for p in parts:
        val = val[p]
    return val


def get_db_conn_str(section: str = "tushare"):
    """获取数据库连接字符串"""
    db = get_config(f"database.{section}")
    return (
        f"DRIVER={{{db['driver']}}};"
        f"SERVER={db['server']},{db['port']};"
        f"DATABASE={db['database']};"
        f"UID={db['username']};"
        f"PWD={db['password']};"
        f"TrustServerCertificate={'yes' if db.get('trust_server_cert') else 'no'};"
    )


def get_tushare_api(token: str | None = None, http_url: str | None = None):
    """获取 Tushare pro 接口"""
    import tushare as ts
    cfg = get_config("tushare_api")
    tok = token or cfg["token"]
    url = http_url or cfg["http_url"]
    pro = ts.pro_api(tok)
    pro._DataApi__http_url = url
    return pro
