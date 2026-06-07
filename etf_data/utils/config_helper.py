""""
ETF_data 统一配置加载器

配置目录查找优先级：
  1. init(config_dir=...) 显式传入
  2. 环境变量 ETF_DATA_CONFIG_DIR
  3. dev 模式：项目根目录下的 config/

用法:
    from etf_data.config_helper import init, get_config, get_db_conn_str, get_tushare_api
    init()                              # dev 模式
    init(config_dir="/etc/etf_data")    # 生产部署
    init()                              # 或设置 export ETF_DATA_CONFIG_DIR=/etc/etf_data
    db_cfg = get_config("tushare_db")
    conn_str = get_db_conn_str("tushare")
    api = get_tushare_api()
"""
import os
import re
from pathlib import Path

import yaml

_config_cache: dict = {}


def _resolve_config_dir(config_dir: str | None = None) -> Path:
    """解析配置目录：显式参数 > 环境变量 > dev 模式相对路径。"""
    if config_dir:
        return Path(config_dir)
    env_val = os.environ.get("ETF_DATA_CONFIG_DIR")
    if env_val:
        return Path(env_val)
    # dev 模式：项目根目录 /config/
    return Path(__file__).resolve().parent.parent.parent / "config"


def _resolve_project_root(config_dir: str | None = None) -> Path:
    """项目根目录 = 配置目录的父目录。"""
    return _resolve_config_dir(config_dir).parent


def _load_env(config_dir: str | None = None) -> dict:
    """从 .env 加载环境变量（已存在的不过载）"""
    env_file = _resolve_project_root(config_dir) / ".env"
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


def init(config_dir: str | None = None):
    """加载所有配置，返回配置字典

    Args:
        config_dir: 配置目录路径。默认取 ETF_DATA_CONFIG_DIR 环境变量，
                    未设置时使用 dev 模式的项目根目录下 config/。
    """
    global _config_cache
    _load_env(config_dir)

    cfg_dir = _resolve_config_dir(config_dir)
    config_files = ["etf_data_config.yaml"]
    merged = {}

    for cf in config_files:
        path = cfg_dir / cf
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
