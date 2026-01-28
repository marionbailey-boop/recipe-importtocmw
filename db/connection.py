import os
from typing import Optional

import pyodbc
import requests
from dotenv import load_dotenv

load_dotenv()


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


def build_connection_string() -> str:
    """
    Build a pyodbc connection string.

    Preferred:
      - DB_CONNECTION_STRING (or CMWEB_CONNECTION_STRING) for a full string.

    Fallback (split fields):
      - DB_DRIVER, DB_SERVER, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    direct = os.getenv("DB_CONNECTION_STRING") or os.getenv("CMWEB_CONNECTION_STRING")
    if direct:
        return direct

    driver = _required_env("DB_DRIVER")
    server = _required_env("DB_SERVER")
    port = os.getenv("DB_PORT")
    if port and "," not in server:
        server = f"{server},{port}"
    database = _required_env("DB_NAME")
    uid = _required_env("DB_USER")
    pwd = _required_env("DB_PASSWORD")

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={uid};"
        f"PWD={pwd};"
        "TrustServerCertificate=yes;"
    )


CONNECTION_STRING = build_connection_string()

_CACHED_CONNECTION_STRING: Optional[str] = None


def fetch_conn_str(apikey: str) -> str:
    url = os.getenv("CMWEB_CONNSTR_URL", "http://192.168.1.23:8006/get-connection-string")
    r = requests.get(url, params={"apikey": apikey}, timeout=15)
    r.raise_for_status()
    return r.text.strip().strip('"')


def get_connection(apikey: Optional[str] = None) -> pyodbc.Connection:
    global _CACHED_CONNECTION_STRING

    key = apikey or os.getenv("CMWEB_APIKEY")
    if key:
        if _CACHED_CONNECTION_STRING is None:
            _CACHED_CONNECTION_STRING = fetch_conn_str(key)
        return pyodbc.connect(_CACHED_CONNECTION_STRING)

    return pyodbc.connect(CONNECTION_STRING)
