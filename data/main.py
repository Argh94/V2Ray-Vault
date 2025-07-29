import os
import asyncio
import json
import re
import base64
import logging
import random
import string
import math
import socket
from pathlib import Path
from typing import List, Dict, Set, Optional, Any, Tuple, Coroutine
from urllib.parse import urlparse, parse_qs, unquote
import ipaddress
from collections import Counter

import httpx
import aiofiles
import jdatetime

try:
    import geoip2.database
except ImportError:
    print("Error: 'geoip2' library not found. Please run: pip install geoip2")
    exit(1)

try:
    from rich.console import Console
    from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, SpinnerColumn
    from rich.table import Table
    from rich.panel import Panel
except ImportError:
    print("Error: 'rich' library not found. Please run: pip install rich")
    exit(1)

from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel, Field, model_validator, ValidationError

class AppConfig:
    BASE_DIR = Path(__file__).parent
    DATA_DIR = BASE_DIR / "data"
    OUTPUT_DIR = BASE_DIR / "sub"  # حالا sub تو ریشه پروژه ساخته می‌شه

    DIRS = {
        "splitted": OUTPUT_DIR / "splitted",
        "security": OUTPUT_DIR / "security",
        "protocols": OUTPUT_DIR / "protocols",
        "networks": OUTPUT_DIR / "networks",
        "subscribe": OUTPUT_DIR / "subscribe",
        "countries": OUTPUT_DIR / "countries",
        "datacenters": OUTPUT_DIR / "datacenters",
        "channel_subs": OUTPUT_DIR / "channel_subs",
        "mix_protocol": OUTPUT_DIR / "mix_protocol",
    }

    TELEGRAM_CHANNELS_FILE = DATA_DIR / "telegram_channels.json"
    SUBSCRIPTION_LINKS_FILE = DATA_DIR / "subscription_links.json"
    LAST_UPDATE_FILE = DATA_DIR / "last_update.log"
    SEEN_CONFIGS_FILE = DATA_DIR / "seen_configs.json"
    TELEGRAM_REPORT_FILE = DATA_DIR / "telegram_report.log"
    GEOIP_DB_FILE = DATA_DIR / "GeoLite2-Country.mmdb"
    GEOIP_ASN_DB_FILE = DATA_DIR / "GeoLite2-ASN.mmdb"

    REMOTE_CHANNELS_URL = "https://raw.githubusercontent.com/Argh94/V2Ray-Vault/main/data/telegram_channels.json"
    REMOTE_SUBS_URL = "https://raw.githubusercontent.com/Argh94/V2Ray-Vault/main/data/subscription_links.json"
    GEOIP_DB_URL = "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-Country.mmdb"
    GEOIP_ASN_DB_URL = "https://github.com/P3TERX/GeoLite.mmdb/raw/download/GeoLite2-ASN.mmdb"

    HTTP_TIMEOUT = 25.0
    HTTP_MAX_REDIRECTS = 5
    HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"}
    MAX_CONCURRENT_REQUESTS = 20

    TELEGRAM_BASE_URL = "https://t.me/s/{}"
    TELEGRAM_MESSAGE_LIMIT = 50
    TELEGRAM_IGNORE_LAST_UPDATE = True
    MAX_CONFIGS_PER_CHANNEL = 400 

    ENABLE_SUBSCRIPTION_FETCHING = True
    ENABLE_IP_DEDUPLICATION = True
    ENABLE_SEEN_CONFIG_FILTER = False
    SEEN_CONFIG_TIMEOUT_HOURS = 1
    
    ENABLE_CONNECTIVITY_TEST = False 
    CONNECTIVITY_TEST_TIMEOUT = 4
    MAX_CONNECTIVITY_TESTS = 250

    ADD_SIGNATURES = True
    ADV_SIGNATURE = "This is a free trial project."
    DNT_SIGNATURE = "For enhanced GitHub access, visit Argh94 now!"
    DEV_SIGNATURE = "</> Collector v5.0"
    CUSTOM_SIGNATURE = "「 GitHub Argh94 ☕ 」"

CONFIG = AppConfig()
console = Console()

def setup_logger():
    logging.basicConfig(level=logging.INFO, format='%(message)s', datefmt="[%X]", handlers=[])
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("geoip2").setLevel(logging.WARNING)
    return logging.getLogger("V2RayCollector")

logger = setup_logger()

class V2RayCollectorException(Exception): pass
class ParsingError(V2RayCollectorException): pass
class NetworkError(V2RayCollectorException): pass

COUNTRY_CODE_TO_FLAG = {
    'AD': '🇦🇩', 'AE': '🇦🇪', 'AF': '🇦🇫', 'AG': '🇦🇬', 'AI': '🇦🇮', 'AL': '🇦🇱', 'AM': '🇦🇲', 'AO': '🇦🇴', 'AQ': '🇦🇶', 'AR': '🇦🇷', 'AS': '🇦🇸', 'AT': '🇦🇹', 'AU': '🇦🇺', 'AW': '🇦🇼', 'AX': '🇦🇽', 'AZ': '🇦🇿', 'BA': '🇧🇦', 'BB': '🇧🇧',
    'BD': '🇧🇩', 'BE': '🇧🇪', 'BF': '🇧🇫', 'BG': '🇧🇬', 'BH': '🇧🇭', 'BI': '🇧🇮', 'BJ': '🇧🇯', 'BL': '🇧🇱', 'BM': '🇧🇲', 'BN': '🇧🇳', 'BO': '🇧🇴', 'BR': '🇧🇷', 'BS': '🇧🇸', 'BT': '🇧🇹', 'BW': '🇧🇼', 'BY': '🇧🇾', 'BZ': '🇧🇿', 'CA': '🇨🇦',
    'CC': '🇨🇨', 'CD': '🇨🇩', 'CF': '🇨🇫', 'CG': '🇨🇬', 'CH': '🇨🇭', 'CI': '🇨🇮', 'CK': '🇨🇰', 'CL': '🇨🇱', 'CM': '🇨🇲', 'CN': '🇨🇳', 'CO': '🇨🇴', 'CR': '🇨🇷', 'CU': '🇨🇺', 'CV': '🇨🇻', 'CW': '🇨🇼', 'CX': '🇨🇽', 'CY': '🇨🇾', 'CZ': '🇨🇿',
    'DE': '🇩🇪', 'DJ': '🇩🇯', 'DK': '🇩🇰', 'DM': '🇩🇲', 'DO': '🇩🇴', 'DZ': '🇩🇿', 'EC': '🇪🇨', 'EE': '🇪🇪', 'EG': '🇪🇬', 'ER': '🇪🇷', 'ES': '🇪🇸', 'ET': '🇪🇹', 'FI': '🇫🇮', 'FJ': '🇫🇯', 'FK': '🇫🇰', 'FM': '🇫🇲', 'FO': '🇫🇴', 'FR': '🇫🇷',
    'GA': '🇬🇦', 'GB': '🇬🇧', 'GD': '🇬🇩', 'GE': '🇬🇪', 'GF': '🇬🇫', 'GG': '🇬🇬', 'GH': '🇬🇭', 'GI': '🇬🇮', 'GL': '🇬🇱', 'GM': '🇬🇲', 'GN': '🇬🇳', 'GP': '🇬🇵', 'GQ': '🇬🇶', 'GR': '🇬🇷', 'GS': '🇬🇸', 'GT': '🇬🇹', 'GU': '🇬🇺', 'GW': '🇬🇼',
    'GY': '🇬🇾', 'HK': '🇭🇰', 'HN': '🇭🇳', 'HR': '🇭🇷', 'HT': '🇭🇹', 'HU': '🇭🇺', 'ID': '🇮🇩', 'IE': '🇮🇪', 'IL': '🇮🇱', 'IM': '🇮🇲', 'IN': '🇮🇳', 'IO': '🇮🇴', 'IQ': '🇮🇶', 'IR': '🇮🇷', 'IS': '🇮🇸', 'IT': '🇮🇹', 'JE': '🇯🇪', 'JM': '🇯🇲',
    'JO': '🇯🇴', 'JP': '🇯🇵', 'KE': '🇰🇪', 'KG': '🇰🇬', 'KH': '🇰🇭', 'KI': '🇰🇮', 'KM': '🇰🇲', 'KN': '🇰🇳', 'KP': '🇰🇵', 'KR': '🇰🇷', 'KW': '🇰🇼', 'KY': '🇰🇾', 'KZ': '🇰🇿', 'LA': '🇱🇦', 'LB': '🇱🇧', 'LC': '🇱🇨', 'LI': '🇱🇮', 'LK': '🇱🇰',
    'LR': '🇱🇷', 'LS': '🇱🇸', 'LT': '🇱🇹', 'LU': '🇱🇺', 'LV': '🇱🇻', 'LY': '🇱🇾', 'MA': '🇲🇦', 'MC': '🇲🇨', 'MD': '🇲🇩', 'ME': '🇲🇪', 'MF': '🇲🇫', 'MG': '🇲🇬', 'MH': '🇲🇭', 'MK': '🇲🇰', 'ML': '🇲🇱', 'MM': '🇲🇲', 'MN': '🇲🇳', 'MO': '🇲🇴',
    'MP': '🇲🇵', 'MQ': '🇲🇶', 'MR': '🇲🇷', 'MS': '🇲🇸', 'MT': '🇲🇹', 'MU': '🇲🇺', 'MV': '🇲🇻', 'MW': '🇲🇼', 'MX': '🇲🇽', 'MY': '🇲🇾', 'MZ': '🇲🇿', 'NA': '🇳🇦', 'NC': '🇳🇨', 'NE': '🇳🇪', 'NF': '🇳🇫', 'NG': '🇳🇬', 'NI': '🇳🇮', 'NL': '🇳🇱',
    'NO': '🇳🇴', 'NP': '🇳🇵', 'NR': '🇳🇷', 'NU': '🇳🇺', 'NZ': '🇳🇿', 'OM': '🇴🇲', 'PA': '🇵🇦', 'PE': '🇵🇪', 'PF': '🇵🇫', 'PG': '🇵🇬', 'PH': '🇵🇭', 'PK': '🇵🇰', 'PL': '🇵🇱', 'PM': '🇵🇲', 'PN': '🇵🇳', 'PR': '🇵🇷', 'PS': '🇵🇸', 'PT': '🇵🇹',
    'PW': '🇵🇼', 'PY': '🇵🇾', 'QA': '🇶🇦', 'RE': '🇷🇪', 'RO': '🇷🇴', 'RS': '🇷🇸', 'RU': '🇷🇺', 'RW': '🇷🇼', 'SA': '🇸🇦', 'SB': '🇸🇧', 'SC': '🇸🇨', 'SD': '🇸🇩', 'SE': '🇸🇪', 'SG': '🇸🇬', 'SH': '🇸🇭', 'SI': '🇸🇮', 'SJ': '🇸🇯', 'SK': '🇸🇰',
    'SL': '🇸🇱', 'SM': '🇸🇲', 'SN': '🇸🇳', 'SO': '🇸🇴', 'SR': '🇸🇷', 'SS': '🇸🇸', 'ST': '🇸🇹', 'SV': '🇸🇻', 'SX': '🇸🇽', 'SY': '🇸🇾', 'SZ': '🇸🇿', 'TC': '🇹🇨', 'TD': '🇹🇩', 'TF': '🇹🇫', 'TG': '🇹🇬', 'TH': '🇹🇭', 'TJ': '🇹🇯', 'TK': '🇹🇰',
    'TL': '🇹🇱', 'TM': '🇹🇲', 'TN': '🇹🇳', 'TO': '🇹🇴', 'TR': '🇹🇷', 'TT': '🇹🇹', 'TV': '🇹🇻', 'TW': '🇹🇼', 'TZ': '🇹🇿', 'UA': '🇺🇦', 'UG': '🇺🇬', 'US': '🇺🇸', 'UY': '🇺🇾', 'UZ': '🇺🇿', 'VA': '🇻🇦', 'VC': '🇻🇨', 'VE': '🇻🇪', 'VG': '🇻🇬',
    'VI': '🇻🇮', 'VN': '🇻🇳', 'VU': '🇻🇺', 'WF': '🇼🇫', 'WS': '🇼🇸', 'YE': '🇾🇪', 'YT': '🇾🇹', 'ZA': '🇿🇦', 'ZM': '🇿🇲', 'ZW': '🇿🇼', 'XX': '🏳️'
}

def b64_decode(s: str) -> str:
    s += '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s).decode('utf-8')

def b64_encode(s: str) -> str:
    return base64.urlsafe_b64encode(s.encode('utf-8')).rstrip(b'=').decode('utf-8')

def is_valid_base64(s: str) -> bool:
    try:
        s_padded = s + '=' * (-len(s) % 4)
        return base64.b64encode(base64.b64decode(s_padded)).decode('utf-8') == s_padded
    except (ValueError, TypeError):
        return False

def get_iran_timezone():
    return timezone(timedelta(hours=3, minutes=30))

def generate_random_uuid_string() -> str:
    return '-'.join([''.join(random.choices(string.ascii_lowercase + string.digits, k=k)) for k in [8, 4, 4, 4, 12]])

def is_ip_address(address: str) -> bool:
    try:
        ipaddress.ip_address(address)
        return True
    except ValueError:
        return False

class BaseConfig(BaseModel):
    model_config = {'str_strip_whitespace': True}
    protocol: str
    host: str
    port: int
    uuid: str
    remarks: str = "N/A"
    network: str = 'tcp'
    security: str = 'none'
    path: Optional[str] = None
    sni: Optional[str] = None
    fingerprint: Optional[str] = None
    country: Optional[str] = Field("XX", exclude=True)
    source_type: str = Field("unknown", exclude=True)
    ping: Optional[int] = Field(None, exclude=True)
    asn_org: Optional[str] = Field(None, exclude=True)

    def get_deduplication_key(self) -> str:
        return f"{self.protocol}:{self.host}:{self.port}:{self.uuid}"

    def to_uri(self) -> str:
        raise NotImplementedError

class VmessConfig(BaseConfig):
    protocol: str = 'vmess'
    source_type: str = 'vmess'
    ps: str
    add: str
    v: Any = "2"
    aid: int = 0
    scy: str = 'auto'
    net: str
    type: str = 'none'
    tls: str = ''

    @model_validator(mode='before')
    def map_fields(cls, values):
        values['remarks'] = values.get('ps', 'N/A')
        values['host'] = values.get('add', '')
        values['uuid'] = values.get('id', '')
        values['network'] = values.get('net', 'tcp')
        values['security'] = values.get('tls') or 'none'
        values['v'] = str(values.get('v', '2'))
        return values

    def to_uri(self) -> str:
        vmess_data = {"v": self.v, "ps": self.remarks, "add": self.host, "port": self.port, "id": self.uuid, "aid": self.aid, "scy": self.scy, "net": self.network, "type": self.type, "host": self.sni, "path": self.path, "tls": self.security if self.security != 'none' else '', "sni": self.sni}
        vmess_data_clean = {k: v for k, v in vmess_data.items() if v is not None and v != ""}
        json_str = json.dumps(vmess_data_clean, separators=(',', ':'))
        encoded = base64.b64encode(json_str.encode('utf-8')).decode('utf-8').rstrip("=")
        return f"vmess://{encoded}"

class VlessConfig(BaseConfig):
    protocol: str = 'vless'
    flow: Optional[str] = None
    pbk: Optional[str] = None
    sid: Optional[str] = None

    def to_uri(self) -> str:
        params = {'type': self.network, 'security': self.security, 'path': self.path, 'sni': self.sni, 'fp': self.fingerprint, 'flow': self.flow, 'pbk': self.pbk, 'sid': self.sid}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v is not None and v != ""])
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"vless://{self.uuid}@{self.host}:{self.port}?{query_string}{remarks_encoded}"

class TrojanConfig(BaseConfig):
    protocol: str = 'trojan'
    source_type: str = 'trojan'

    def to_uri(self) -> str:
        params = {'sni': self.sni, 'peer': self.sni, 'security': self.security}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v is not None])
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"trojan://{self.uuid}@{self.host}:{self.port}?{query_string}{remarks_encoded}"

class ShadowsocksConfig(BaseConfig):
    protocol: str = 'shadowsocks'
    source_type: str = 'shadowsocks'
    method: str

    @model_validator(mode='before')
    def map_fields(cls, values):
        values['uuid'] = values.get('password', '')
        return values

    def to_uri(self) -> str:
        user_info = f"{self.method}:{self.uuid}"
        encoded_user_info = base64.b64encode(user_info.encode('utf-8')).decode('utf-8').rstrip('=')
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"ss://{encoded_user_info}@{self.host}:{self.port}{remarks_encoded}"

class Hysteria2Config(BaseConfig):
    protocol: str = 'hysteria2'
    insecure: Optional[int] = None
    obfs: Optional[str] = None
    obfs_password: Optional[str] = Field(None, alias='obfs-password')

    def to_uri(self) -> str:
        params = {'sni': self.sni, 'insecure': self.insecure, 'obfs': self.obfs, 'obfs-password': self.obfs_password}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v is not None])
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"hysteria2://{self.uuid}@{self.host}:{self.port}?{query_string}{remarks_encoded}"

class TuicConfig(BaseConfig):
    protocol: str = 'tuic'
    password: str
    congestion_control: Optional[str] = Field('bbr', alias='congestion_control')
    udp_relay_mode: Optional[str] = Field('native', alias='udp_relay_mode')
    alpn: Optional[str] = None

    def to_uri(self) -> str:
        params = {'sni': self.sni, 'congestion_control': self.congestion_control, 'udp_relay_mode': self.udp_relay_mode, 'alpn': self.alpn}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v is not None])
        remarks_encoded = f"#{unquote(self.remarks)}"
        return f"tuic://{self.uuid}:{self.password}@{self.host}:{self.port}?{query_string}{remarks_encoded}"

class ShadowsocksRConfig(BaseConfig):
    protocol: str = 'shadowsocksr'
    source_type: str = 'shadowsocksr'
    method: str
    obfs: str
    protocol_param: Optional[str] = Field(None, alias='protoparam')
    obfs_param: Optional[str] = Field(None, alias='obfsparam')

    def to_uri(self) -> str:
        main_part = f"{self.host}:{self.port}:{self.protocol}:{self.method}:{self.obfs}:{b64_encode(self.uuid)}"
        params = {'obfsparam': self.obfs_param, 'protoparam': self.protocol_param, 'remarks': b64_encode(self.remarks)}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v])
        return f"ssr://{b64_encode(f'{main_part}/?{query_string}')}"

class WireGuardConfig(BaseConfig):
    protocol: str = 'wireguard'
    source_type: str = 'wireguard'
    private_key: Optional[str] = None
    public_key: Optional[str] = None
    preshared_key: Optional[str] = None
    ip: Optional[str] = None

    def to_uri(self) -> str:
        params = {'public_key': self.public_key, 'preshared_key': self.preshared_key, 'ip': self.ip}
        query_string = '&'.join([f"{k}={v}" for k, v in params.items() if v])
        remarks_encoded = f"#{unquote(self.remarks)}"
        if self.private_key:
            return f"wg://{self.private_key}@{self.host}:{self.port}?{query_string}{remarks_encoded}"
        else:
            return f"warp://{self.host}:{self.port}?{query_string}{remarks_encoded}"

class AsyncHttpClient:
    _client: Optional[httpx.AsyncClient] = None

    @classmethod
    async def get_client(cls) -> httpx.AsyncClient:
        if cls._client is None or cls._client.is_closed:
            limits = httpx.Limits(max_connections=CONFIG.MAX_CONCURRENT_REQUESTS, max_keepalive_connections=20)
            cls._client = httpx.AsyncClient(headers=CONFIG.HTTP_HEADERS, timeout=CONFIG.HTTP_TIMEOUT, max_redirects=CONFIG.HTTP_MAX_REDIRECTS, http2=True, follow_redirects=True, limits=limits)
        return cls._client

    @classmethod
    async def close(cls):
        if cls._client and not cls._client.is_closed:
            await cls._client.aclose()
            cls._client = None

    @classmethod
    async def get(cls, url: str) -> Tuple[int, str]:
        client = await cls.get_client()
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.status_code, response.text
        except httpx.RequestError as e:
            raise NetworkError(f"Failed to fetch {url}") from e
        except httpx.HTTPStatusError as e:
            return e.response.status_code, e.response.text

class V2RayParser:
    @staticmethod
    def parse(uri: str, source_type: str = "unknown") -> Optional[BaseConfig]:
        uri = uri.strip()
        if not uri:
            return None
            
        parsed_config: Optional[BaseConfig] = None
        try:
            if uri.startswith("vmess://"): parsed_config = V2RayParser._parse_vmess(uri)
            elif uri.startswith("vless://"): parsed_config = V2RayParser._parse_vless(uri)
            elif uri.startswith("trojan://"): parsed_config = V2RayParser._parse_trojan(uri)
            elif uri.startswith("ss://"): parsed_config = V2RayParser._parse_shadowsocks(uri)
            elif uri.startswith("hy2://") or uri.startswith("hysteria2://"): parsed_config = V2RayParser._parse_hysteria2(uri)
            elif uri.startswith("tuic://"): parsed_config = V2RayParser._parse_tuic(uri)
            elif uri.startswith("ssr://"): parsed_config = V2RayParser._parse_shadowsocksr(uri)
            elif uri.startswith("wg://") or uri.startswith("warp://"): parsed_config = V2RayParser._parse_wireguard(uri)

            if parsed_config:
                parsed_config.source_type = source_type
            return parsed_config
        except (ValidationError, ParsingError):
            return None
        except Exception:
            return None

    @staticmethod
    def _parse_vmess(uri: str) -> Optional[VmessConfig]:
        b64_data = uri[len("vmess://"):]
        if not is_valid_base64(b64_data): return None
        data = json.loads(base64.b64decode(b64_data + '==').decode('utf-8'))
        return VmessConfig(**data)

    @staticmethod
    def _parse_vless(uri: str) -> Optional[VlessConfig]:
        try:
            parsed_url = urlparse(uri)
            if not parsed_url.hostname or not parsed_url.port:
                raise ParsingError(f"Missing hostname or port in VLESS URI.")

            params = parse_qs(parsed_url.query)
            return VlessConfig(
                uuid=parsed_url.username, 
                host=parsed_url.hostname, 
                port=parsed_url.port, 
                remarks=unquote(parsed_url.fragment) if parsed_url.fragment else f"{parsed_url.hostname}:{parsed_url.port}",
                network=params.get('type', ['tcp'])[0], 
                security=params.get('security', ['none'])[0], 
                path=unquote(params.get('path', [None])[0]) if params.get('path') else None, 
                sni=params.get('sni', [None])[0], 
                fingerprint=params.get('fp', [None])[0], 
                flow=params.get('flow', [None])[0], 
                pbk=params.get('pbk', [None])[0], 
                sid=params.get('sid', [None])[0]
            )
        except (ValueError, TypeError, AttributeError) as e:
            raise ParsingError(f"Could not parse VLESS link correctly: {uri[:60]}") from e

    @staticmethod
    def _parse_trojan(uri: str) -> Optional[TrojanConfig]:
        parsed_url = urlparse(uri)
        if not parsed_url.hostname or not parsed_url.port:
            raise ParsingError(f"Missing hostname or port in Trojan URI.")
        params = parse_qs(parsed_url.query)
        return TrojanConfig(uuid=parsed_url.username, host=parsed_url.hostname, port=parsed_url.port, remarks=unquote(parsed_url.fragment) if parsed_url.fragment else f"{parsed_url.hostname}:{parsed_url.port}", security=params.get('security', ['tls'])[0], sni=params.get('sni', [None])[0], network='tcp')

    @staticmethod
    def _parse_shadowsocks(uri: str) -> Optional[ShadowsocksConfig]:
        try:
            main_part, remarks_part = (uri[len("ss://"):].split('#', 1) + [''])[:2]
            remarks = unquote(remarks_part) if remarks_part else ''
            
            if '@' not in main_part:
                raise ParsingError("Invalid SS URI format: missing '@'.")
                
            user_info_part, host_port_part = main_part.split('@', 1)
            decoded_user_info = base64.b64decode(user_info_part + '==').decode('utf-8')
            
            if ':' not in decoded_user_info or ':' not in host_port_part:
                raise ParsingError("Invalid SS URI format: missing method/password or host/port separator.")
                
            method, password = decoded_user_info.split(':', 1)
            host, port_str = host_port_part.rsplit(':', 1)
            
            if host.startswith('[') and host.endswith(']'): host = host[1:-1]
            if not remarks: remarks = f"{host}:{port_str}"
            
            return ShadowsocksConfig(host=host, port=int(port_str), remarks=remarks, method=method, password=password)
        except Exception as e:
            raise ParsingError(f"Could not parse Shadowsocks link: {uri[:60]}") from e
            
    @staticmethod
    def _parse_hysteria2(uri: str) -> Optional[Hysteria2Config]:
        try:
            uri = uri.replace("hy2://", "hysteria2://")
            parsed_url = urlparse(uri)
            if not parsed_url.hostname or not parsed_url.port:
                raise ParsingError("Missing hostname or port in Hysteria2 URI.")
            
            params = parse_qs(parsed_url.query)
            return Hysteria2Config(
                uuid=parsed_url.username or '',
                host=parsed_url.hostname,
                port=parsed_url.port,
                remarks=unquote(parsed_url.fragment) if parsed_url.fragment else f"{parsed_url.hostname}:{parsed_url.port}",
                sni=params.get('sni', [None])[0],
                insecure=int(params.get('insecure', [0])[0]),
                obfs=params.get('obfs', [None])[0],
                obfs_password=params.get('obfs-password', [None])[0],
            )
        except (ValueError, TypeError, AttributeError) as e:
            raise ParsingError(f"Could not parse Hysteria2 link correctly: {uri[:60]}") from e

    @staticmethod
    def _parse_tuic(uri: str) -> Optional[TuicConfig]:
        try:
            parsed_url = urlparse(uri)
            if not parsed_url.hostname or not parsed_url.port or not parsed_url.username or not parsed_url.password:
                raise ParsingError("Missing essential parts in TUIC URI.")
            
            params = parse_qs(parsed_url.query)
            return TuicConfig(
                uuid=parsed_url.username,
                password=parsed_url.password,
                host=parsed_url.hostname,
                port=parsed_url.port,
                remarks=unquote(parsed_url.fragment) if parsed_url.fragment else f"{parsed_url.hostname}:{parsed_url.port}",
                sni=params.get('sni', [None])[0],
                congestion_control=params.get('congestion_control', ['bbr'])[0],
                udp_relay_mode=params.get('udp_relay_mode', ['native'])[0],
                alpn=params.get('alpn', [None])[0],
            )
        except (ValueError, TypeError, AttributeError) as e:
            raise ParsingError(f"Could not parse TUIC link correctly: {uri[:60]}") from e

    @staticmethod
    def _parse_shadowsocksr(uri: str) -> Optional[ShadowsocksRConfig]:
        try:
            b64_data = uri[len("ssr://"):]
            decoded_data = b64_decode(b64_data)
            
            main_part, params_part = decoded_data.split('/?')
            params = parse_qs(params_part)

            parts = main_part.split(':')
            if len(parts) != 6: raise ParsingError("Invalid SSR main part.")

            host, port, protocol, method, obfs, password_b64 = parts
            password = b64_decode(password_b64)
            
            remarks_b64 = params.get('remarks', [b64_encode('N/A')])[0]
            remarks = b64_decode(remarks_b64)

            return ShadowsocksRConfig(
                host=host,
                port=int(port),
                uuid=password,
                remarks=remarks,
                protocol_param=params.get('protoparam', [None])[0],
                obfs_param=params.get('obfsparam', [None])[0],
                method=method,
                obfs=obfs,
                protocol='shadowsocksr'
            )
        except Exception as e:
            raise ParsingError(f"Could not parse SSR link: {uri[:60]}") from e

    @staticmethod
    def _parse_wireguard(uri: str) -> Optional[WireGuardConfig]:
        try:
            parsed_url = urlparse(uri)
            if not parsed_url.hostname or not parsed_url.port:
                raise ParsingError("Missing host or port in WireGuard/Warp URI.")
            
            params = parse_qs(parsed_url.query)
            
            private_key = None
            uuid_val = f"warp-{parsed_url.hostname}-{parsed_url.port}"
            
            if parsed_url.scheme == 'wg':
                if not parsed_url.username:
                    raise ParsingError("Missing private key in wg:// URI.")
                private_key = parsed_url.username
                uuid_val = private_key

            return WireGuardConfig(
                uuid=uuid_val,
                private_key=private_key,
                host=parsed_url.hostname,
                port=int(parsed_url.port),
                remarks=unquote(parsed_url.fragment) if parsed_url.fragment else f"{parsed_url.hostname}:{parsed_url.port}",
                public_key=params.get('public_key', [None])[0],
                preshared_key=params.get('preshared_key', [None])[0],
                ip=params.get('ip', [None])[0]
            )
        except Exception as e:
            raise ParsingError(f"Could not parse WireGuard/Warp link: {uri[:60]}") from e

class RawConfigCollector:
    PATTERNS = {
        "ss": r"(ss://[^\s<>#]+)", 
        "trojan": r"(trojan://[^\s<>#]+)", 
        "vmess": r"(vmess://[^\s<>#]+)", 
        "vless": r"(vless://(?:(?!=reality)[^\s<>#])+(?=[\s<>#]))", 
        "reality": r"(vless://[^\s<>#]+?security=reality[^\s<>#]*)",
        "hysteria2": r"((?:hy2|hysteria2)://[^\s<>#]+)",
        "tuic": r"(tuic://[^\s<>#]+)",
        "shadowsocksr": r"(ssr://[^\s<>#]+)",
        "wireguard": r"((?:wg|warp)://[^\s<>#]+)",
    }

    @classmethod
    def find_all(cls, text_content: str) -> Dict[str, List[str]]:
        all_matches = {}
        for name, pattern in cls.PATTERNS.items():
            full_pattern = r"(?<![\w-])" + pattern
            matches = re.findall(full_pattern, text_content, re.IGNORECASE)
            cleaned_matches = [re.sub(r"#[^#]*$", "", m) for m in matches if "…" not in m]
            if cleaned_matches:
                all_matches[name] = cleaned_matches
        return all_matches


class TelegramScraper:
    def __init__(self, channels: List[str], since_datetime: datetime):
        self.channels, self.since_datetime, self.iran_tz = channels, since_datetime, get_iran_timezone()
        self.configs_by_channel: Dict[str, List[str]] = {}
        self.successful_channels: List[Tuple[str, int]] = []
        self.failed_channels: List[str] = []

    async def scrape_all(self):
        with Progress(
            TextColumn("[bold blue]Scraping Telegram..."),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[green]{task.completed}/{task.total} Channels"),
            "•",
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("channels", total=len(self.channels))
            
            batch_size = 10
            channel_batches = [self.channels[i:i + batch_size] for i in range(0, len(self.channels), batch_size)]

            for i, batch in enumerate(channel_batches):
                tasks = [self._scrape_channel_with_retry(ch) for ch in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for j, channel_results in enumerate(results):
                    channel_name = batch[j]
                    if isinstance(channel_results, dict):
                        configs_found = sum(len(v) for v in channel_results.values())
                        if configs_found > 0:
                            self.successful_channels.append((channel_name, configs_found))
                            flat_configs = [config for sublist in channel_results.values() for config in sublist]
                            self.configs_by_channel[channel_name] = flat_configs
                    else:
                        self.failed_channels.append(channel_name)
                    
                    progress.update(task, advance=1)

                if i < len(channel_batches) - 1:
                    await asyncio.sleep(random.uniform(5, 10))

        await self._write_scrape_report()

    async def _write_scrape_report(self):
        now = datetime.now(get_iran_timezone())
        report_str = f"--- Telegram Scrape Report ---\n"
        report_str += f"Timestamp: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        report_str += f"Total Channels: {len(self.channels)}\n"
        report_str += f"Successful Scrapes: {len(self.successful_channels)}\n"
        report_str += f"Failed Scrapes: {len(self.failed_channels)}\n\n"

        report_str += "--- Channels with Found Configs ---\n"
        for channel, count in sorted(self.successful_channels, key=lambda item: item[1], reverse=True):
            report_str += f"{channel}: {count} configs\n"

        report_str += "\n--- Failed Channels ---\n"
        for channel in sorted(self.failed_channels):
            report_str += f"{channel}\n"

        try:
            async with aiofiles.open(CONFIG.TELEGRAM_REPORT_FILE, "w", encoding='utf-8') as f:
                await f.write(report_str)
        except IOError:
            pass

    async def _scrape_channel_with_retry(self, channel: str, max_retries: int = 2) -> Optional[Dict[str, List[str]]]:
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(random.uniform(1.5, 3.0))
                url = CONFIG.TELEGRAM_BASE_URL.format(channel)

                status, html = await AsyncHttpClient.get(url)
                if status == 200 and html:
                    soup = BeautifulSoup(html, "html.parser")
                    messages = soup.find_all("div", class_="tgme_widget_message", limit=CONFIG.TELEGRAM_MESSAGE_LIMIT)
                    if not messages: return {}

                    channel_configs: Dict[str, List[str]] = {key: [] for key in RawConfigCollector.PATTERNS.keys()}
                    configs_count_in_channel = 0
                    
                    for msg in messages:
                        if configs_count_in_channel >= CONFIG.MAX_CONFIGS_PER_CHANNEL:
                            break

                        time_tag = msg.find("time", class_="time")
                        if time_tag and 'datetime' in time_tag.attrs:
                            try:
                                message_dt = datetime.fromisoformat(time_tag['datetime']).astimezone(self.iran_tz)
                                if CONFIG.TELEGRAM_IGNORE_LAST_UPDATE or message_dt > self.since_datetime:
                                    text_div = msg.find("div", class_="tgme_widget_message_text")
                                    if text_div:
                                        found_configs = RawConfigCollector.find_all(text_div.get_text('\n', strip=True))
                                        for config_type, configs in found_configs.items():
                                            remaining_slots = CONFIG.MAX_CONFIGS_PER_CHANNEL - configs_count_in_channel
                                            if remaining_slots <= 0: break
                                            
                                            configs_to_add = configs[:remaining_slots]
                                            channel_configs[config_type].extend(configs_to_add)
                                            configs_count_in_channel += len(configs_to_add)
                                        
                                        if configs_count_in_channel >= CONFIG.MAX_CONFIGS_PER_CHANNEL:
                                            break
                            except (ValueError, TypeError): continue
                    return channel_configs
            except (NetworkError, Exception):
                pass
            if attempt < max_retries - 1:
                await asyncio.sleep((attempt + 1) * 5)
        return None

class SubscriptionFetcher:
    def __init__(self, sub_links: List[str]):
        self.sub_links = sub_links
        self.total_configs_by_type: Dict[str, List[str]] = {key: [] for key in RawConfigCollector.PATTERNS.keys()}

    async def fetch_all(self):
        with Progress(
            TextColumn("[bold blue]Fetching Subscriptions..."),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[green]{task.completed}/{task.total} Links"),
            console=console
        ) as progress:
            tasks = [self._fetch_and_decode(link) for link in self.sub_links]
            for f in progress.track(asyncio.as_completed(tasks), total=len(self.sub_links), description="links"):
                content = await f
                if isinstance(content, str):
                    found_configs = RawConfigCollector.find_all(content)
                    for config_type, configs in found_configs.items():
                        self.total_configs_by_type[config_type].extend(configs)

    async def _fetch_and_decode(self, link: str) -> str:
        try:
            _, content = await AsyncHttpClient.get(link)
            try:
                return base64.b64decode(content + '==').decode('utf-8')
            except Exception:
                return content
        except Exception:
            return ""

class FileManager:
    def __init__(self, config: AppConfig):
        self.config = config
        self._setup_directories()

    def _setup_directories(self):
        CONFIG.DATA_DIR.mkdir(exist_ok=True)
        CONFIG.OUTPUT_DIR.mkdir(exist_ok=True)
        for path in self.config.DIRS.values():
            path.mkdir(parents=True, exist_ok=True)

    async def read_json_file(self, file_path: Path) -> List[Any]:
        if not file_path.exists(): return []
        try:
            async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
                return json.loads(await f.read())
        except Exception: return []

    async def write_configs_to_file(self, file_path: Path, configs: List[BaseConfig], base64_encode: bool = True):
        if not configs: return
        final_list = self._add_signatures(configs) if CONFIG.ADD_SIGNATURES else [c.to_uri() for c in configs]
        content = "\n".join(final_list)
        if base64_encode: content = base64.b64encode(content.encode('utf-8')).decode('utf-8')
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(file_path, 'w', encoding='utf-8') as f: await f.write(content)
        except IOError: pass

    def _add_signatures(self, configs: List[BaseConfig]) -> List[str]:
        uris = [c.to_uri() for c in configs]
        now = datetime.now(get_iran_timezone())
        update_str = f"🕒 LAST UPDATE: {now.strftime('%Y/%m/%d | %H:%M')}"

        final_list = uris[:]
        final_list.insert(0, self._create_title_config(update_str, 1080))
        final_list.insert(1, self._create_title_config(CONFIG.ADV_SIGNATURE, 2080))
        final_list.insert(2, self._create_title_config(CONFIG.CUSTOM_SIGNATURE, 4080))
        final_list.append(self._create_title_config(CONFIG.DNT_SIGNATURE, 8080))
        final_list.append(self._create_title_config(CONFIG.DEV_SIGNATURE, 8081))
        return final_list

    def _create_title_config(self, title: str, port: int) -> str:
        return f"trojan://{generate_random_uuid_string()}@127.0.0.1:{port}?security=tls&type=tcp#{unquote(title)}"

class Geolocation:
    _country_reader: Optional[geoip2.database.Reader] = None
    _asn_reader: Optional[geoip2.database.Reader] = None
    _ip_cache: Dict[str, Optional[str]] = {}

    @classmethod
    def initialize(cls):
        if CONFIG.GEOIP_DB_FILE.exists():
            try:
                cls._country_reader = geoip2.database.Reader(str(CONFIG.GEOIP_DB_FILE))
            except Exception: 
                cls._country_reader = None
        
        if CONFIG.GEOIP_ASN_DB_FILE.exists():
            try:
                cls._asn_reader = geoip2.database.Reader(str(CONFIG.GEOIP_ASN_DB_FILE))
            except Exception:
                cls._asn_reader = None

    @classmethod
    async def get_ip(cls, hostname: str) -> Optional[str]:
        if hostname in cls._ip_cache: return cls._ip_cache[hostname]
        if is_ip_address(hostname):
            cls._ip_cache[hostname] = hostname
            return hostname
        try:
            loop = asyncio.get_running_loop()
            addr_info = await loop.getaddrinfo(hostname, None, family=socket.AF_INET)
            ip = addr_info[0][4][0]
            cls._ip_cache[hostname] = ip
            return ip
        except Exception:
            cls._ip_cache[hostname] = None
            return None

    @classmethod
    def get_country_from_ip(cls, ip: str) -> str:
        if cls._country_reader is None or ip is None: return "XX"
        try:
            response = cls._country_reader.country(ip)
            return response.country.iso_code or "XX"
        except (geoip2.errors.AddressNotFoundError, Exception):
            return "XX"
    
    @classmethod
    def get_asn_from_ip(cls, ip: str) -> Optional[str]:
        if cls._asn_reader is None or ip is None: return None
        try:
            response = cls._asn_reader.asn(ip)
            return response.autonomous_system_organization
        except (geoip2.errors.AddressNotFoundError, Exception):
            return None

    @classmethod
    def close(cls):
        if cls._country_reader: cls._country_reader.close()
        if cls._asn_reader: cls._asn_reader.close()

class ConfigProcessor:
    def __init__(self, raw_configs_by_type: Dict[str, List[str]], seen_configs: Dict[str, str]):
        self.raw_configs_by_type = raw_configs_by_type
        self.parsed_configs: Dict[str, BaseConfig] = {}
        self.total_raw_count = sum(len(v) for v in raw_configs_by_type.values())
        self.seen_configs = seen_configs
        self.tested_configs_count = 0
        self.active_configs_count = 0

    async def process(self):
        console.log(f"Processing {self.total_raw_count} raw config strings...")

        all_parsed_configs: List[BaseConfig] = []
        for config_type, configs in self.raw_configs_by_type.items():
            for uri in configs:
                parsed = V2RayParser.parse(uri, source_type=config_type)
                if parsed:
                    all_parsed_configs.append(parsed)
        console.log(f"Successfully parsed {len(all_parsed_configs)} configs.")

        for config in all_parsed_configs:
            key = config.get_deduplication_key()
            if key not in self.parsed_configs:
                self.parsed_configs[key] = config
        console.log(f"Deduplication by URI resulted in {len(self.parsed_configs)} unique configs.")

        if CONFIG.ENABLE_SEEN_CONFIG_FILTER:
            self._filter_by_seen_cache()
        
        await self._resolve_geo_info()
        if CONFIG.ENABLE_IP_DEDUPLICATION:
            self._deduplicate_by_ip()

        if CONFIG.ENABLE_CONNECTIVITY_TEST:
            await self._test_connectivity()
            
        self._format_config_remarks()
        
        temp_list = list(self.parsed_configs.values())
        random.shuffle(temp_list)
        
        if CONFIG.ENABLE_CONNECTIVITY_TEST:
            temp_list.sort(key=lambda item: item.ping if item.ping is not None else 9999)
        
        self.parsed_configs = {cfg.get_deduplication_key(): cfg for cfg in temp_list}

    def _filter_by_seen_cache(self):
        now_utc = datetime.now(timezone.utc)
        timeout = timedelta(hours=CONFIG.SEEN_CONFIG_TIMEOUT_HOURS)
        
        configs_to_keep = {}
        removed_count = 0
        
        for key, config in self.parsed_configs.items():
            if key in self.seen_configs:
                try:
                    seen_time = datetime.fromisoformat(self.seen_configs[key])
                    if now_utc - seen_time < timeout:
                        removed_count += 1
                        continue 
                except (ValueError, TypeError):
                    pass
            
            configs_to_keep[key] = config
            self.seen_configs[key] = now_utc.isoformat()
            
        self.parsed_configs = configs_to_keep
        if removed_count > 0:
            console.log(f"Filtered out {removed_count} recently seen configs. {len(self.parsed_configs)} remaining.")

    async def _resolve_geo_info(self):
        unique_hosts = list({c.host for c in self.parsed_configs.values()})
        console.log(f"Resolving geo-information for {len(unique_hosts)} unique hosts...")
        await asyncio.gather(*[Geolocation.get_ip(host) for host in unique_hosts])
        
        for config in self.parsed_configs.values():
            ip_address = Geolocation._ip_cache.get(config.host)
            if ip_address:
                config.country = Geolocation.get_country_from_ip(ip_address)
                config.asn_org = Geolocation.get_asn_from_ip(ip_address)

    def _deduplicate_by_ip(self):
        unique_ips: Dict[str, BaseConfig] = {}
        kept_configs: Dict[str, BaseConfig] = {}
        
        for key, config in self.parsed_configs.items():
            ip = Geolocation._ip_cache.get(config.host)
            if ip and ip not in unique_ips:
                unique_ips[ip] = config
                kept_configs[key] = config
            elif not ip:
                kept_configs[key] = config

        removed_count = len(self.parsed_configs) - len(kept_configs)
        self.parsed_configs = kept_configs
        console.log(f"IP-based deduplication removed {removed_count} configs. {len(self.parsed_configs)} remaining.")

    async def _test_tcp_connection(self, config: BaseConfig) -> Optional[int]:
        ip = Geolocation._ip_cache.get(config.host)
        if not ip: return None
        
        try:
            start_time = asyncio.get_event_loop().time()
            fut = asyncio.open_connection(ip, config.port)
            reader, writer = await asyncio.wait_for(fut, timeout=CONFIG.CONNECTIVITY_TEST_TIMEOUT)
            
            writer.write(b"\x01") 
            await writer.drain()
            await reader.read(1)

            end_time = asyncio.get_event_loop().time()
            writer.close()
            await writer.wait_closed()
            return int((end_time - start_time) * 1000)
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError, Exception):
            return None

    async def _test_connectivity(self):
        configs_to_test = list(self.parsed_configs.values())
        if len(configs_to_test) > CONFIG.MAX_CONNECTIVITY_TESTS:
            configs_to_test = random.sample(configs_to_test, CONFIG.MAX_CONNECTIVITY_TESTS)
        
        self.tested_configs_count = len(configs_to_test)

        with Progress(
            TextColumn("[bold blue]Testing Connectivity..."),
            BarColumn(bar_width=None),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "•",
            TextColumn("[green]{task.completed}/{task.total} Tested"),
            console=console
        ) as progress:
            ping_task = progress.add_task("pinging", total=len(configs_to_test))
            
            tasks = [self._test_tcp_connection(config) for config in configs_to_test]
            results = await asyncio.gather(*tasks)

            for config, ping_result in zip(configs_to_test, results):
                if ping_result is not None:
                    config.ping = ping_result
                progress.update(ping_task, advance=1)
        
        self.active_configs_count = sum(1 for c in configs_to_test if c.ping is not None)
        console.log(f"Connectivity test complete. {self.active_configs_count}/{self.tested_configs_count} configs responded.")

    def _format_config_remarks(self):
        for config in self.parsed_configs.values():
            proto_full_map = {
                'vmess': 'VMESS', 'vless': 'VLESS', 'trojan': 'TROJAN', 
                'shadowsocks': 'SHADOWSOCKS', 'hysteria2': 'HYSTERIA2', 'tuic': 'TUIC',
                'shadowsocksr': 'SSR', 'wireguard': 'WIREGUARD'
            }
            proto_full = proto_full_map.get(config.protocol, 'CFG')

            sec = 'RLT' if config.source_type == 'reality' else (config.security.upper() if config.security != 'none' else 'NTLS')
            net = config.network.upper() if config.network else 'N/A'
            flag = COUNTRY_CODE_TO_FLAG.get(config.country, "🏳️")
            ip_address = Geolocation._ip_cache.get(config.host, "N/A")
            
            asn_str = f" - {config.asn_org}" if config.asn_org else ""
            new_remark = f"{config.country} {flag} ┇ {proto_full}-{net}-{sec}{asn_str} ┇ {ip_address}"
            config.remarks = new_remark.strip()

    def get_all_unique_configs(self) -> List[BaseConfig]:
        return list(self.parsed_configs.values())

    def categorize(self) -> Dict[str, Dict[str, List[BaseConfig]]]:
        configs = self.get_all_unique_configs()
        categories: Dict[str, Dict[str, List[BaseConfig]]] = { 
            "protocols": {}, "networks": {}, "security": {}, "countries": {}, "datacenters": {} 
        }
        
        for config in configs:
            categories["protocols"].setdefault(config.protocol, []).append(config)
            
            if config.network:
                categories["networks"].setdefault(config.network, []).append(config)
            if config.source_type == 'reality':
                categories["networks"].setdefault('reality', []).append(config)

            if config.security == 'tls':
                categories["security"].setdefault('tls', []).append(config)
            elif config.security == 'xtls':
                categories["security"].setdefault('xtls', []).append(config)
            elif config.security == 'none':
                categories["security"].setdefault('nontls', []).append(config)

            if config.country and config.country != "XX":
                categories["countries"].setdefault(config.country, []).append(config)
            
            if config.asn_org:
                sanitized_asn = re.sub(r'[\\/*?:"<>|,]', "", config.asn_org).replace(" ", "_")
                categories["datacenters"].setdefault(sanitized_asn, []).append(config)
                
        return categories

class V2RayCollectorApp:
    def __init__(self):
        self.config = CONFIG
        self.file_manager = FileManager(self.config)
        self.last_update_time = datetime.now(timezone.utc) - timedelta(days=1)
        self.seen_configs = {}
        self.start_time = datetime.now()

    async def run(self):
        console.rule("[bold green]V2Ray Config Collector - v7.0.4[/bold green]")
        await self._load_state()

        tg_channels = await self.file_manager.read_json_file(self.config.TELEGRAM_CHANNELS_FILE)
        sub_links = await self.file_manager.read_json_file(self.config.SUBSCRIPTION_LINKS_FILE)

        tg_scraper = TelegramScraper(tg_channels, self.last_update_time)
        sub_fetcher = SubscriptionFetcher(sub_links)

        if tg_channels: await tg_scraper.scrape_all()
        if sub_links and CONFIG.ENABLE_SUBSCRIPTION_FETCHING: await sub_fetcher.fetch_all()

        combined_raw_configs: Dict[str, List[str]] = {key: [] for key in RawConfigCollector.PATTERNS.keys()}
        for channel, configs in tg_scraper.configs_by_channel.items():
            for config in configs:
                for proto, pattern in RawConfigCollector.PATTERNS.items():
                    if re.match(r"(?<![\w-])" + pattern, config):
                        combined_raw_configs[proto].append(config)
                        break
        
        for config_type, configs in sub_fetcher.total_configs_by_type.items():
            combined_raw_configs[config_type].extend(configs)

        if not any(combined_raw_configs.values()):
            console.log("[yellow]No new configurations found. Exiting.[/yellow]")
            return

        processor = ConfigProcessor(combined_raw_configs, self.seen_configs)
        await processor.process()
        self.seen_configs = processor.seen_configs

        all_unique_configs = processor.get_all_unique_configs()
        if not all_unique_configs:
            console.log("[yellow]No valid unique configurations to save. Exiting.[/yellow]")
            return
            
        categories = processor.categorize()
        await self._save_results(all_unique_configs, categories, tg_scraper.configs_by_channel)
        await self._save_state()
        self._print_summary_report(processor, tg_scraper, sub_fetcher, self.start_time)
        console.log("[bold green]Collection and processing complete.[/bold green]")

    async def _load_state(self):
        if self.config.SEEN_CONFIGS_FILE.exists():
            try:
                async with aiofiles.open(self.config.SEEN_CONFIGS_FILE, 'r') as f:
                    self.seen_configs = json.loads(await f.read())
            except Exception:
                self.seen_configs = {}
        
        if self.config.LAST_UPDATE_FILE.exists():
            try:
                async with aiofiles.open(self.config.LAST_UPDATE_FILE, 'r') as f:
                    self.last_update_time = datetime.fromisoformat(await f.read())
            except Exception: pass

    async def _save_state(self):
        try:
            async with aiofiles.open(self.config.SEEN_CONFIGS_FILE, 'w') as f:
                await f.write(json.dumps(self.seen_configs, indent=4))
        except IOError: pass
        
        try:
            async with aiofiles.open(self.config.LAST_UPDATE_FILE, 'w') as f:
                await f.write(datetime.now(timezone.utc).isoformat())
        except IOError: pass

    def _sanitize_filename(self, name: str) -> str:
        name = re.sub(r'[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002702-\U000027B0\U00002620-\U0000262F\U00002300-\U000023FF\U00002B50]', '', name)
        return re.sub(r'[\\/*?:"<>|,@=]', "", name).replace(" ", "_")

    async def _save_results(self, all_configs: List[BaseConfig], categories: Dict[str, Any], configs_by_channel: Dict[str, List[str]]):
        console.log("Saving categorized configurations...")
        
        save_tasks: List[Coroutine] = []
        save_tasks.append(self.file_manager.write_configs_to_file(self.config.DIRS["subscribe"] / "base64.txt", all_configs))
        save_tasks.append(self.file_manager.write_configs_to_file(self.config.OUTPUT_DIR / "all_configs.txt", all_configs, base64_encode=False))
        
        for cat_name, cat_items in categories.items():
            for item_name, configs in cat_items.items():
                if configs:
                    sanitized_name = self._sanitize_filename(str(item_name))
                    if not sanitized_name: continue
                    
                    if item_name == 'wireguard':
                        path = self.config.DIRS[cat_name] / "warp.txt"
                    else:
                        path = self.config.DIRS[cat_name] / f"{sanitized_name}.txt"

                    save_tasks.append(self.file_manager.write_configs_to_file(path, configs, base64_encode=False))
            
        chunk_size = math.ceil(len(all_configs) / 20) if all_configs else 0
        if chunk_size > 0:
            for i, chunk in enumerate([all_configs[i:i + chunk_size] for i in range(0, len(all_configs), chunk_size)]):
                path = self.config.DIRS["splitted"] / f"mixed_{i+1}.txt"
                save_tasks.append(self.file_manager.write_configs_to_file(path, chunk, base64_encode=False))
        
        allowed_protocols_for_mix = ['vmess', 'vless', 'trojan', 'shadowsocks']
        for protocol, configs in categories["protocols"].items():
            if protocol not in allowed_protocols_for_mix:
                continue

            if not configs: continue
            random.shuffle(configs)
            chunk_size_proto = math.ceil(len(configs) / 5)
            if chunk_size_proto > 0:
                for i, chunk in enumerate([configs[i:i + chunk_size_proto] for i in range(0, len(configs), chunk_size_proto)][:5]):
                    path = self.config.DIRS["mix_protocol"] / f"mix_{protocol}_{i+1}.txt"
                    save_tasks.append(self.file_manager.write_configs_to_file(path, chunk, base64_encode=False))

        for channel_name, raw_configs in configs_by_channel.items():
            if raw_configs:
                parsed_channel_configs = [V2RayParser.parse(uri) for uri in raw_configs]
                parsed_channel_configs = [c for c in parsed_channel_configs if c is not None]
                if parsed_channel_configs:
                    sanitized_name = self._sanitize_filename(channel_name)
                    path = self.config.DIRS["channel_subs"] / f"{sanitized_name}.txt"
                    save_tasks.append(self.file_manager.write_configs_to_file(path, parsed_channel_configs, base64_encode=False))

        await asyncio.gather(*save_tasks)

    def _print_summary_report(self, processor: ConfigProcessor, tg_scraper: TelegramScraper, sub_fetcher: SubscriptionFetcher, start_time: datetime):
        all_configs = processor.get_all_unique_configs()
        protocol_counts = Counter(c.protocol for c in all_configs)
        country_counts = Counter(c.country for c in all_configs if c.country and c.country != 'XX')
        asn_counts = Counter(c.asn_org for c in all_configs if c.asn_org)
        
        duration = datetime.now() - start_time
        duration_str = str(duration).split('.')[0]

        run_details_table = Table(title="⚙️ Run Details ⚙️", title_style="bold yellow")
        run_details_table.add_column("Item", style="cyan", justify="right")
        run_details_table.add_column("Value", style="bold green", justify="left")
        run_details_table.add_row("Script Version", CONFIG.DEV_SIGNATURE.split(' ')[-1])
        run_details_table.add_row("Start Time", start_time.strftime('%Y-%m-%d %H:%M:%S'))
        run_details_table.add_row("Duration", duration_str)
        console.print(run_details_table)

        source_table = Table(title="📊 Source Summary 📊", title_style="bold magenta")
        source_table.add_column("Source", style="cyan", justify="right")
        source_table.add_column("Raw Configs Found", style="bold green", justify="left")
        tg_raw_count = sum(len(v) for v in tg_scraper.configs_by_channel.values())
        sub_raw_count = sum(len(v) for v in sub_fetcher.total_configs_by_type.values())
        source_table.add_row("Telegram Channels", str(tg_raw_count))
        source_table.add_row("Subscription Links", str(sub_raw_count))
        source_table.add_row("[b]Total Raw[/b]", f"[b]{processor.total_raw_count}[/b]")
        console.print(source_table)

        now_str = datetime.now(get_iran_timezone()).strftime('%Y-%m-%d %H:%M')
        summary_table = Table(title=f"📈 Final Collection Report ({now_str}) 📈", title_style="bold magenta", show_header=False)
        summary_table.add_column("Key", style="cyan")
        summary_table.add_column("Value", style="bold green")
        summary_table.add_row("Unique & Valid Configs", str(len(all_configs)))
        console.print(summary_table)

        proto_table = Table(title="📈 Configs by Protocol", title_style="bold blue")
        proto_table.add_column("Protocol", style="cyan")
        proto_table.add_column("Count", style="bold green")
        for protocol, count in protocol_counts.most_common():
            proto_table.add_row(protocol.upper(), str(count))
            
        country_table = Table(title="🌍 Top 5 Countries", title_style="bold blue")
        country_table.add_column("Flag")
        country_table.add_column("Country", style="cyan")
        country_table.add_column("Count", style="bold green")
        for country_code, count in country_counts.most_common(5):
            flag = COUNTRY_CODE_TO_FLAG.get(country_code, '🏳️')
            country_table.add_row(flag, country_code, str(count))

        asn_table = Table(title="🏢 Top 5 Datacenters", title_style="bold blue")
        asn_table.add_column("Datacenter", style="cyan")
        asn_table.add_column("Count", style="bold green")
        for asn, count in asn_counts.most_common(5):
            asn_table.add_row(asn, str(count))

        console.print(proto_table)
        console.print(country_table)
        console.print(asn_table)
        
        commit_message = f"feat: Update configs - {len(all_configs)} total"
        console.print(Panel(f"[bold cyan]{commit_message}[/bold cyan]", title="💡 Suggested Commit Message", border_style="yellow"))

async def _download_db_if_needed(url: str, file_path: Path):
    if not file_path.exists():
        console.log(f"[yellow]{file_path.name} not found, downloading...[/yellow]")
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, follow_redirects=True, timeout=120.0)
                response.raise_for_status()
                async with aiofiles.open(file_path, "wb") as f:
                    await f.write(response.content)
                console.log(f"[green]{file_path.name} downloaded successfully.[/green]")
        except Exception as e:
            console.log(f"[bold red]Failed to download {file_path.name}: {e}.[/bold red]")

async def _setup_data_file(remote_url: str, local_path: Path):
    if not local_path.exists():
        console.log(f"[yellow]{local_path.name} not found, fetching from remote...[/yellow]")
        try:
            status, content = await AsyncHttpClient.get(remote_url)
            if status == 200 and content:
                # The content is expected to be JSON. Let's parse and re-dump to ensure format.
                data = json.loads(content)
                async with aiofiles.open(local_path, "w", encoding='utf-8') as f:
                    await f.write(json.dumps(data, indent=4))
                console.log(f"[green]Successfully created {local_path.name} from remote source.[/green]")
        except Exception as e:
            console.log(f"[bold red]Failed to create {local_path.name} from {remote_url}: {e}[/bold red]")

async def main():
    CONFIG.DATA_DIR.mkdir(exist_ok=True)

    await _download_db_if_needed(CONFIG.GEOIP_DB_URL, CONFIG.GEOIP_DB_FILE)
    await _download_db_if_needed(CONFIG.GEOIP_ASN_DB_URL, CONFIG.GEOIP_ASN_DB_FILE)

    await _setup_data_file(CONFIG.REMOTE_CHANNELS_URL, CONFIG.TELEGRAM_CHANNELS_FILE)
    await _setup_data_file(CONFIG.REMOTE_SUBS_URL, CONFIG.SUBSCRIPTION_LINKS_FILE)

    Geolocation.initialize()

    app = V2RayCollectorApp()
    try:
        await app.run()
    except KeyboardInterrupt:
        console.log("\n[yellow]Application interrupted by user.[/yellow]")
    except Exception as e:
        console.log(f"\n[bold red]An unhandled exception occurred: {e}[/bold red]")
        console.print_exception()
    finally:
        await AsyncHttpClient.close()
        Geolocation.close()
        console.rule("[bold green]Shutdown complete.[/bold green]")

if __name__ == "__main__":
    asyncio.run(main())
