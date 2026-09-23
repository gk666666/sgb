import os
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class WatermarkStore:
    def __init__(
        self,
        *,
        account_url_setting: str | tuple[str, ...] = "STATE_STORAGE_ACCOUNT_URL",
        container_name_setting: str | tuple[str, ...] = "WATERMARK_CONTAINER_NAME",
        blob_name_setting: str | tuple[str, ...] = (),
        default_container_name: str = "adx-watermark",
        default_blob_name: str = "defect-global.json",
    ):
        account_url = _normalize_storage_account_url(_get_required_setting(account_url_setting))
        self.container_name = _get_setting(container_name_setting, default_container_name)
        self.blob_name = _get_setting(blob_name_setting, default_blob_name)
        credential = DefaultAzureCredential()
        service = BlobServiceClient(account_url=account_url, credential=credential)
        self.container_client = service.get_container_client(self.container_name)
        try:
            self.container_client.create_container()
        except Exception:
            pass

    def load(self, initial_watermark: str) -> dict:
        try:
            payload = self.container_client.download_blob(self.blob_name).readall()
            entity = json.loads(payload)
            last_watermark = _normalize_utc_string(entity.get("last_watermark"), fallback=initial_watermark)
            return {
                "last_watermark": last_watermark,
                "updated_at": _normalize_utc_string(entity.get("updated_at")),
            }
        except Exception:
            return {"last_watermark": _normalize_utc_string(initial_watermark, fallback=initial_watermark), "updated_at": None}

    def save(self, watermark: str):
        entity = {
            "last_watermark": _normalize_utc_string(watermark, fallback=watermark),
            "updated_at": _normalize_utc_string(datetime.now(timezone.utc).isoformat()),
        }
        self.container_client.upload_blob(
            name=self.blob_name,
            data=json.dumps(entity, ensure_ascii=False, indent=2),
            overwrite=True,
        )


def _as_setting_names(value) -> tuple[str, ...]:
    if isinstance(value, (tuple, list)):
        return tuple(str(item) for item in value if item)
    return (str(value),)


def _get_setting(name_or_names, default: str | None = None) -> str | None:
    for name in _as_setting_names(name_or_names):
        value = os.getenv(name)
        if value:
            return value
    return default


def _get_required_setting(name_or_names) -> str:
    value = _get_setting(name_or_names)
    if not value:
        names = " or ".join(_as_setting_names(name_or_names))
        raise ValueError(f"Missing required setting: {names}")
    return value


def _normalize_storage_account_url(value: str) -> str:
    raw = value.strip().rstrip("/")
    if not raw:
        raise ValueError("STATE_STORAGE_ACCOUNT_URL is empty")

    if "://" not in raw:
        return f"https://{raw}.blob.core.chinacloudapi.cn"

    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Invalid STATE_STORAGE_ACCOUNT_URL: {value}")

    netloc = parsed.netloc.replace(".dfs.core.chinacloudapi.cn", ".blob.core.chinacloudapi.cn")
    return f"{parsed.scheme}://{netloc}"


def _normalize_utc_string(value, fallback: str | None = None) -> str | None:
    if value is None:
        return fallback
    text = str(value).strip()
    if not text:
        return fallback
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc)
        else:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return text if fallback is None else fallback
