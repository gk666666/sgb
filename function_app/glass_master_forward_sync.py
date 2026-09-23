import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable

import requests
from runtime_settings import (
    build_url,
    get_first_setting,
    get_integration_base_url,
    get_integration_env_setting,
    get_int_setting,
)


logger = logging.getLogger(__name__)

CN_TZ = timezone(timedelta(hours=8))


def _truncate_text(value: str, limit: int = 500) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


def _present(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        text = value.strip()
        return text != "" and text.lower() not in {"none", "nan", "null"}
    return True


def _null_if_blank(value):
    if not _present(value):
        return None
    if isinstance(value, str):
        text = value.strip()
        return text if text else None
    return value


def _parse_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=timezone.utc)


def _to_cn_datetime(value) -> datetime | None:
    dt = _parse_datetime(value)
    if dt is None:
        return None
    return dt.astimezone(CN_TZ)


def _format_cn_datetime(value) -> str | None:
    dt = _to_cn_datetime(value)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _resolve_position(row: dict) -> str:
    defect_report_post = str(row.get("defect_report_post") or "").strip()
    if defect_report_post.startswith("5FN6-") and len(defect_report_post) == len("5FN6-X"):
        return f"5FN6-竖检{defect_report_post[-1]}"
    if defect_report_post:
        return defect_report_post
    for key in ("factory", "source_topic", "payload_family"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _resolve_business_time(row: dict) -> str | None:
    for key in ("defect_report_time_local", "defect_report_time", "effective_source_time", "source_time", "watermark_time_local", "watermark_time_utc"):
        text = _format_cn_datetime(row.get(key))
        if text:
            return text
    return None


def _compact_dict(value: dict) -> dict:
    return {key: item for key, item in value.items() if item is not None}


class GlassMasterSyncPartialFailure(RuntimeError):
    def __init__(self, message: str, sent_rows: list[dict], failed_rows: list[dict], http_status: str = ""):
        super().__init__(message)
        self.sent_rows = sent_rows
        self.failed_rows = failed_rows
        self.http_status = http_status


class GlassMasterSync:
    def __init__(self):
        self.base_url = get_first_setting(
            "GLASS_MASTER_API_BASE_URL",
            "INTEGRATION_BASE_URL",
        ) or get_integration_base_url()
        self.sync_data_url = get_first_setting(
            "GLASS_MASTER_API_SYNC_DATA_URL",
        )
        if not self.sync_data_url and self.base_url:
            self.sync_data_url = build_url(self.base_url, "/sgb-fm-extend/api/pub/sync/plc/glass/defect/sync")
        self.app_id = get_first_setting(
            "GLASS_MASTER_API_APP_ID",
            default=get_integration_env_setting("APP_ID"),
        )
        if not self.app_id:
            raise ValueError("Missing required setting: GLASS_MASTER_API_APP_ID / INTEGRATION_*_APP_ID")
        self.app_secret = get_first_setting(
            "GLASS_MASTER_API_APP_SECRET",
            default=get_integration_env_setting("APP_SECRET"),
        )
        if not self.app_secret:
            raise ValueError("Missing required setting: GLASS_MASTER_API_APP_SECRET / INTEGRATION_*_APP_SECRET")
        self.tenant_id = get_first_setting(
            "GLASS_MASTER_API_TENANT_ID",
            default=get_integration_env_setting("TENANT_ID", default="sekurit"),
        )
        self.timeout = get_int_setting(
            "GLASS_MASTER_API_TIMEOUT",
            "INTEGRATION_API_TIMEOUT",
            default=60,
            minimum=1,
        )
        self.batch_size = get_int_setting(
            "GLASS_MASTER_API_BATCH_SIZE",
            "INTEGRATION_API_BATCH_SIZE",
            default=200,
            minimum=1,
        )
        if self.batch_size <= 0:
            self.batch_size = 200
        if not self.sync_data_url:
            raise ValueError(
                "Missing required setting: GLASS_MASTER_API_SYNC_DATA_URL. "
                "或配置 GLASS_MASTER_API_BASE_URL / INTEGRATION_*_BASE_URL 以自动拼接玻璃维度接口地址。"
            )

    def _get_timestamp(self) -> str:
        return datetime.now(CN_TZ).strftime("%Y%m%d%H%M%S")

    def _sign(self, timestamp: str, biz_params: str) -> str:
        raw = f"{self.app_secret}{timestamp}{biz_params}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _validate_sync_response(self, response: requests.Response) -> dict | None:
        if not response.text.strip():
            logger.warning("Glass master sync returned empty body with HTTP %s", response.status_code)
            return None

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Glass master sync returned non-JSON body: {_truncate_text(response.text)}"
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"Glass master sync returned unexpected payload type: {type(payload).__name__}")

        code = payload.get("code")
        state = payload.get("state")
        success = payload.get("success")
        if code in (0, "0", "000000") or state == "SUCCESS" or success is True:
            return payload

        raise RuntimeError(f"Glass master sync business failure: {_truncate_text(json.dumps(payload, ensure_ascii=False))}")

    def _build_row(self, row: dict, create_time: datetime) -> dict:
        _ = create_time
        return _compact_dict(
            {
                "dmc_code": _null_if_blank(row.get("dmc_code")),
                "rfid": _null_if_blank(row.get("rfid")),
                "aura_code": _null_if_blank(row.get("aura_code")),
                "pdlc_code": _null_if_blank(row.get("pdlc_code")),
                "mest_result": _null_if_blank(row.get("mest_result")),
                "data_time": _resolve_business_time(row),
                "report_post": _null_if_blank(_resolve_position(row)),
                "defects": [],
            }
        )

    def sync_rows(self, rows: list[dict], on_batch_success: Callable[[list[dict]], None] | None = None) -> list[dict]:
        if not rows:
            return []

        create_time = datetime.now(timezone.utc)
        total_batches = (len(rows) + self.batch_size - 1) // self.batch_size
        sent_rows: list[dict] = []

        for i in range(0, len(rows), self.batch_size):
            batch_rows = rows[i : i + self.batch_size]
            batch_payload = [self._build_row(row, create_time) for row in batch_rows]
            batch_idx = (i // self.batch_size) + 1
            biz_params = json.dumps(batch_payload, ensure_ascii=False, separators=(",", ":"))

            try:
                timestamp = self._get_timestamp()
                request_json = {
                    "app_id": self.app_id,
                    "biz_params": biz_params,
                    "sign": self._sign(timestamp, biz_params),
                    "tenant_id": self.tenant_id,
                    "timestamp": timestamp,
                }
                response = requests.post(self.sync_data_url, json=request_json, timeout=(10, self.timeout))
                response.raise_for_status()
                response_payload = self._validate_sync_response(response)
                if on_batch_success is not None:
                    on_batch_success(batch_rows)
                sent_rows.extend(batch_rows)
                logger.info(
                    "Glass master sync batch %s/%s succeeded, rows=%s, status=%s, response=%s",
                    batch_idx,
                    total_batches,
                    len(batch_rows),
                    response.status_code,
                    _truncate_text(json.dumps(response_payload, ensure_ascii=False) if response_payload is not None else response.text),
                )
            except Exception as exc:
                http_status = ""
                response = getattr(exc, "response", None)
                if response is not None and getattr(response, "status_code", None) is not None:
                    http_status = str(response.status_code)
                raise GlassMasterSyncPartialFailure(
                    f"Glass master sync batch {batch_idx}/{total_batches} failed: {exc}",
                    sent_rows=sent_rows,
                    failed_rows=rows[i:],
                    http_status=http_status,
                ) from exc

        return sent_rows
