import hashlib
import json
import logging
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


def _compact_dict(value: dict) -> dict:
    return {key: item for key, item in value.items() if item is not None}


class ChemicalTraceabilitySyncPartialFailure(RuntimeError):
    def __init__(self, message: str, sent_rows: list[dict], failed_rows: list[dict], http_status: str = ""):
        super().__init__(message)
        self.sent_rows = sent_rows
        self.failed_rows = failed_rows
        self.http_status = http_status


class ChemicalTraceabilitySync:
    def __init__(self):
        self.base_url = get_first_setting(
            "CHEMICAL_TRACEABILITY_API_BASE_URL",
            "INTEGRATION_BASE_URL",
        ) or get_integration_base_url()
        self.get_new_time_url = get_first_setting(
            "CHEMICAL_TRACEABILITY_API_GET_NEW_TIME_URL",
            "INTEGRATION_GET_NEW_TIME_URL",
        )
        if not self.get_new_time_url and self.base_url:
            self.get_new_time_url = build_url(
                self.base_url,
                get_first_setting(
                    "INTEGRATION_GET_NEW_TIME_PATH",
                    default="/sgb-fm-extend/api/pub/sync/getNewTime",
                ),
            )
        self.sync_data_url = get_first_setting(
            "CHEMICAL_TRACEABILITY_API_SYNC_DATA_URL",
        )
        if not self.sync_data_url and self.base_url:
            self.sync_data_url = build_url(self.base_url, "/sgb-fm-extend/api/pub/sync/plc/glass/defect/sync")
        self.app_id = get_first_setting(
            "CHEMICAL_TRACEABILITY_API_APP_ID",
            default=get_integration_env_setting("APP_ID"),
        )
        if not self.app_id:
            raise ValueError("Missing required setting: CHEMICAL_TRACEABILITY_API_APP_ID / INTEGRATION_*_APP_ID")
        self.app_secret = get_first_setting(
            "CHEMICAL_TRACEABILITY_API_APP_SECRET",
            default=get_integration_env_setting("APP_SECRET"),
        )
        if not self.app_secret:
            raise ValueError("Missing required setting: CHEMICAL_TRACEABILITY_API_APP_SECRET / INTEGRATION_*_APP_SECRET")
        self.tenant_id = get_first_setting(
            "CHEMICAL_TRACEABILITY_API_TENANT_ID",
            default=get_integration_env_setting("TENANT_ID", default="sekurit"),
        )
        self.timeout = get_int_setting(
            "CHEMICAL_TRACEABILITY_API_TIMEOUT",
            "INTEGRATION_API_TIMEOUT",
            default=60,
            minimum=1,
        )
        self.batch_size = get_int_setting(
            "CHEMICAL_TRACEABILITY_API_BATCH_SIZE",
            "INTEGRATION_API_BATCH_SIZE",
            default=200,
            minimum=1,
        )
        if self.batch_size <= 0:
            self.batch_size = 200
        if not self.sync_data_url:
            raise ValueError(
                "Missing required setting: CHEMICAL_TRACEABILITY_API_SYNC_DATA_URL. "
                "或配置 CHEMICAL_TRACEABILITY_API_BASE_URL / GLASS_MASTER_API_BASE_URL / INTEGRATION_*_BASE_URL 以自动拼接接口地址。"
            )
        if not self.get_new_time_url:
            raise ValueError(
                "Missing required setting: CHEMICAL_TRACEABILITY_API_GET_NEW_TIME_URL / INTEGRATION_GET_NEW_TIME_URL"
            )

    def _get_timestamp(self) -> str:
        response = requests.get(self.get_new_time_url, timeout=(10, self.timeout))
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"Chemical traceability getNewTime failed: {_truncate_text(json.dumps(payload, ensure_ascii=False))}")
        timestamp = payload.get("timestamp")
        if timestamp is None:
            raise RuntimeError(f"Chemical traceability getNewTime missing timestamp: {_truncate_text(json.dumps(payload, ensure_ascii=False))}")
        return str(timestamp)

    def _sign(self, timestamp: str, biz_params: str) -> str:
        raw = f"{self.app_secret}{timestamp}{biz_params}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _validate_sync_response(self, response: requests.Response) -> dict | None:
        if not response.text.strip():
            logger.warning("Chemical traceability sync returned empty body with HTTP %s", response.status_code)
            return None

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Chemical traceability sync returned non-JSON body: {_truncate_text(response.text)}"
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError(
                f"Chemical traceability sync returned unexpected payload type: {type(payload).__name__}"
            )

        code = payload.get("code")
        state = payload.get("state")
        success = payload.get("success")
        if code in (0, "0", "000000") or state == "SUCCESS" or success is True:
            return payload

        raise RuntimeError(
            f"Chemical traceability sync business failure: {_truncate_text(json.dumps(payload, ensure_ascii=False))}"
        )

    def _build_row(self, row: dict) -> dict:
        return _compact_dict(
            {
                "plc_id": _null_if_blank(row.get("plc_id")),
                "dmc_code": _null_if_blank(row.get("dmc_code")),
                "data_time": _null_if_blank(row.get("data_time")),
                "report_post": _null_if_blank(row.get("report_post")),
            }
        )

    def sync_rows(self, rows: list[dict], on_batch_success: Callable[[list[dict]], None] | None = None) -> list[dict]:
        if not rows:
            return []

        total_batches = (len(rows) + self.batch_size - 1) // self.batch_size
        sent_rows: list[dict] = []

        for i in range(0, len(rows), self.batch_size):
            batch_rows = rows[i : i + self.batch_size]
            batch_payload = [self._build_row(row) for row in batch_rows]
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
                    "Chemical traceability sync batch %s/%s succeeded, rows=%s, status=%s, response=%s",
                    batch_idx,
                    total_batches,
                    len(batch_rows),
                    response.status_code,
                    _truncate_text(
                        json.dumps(response_payload, ensure_ascii=False) if response_payload is not None else response.text
                    ),
                )
            except Exception as exc:
                http_status = ""
                response = getattr(exc, "response", None)
                if response is not None and getattr(response, "status_code", None) is not None:
                    http_status = str(response.status_code)
                raise ChemicalTraceabilitySyncPartialFailure(
                    f"Chemical traceability sync batch {batch_idx}/{total_batches} failed: {exc}",
                    sent_rows=sent_rows,
                    failed_rows=rows[i:],
                    http_status=http_status,
                ) from exc

        return sent_rows
