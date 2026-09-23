import hashlib
import json
import logging

import requests
from runtime_settings import (
    build_url,
    get_bool_setting,
    get_first_setting,
    get_integration_base_url,
    get_integration_env_setting,
    get_int_setting,
)


logger = logging.getLogger(__name__)

DEFAULT_ALLOWED_TOPICS = (
    "SGH/WS1/NMF180/2CW6/PRINT1/SILK_SCREEN_STATUS",
    "SGH/WS1/NMF180/2CW6/PRINT2",
    "SGH/WS1/NMF180/2CW6/PRINT3",
    "SGH/WS1/MF180/2CW2/PRINT1",
    "SGH/WS1/MF180/2CW2/PRINT2",
    "SGH/WS1/MF180/2CW2/PRINT3/SILK_SCREEN_STATUS",
)


def _truncate_text(value: str, limit: int = 500) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"

def _parse_json_object(value, *, context: str) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        text = value.decode("utf-8")
    else:
        text = str(value)
    try:
        parsed = json.loads(text)
    except Exception as exc:
        raise ValueError(f"{context} is not valid JSON: {_truncate_text(text)}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{context} is not a JSON object: {type(parsed).__name__}")
    return parsed


def parse_silk_screen_event_message(raw_event_body) -> dict:
    envelope = _parse_json_object(raw_event_body, context="Event Hub message")
    payload = envelope.get("payload", envelope)
    payload_dict = _parse_json_object(payload, context="Event Hub payload")

    normalized = dict(payload_dict)
    normalized["__raw_payload__"] = dict(payload_dict)
    for key in ("topic", "message_id", "device_name", "emqx_ts"):
        if key in envelope and key not in normalized:
            normalized[key] = envelope.get(key)
    return normalized


class SilkScreenSyncPartialFailure(RuntimeError):
    def __init__(self, message: str, sent_rows: list[dict], failed_rows: list[dict], http_status: str = ""):
        super().__init__(message)
        self.sent_rows = sent_rows
        self.failed_rows = failed_rows
        self.http_status = http_status


class SilkScreenSync:
    def __init__(self):
        self.base_url = get_first_setting(
            "SILK_SCREEN_API_BASE_URL",
            "INTEGRATION_BASE_URL",
        ) or get_integration_base_url()
        self.get_new_time_url = get_first_setting(
            "SILK_SCREEN_API_GET_NEW_TIME_URL",
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
            "SILK_SCREEN_API_SYNC_DATA_URL",
        )
        if not self.sync_data_url and self.base_url:
            self.sync_data_url = build_url(self.base_url, "/sgb-fm-extend/api/pub/screen/sync/silkDataSync")
        self.app_id = get_first_setting(
            "SILK_SCREEN_API_APP_ID",
            default=get_integration_env_setting("APP_ID"),
        )
        if not self.app_id:
            raise ValueError("Missing required setting: SILK_SCREEN_API_APP_ID / INTEGRATION_*_APP_ID")
        self.app_secret = get_first_setting(
            "SILK_SCREEN_API_APP_SECRET",
            default=get_integration_env_setting("APP_SECRET"),
        )
        if not self.app_secret:
            raise ValueError("Missing required setting: SILK_SCREEN_API_APP_SECRET / INTEGRATION_*_APP_SECRET")
        self.tenant_id = get_first_setting(
            "SILK_SCREEN_API_TENANT_ID",
            default=get_integration_env_setting("TENANT_ID", default="sekurit"),
        )
        self.timeout = get_int_setting(
            "SILK_SCREEN_API_TIMEOUT",
            "INTEGRATION_API_TIMEOUT",
            default=60,
            minimum=1,
        )
        self.batch_size = get_int_setting(
            "SILK_SCREEN_API_BATCH_SIZE",
            "INTEGRATION_API_BATCH_SIZE",
            default=200,
            minimum=1,
        )
        self.dry_run = get_bool_setting(
            "SILK_SCREEN_API_DRY_RUN",
            default=False,
        )
        allowed_topics_text = get_first_setting("SILK_SCREEN_ALLOWED_TOPICS")
        if allowed_topics_text:
            self.allowed_topics = {
                item.strip()
                for item in allowed_topics_text.split(",")
                if item and item.strip()
            }
        else:
            self.allowed_topics = set(DEFAULT_ALLOWED_TOPICS)

        if not self.get_new_time_url:
            raise ValueError(
                "Missing required setting: SILK_SCREEN_API_GET_NEW_TIME_URL. "
                "或配置 SILK_SCREEN_API_BASE_URL / INTEGRATION_*_BASE_URL 以自动拼接 getNewTime 地址。"
            )
        if not self.sync_data_url:
            raise ValueError(
                "Missing required setting: SILK_SCREEN_API_SYNC_DATA_URL. "
                "或配置 SILK_SCREEN_API_BASE_URL / INTEGRATION_*_BASE_URL 以自动拼接丝网接口地址。"
            )

    def _get_timestamp(self) -> str:
        response = requests.get(self.get_new_time_url, timeout=(10, self.timeout))
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"Silk screen getNewTime failed: {payload}")
        timestamp = payload.get("timestamp")
        if timestamp is None:
            raise RuntimeError(f"Silk screen getNewTime missing timestamp: {payload}")
        return str(timestamp)

    def _sign(self, timestamp: str, biz_params: str) -> str:
        raw = f"{self.app_secret}{timestamp}{biz_params}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _validate_sync_response(self, response: requests.Response) -> dict | None:
        if not response.text.strip():
            logger.warning("Silk screen sync returned empty body with HTTP %s", response.status_code)
            return None

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Silk screen sync returned non-JSON body: {_truncate_text(response.text)}"
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"Silk screen sync returned unexpected payload type: {type(payload).__name__}")

        code = payload.get("code")
        state = payload.get("state")
        success = payload.get("success")
        if code in (0, "0", "000000") or state == "SUCCESS" or success is True:
            return payload

        raise RuntimeError(f"Silk screen sync business failure: {_truncate_text(json.dumps(payload, ensure_ascii=False))}")

    def _build_row(self, row: dict) -> dict:
        raw_payload = row.get("__raw_payload__")
        if isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            payload = {
                key: value
                for key, value in row.items()
                if not str(key).startswith("__")
                and key not in {"topic", "message_id", "device_name", "emqx_ts"}
            }
        # 基础字段固定发送：下游接口按这 7 个字段固定接收，源报文缺失时保持原有行为。
        # 结构相关字段（status / 上下网版任务状态）按“源报文里有才发”处理，
        # 用于同时兼容：
        # 1) 老结构 topic（如 .../SILK_SCREEN_STATUS）；
        # 2) 新结构 topic（如 2CW6/PRINT2、2CW6/PRINT3、2CW2/PRINT1、2CW2/PRINT2）；
        # 3) 2CW2/2CW6 已取消 status、仅保留上下网版任务状态的场景。
        result = {
            "DMCode": payload.get("dmc_code"),
            "RFID": payload.get("rfid_code"),
        }
        if "status" in payload:
            result["status"] = payload["status"]
        result.update(
            {
                "silk_screen_life": payload.get("silk_screen_life"),
                "read_time": payload.get("read_time"),
                "workcenter": payload.get("workcenter"),
                "equipment": payload.get("equipment"),
                "silk_screen_status": payload.get("silk_screen_status"),
            }
        )
        for key in ("up_screen_task_status", "down_screen_task_status"):
            if key in payload:
                result[key] = payload[key]
        return result

    def sync_row(self, row: dict) -> dict:
        payload = self._build_row(row)
        biz_params = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

        try:
            if self.dry_run:
                logger.info(
                    "Silk screen sync dry-run single row, topic=%s message_id=%s payload=%s",
                    row.get("topic"),
                    row.get("message_id"),
                    _truncate_text(biz_params),
                )
                return payload

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
            logger.info(
                "Silk screen sync single row succeeded, topic=%s message_id=%s status=%s response=%s",
                row.get("topic"),
                row.get("message_id"),
                response.status_code,
                _truncate_text(json.dumps(response_payload, ensure_ascii=False) if response_payload is not None else response.text),
            )
            return payload
        except Exception as exc:
            http_status = ""
            response = getattr(exc, "response", None)
            if response is not None and getattr(response, "status_code", None) is not None:
                http_status = str(response.status_code)
            raise SilkScreenSyncPartialFailure(
                f"Silk screen sync single row failed: {exc}",
                sent_rows=[],
                failed_rows=[row],
                http_status=http_status,
            ) from exc
