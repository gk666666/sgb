import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Callable
from uuid import NAMESPACE_URL, uuid5

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


class ProdTellusSyncPartialFailure(RuntimeError):
    def __init__(self, message: str, sent_rows: list[dict], failed_rows: list[dict], http_status: str = ""):
        super().__init__(message)
        self.sent_rows = sent_rows
        self.failed_rows = failed_rows
        self.http_status = http_status
def _to_business_local_datetime(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            raise ValueError("Empty datetime value")
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.replace(tzinfo=CN_TZ)


def _format_cn_datetime(value: datetime) -> str:
    dt = value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=timezone.utc).astimezone(CN_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _format_cn_datetime_millis(value: datetime) -> str:
    dt = value.astimezone(CN_TZ) if value.tzinfo else value.replace(tzinfo=timezone.utc).astimezone(CN_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _truncate_text(value: str, limit: int = 500) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


class ProdTellusSync:
    def __init__(self):
        # 产量接口优先使用专属配置；没有单独配置时再回退到通用集成环境变量。
        self.base_url = get_first_setting(
            "PRODUCTION_OUTPUT_API_BASE_URL",
            "INTEGRATION_BASE_URL",
        ) or get_integration_base_url()
        self.get_new_time_url = get_first_setting(
            "PRODUCTION_OUTPUT_API_GET_NEW_TIME_URL",
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
            "PRODUCTION_OUTPUT_API_SYNC_DATA_URL",
        )
        if not self.sync_data_url and self.base_url:
            self.sync_data_url = build_url(self.base_url, "/sgb-fm-extend/api/pub/sync/syncData")
        self.app_id = get_first_setting(
            "PRODUCTION_OUTPUT_API_APP_ID",
            default=get_integration_env_setting("APP_ID"),
        )
        if not self.app_id:
            raise ValueError("Missing required setting: PRODUCTION_OUTPUT_API_APP_ID / INTEGRATION_*_APP_ID")
        self.app_secret = get_first_setting(
            "PRODUCTION_OUTPUT_API_APP_SECRET",
            default=get_integration_env_setting("APP_SECRET"),
        )
        if not self.app_secret:
            raise ValueError("Missing required setting: PRODUCTION_OUTPUT_API_APP_SECRET / INTEGRATION_*_APP_SECRET")
        self.tenant_id = get_first_setting(
            "PRODUCTION_OUTPUT_API_TENANT_ID",
            default=get_integration_env_setting("TENANT_ID", default="sekurit"),
        )
        self.timeout = get_int_setting(
            "PRODUCTION_OUTPUT_API_TIMEOUT",
            "INTEGRATION_API_TIMEOUT",
            default=60,
            minimum=1,
        )
        self.batch_size = get_int_setting(
            "PRODUCTION_OUTPUT_API_BATCH_SIZE",
            "INTEGRATION_API_BATCH_SIZE",
            default=200,
            minimum=1,
        )
        if self.batch_size <= 0:
            self.batch_size = 200
        if not self.get_new_time_url:
            raise ValueError(
                "Missing required setting: PRODUCTION_OUTPUT_API_GET_NEW_TIME_URL. "
                "或配置 INTEGRATION_*_BASE_URL 以自动拼接 getNewTime 地址。"
            )
        if not self.sync_data_url:
            raise ValueError(
                "Missing required setting: PRODUCTION_OUTPUT_API_SYNC_DATA_URL. "
                "或配置 INTEGRATION_*_BASE_URL 以自动拼接产量接口地址。"
            )

    def _get_timestamp(self) -> str:
        # 外层 timestamp 需要向客户的 getNewTime 接口取值，不能直接本地生成。
        response = requests.get(self.get_new_time_url, timeout=(10, self.timeout))
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"Tellus getNewTime failed: {payload}")
        timestamp = payload.get("timestamp")
        if timestamp is None:
            raise RuntimeError(f"Tellus getNewTime missing timestamp: {payload}")
        return str(timestamp)

    def _sign(self, timestamp: str, biz_params: str) -> str:
        raw = f"{self.app_secret}{timestamp}{biz_params}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _build_minute_row(self, row: dict, create_time: datetime) -> dict:
        # 把 ADX 查询出的分钟产量候选行，转换成客户 syncData 接口要求的单条业务报文。
        metric_window_start_cn = _to_business_local_datetime(row["metric_window_start"])
        row_id = str(
            uuid5(
                NAMESPACE_URL,
                f"{row.get('topic')}|{row.get('workcenter')}|{metric_window_start_cn.isoformat()}",
            )
        )
        return {
            "id": row_id,
            "type": row.get("topic"),
            "workCenter": row.get("workcenter"),
            "value": row.get("output_value"),
            "day": metric_window_start_cn.strftime("%Y-%m-%d"),
            "dataTime": _format_cn_datetime(metric_window_start_cn),
            "createTime": _format_cn_datetime_millis(create_time),
            "createBy": "system",
        }

    def _validate_sync_response(self, response: requests.Response) -> dict | None:
        # 对方接口可能返回空体、非 JSON 或业务码失败，这里统一做响应校验。
        if not response.text.strip():
            logger.warning("Prod Tellus syncData returned empty body with HTTP %s", response.status_code)
            return None

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Tellus syncData returned non-JSON body: {_truncate_text(response.text)}"
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"Tellus syncData returned unexpected payload type: {type(payload).__name__}")

        code = payload.get("code")
        if code is not None and str(code) not in {"0", "200"}:
            raise RuntimeError(f"Tellus syncData business failure: {_truncate_text(json.dumps(payload, ensure_ascii=False))}")

        success = payload.get("success")
        if success is not None and str(success).strip().lower() not in {"true", "1"}:
            raise RuntimeError(f"Tellus syncData success flag indicates failure: {_truncate_text(json.dumps(payload, ensure_ascii=False))}")

        return payload

    def sync_minutes(self, rows: list[dict], on_batch_success: Callable[[list[dict]], None] | None = None) -> list[dict]:
        if not rows:
            return []

        # 同一轮执行内 createTime 保持一致，便于客户把这一批识别为同一批次。
        create_time = datetime.now(timezone.utc)
        total_batches = (len(rows) + self.batch_size - 1) // self.batch_size
        sent_rows: list[dict] = []

        for i in range(0, len(rows), self.batch_size):
            # batch_rows 是 ADX 原始候选；batch 才是最终发给接口的 biz_params 数组。
            batch_rows = rows[i : i + self.batch_size]
            batch = [self._build_minute_row(row, create_time) for row in batch_rows]
            batch_idx = (i // self.batch_size) + 1
            biz_params = json.dumps(batch, ensure_ascii=False, separators=(",", ":"))
            try:
                timestamp = self._get_timestamp()
                request_json = {
                    "app_id": self.app_id,
                    "biz_params": biz_params,
                    "sign": self._sign(timestamp, biz_params),
                    "tenant_id": self.tenant_id,
                    "timestamp": timestamp,
                }

                # 只有 HTTP 和业务码都通过后，才通知上层写 success/audit 台账并推进 watermark。
                response = requests.post(self.sync_data_url, json=request_json, timeout=(10, self.timeout))
                response.raise_for_status()
                response_payload = self._validate_sync_response(response)
                if on_batch_success is not None:
                    on_batch_success(batch_rows)
                sent_rows.extend(batch_rows)
                logger.info(
                    "Prod Tellus sync batch %s/%s succeeded, rows=%s, status=%s, response=%s",
                    batch_idx,
                    total_batches,
                    len(batch),
                    response.status_code,
                    _truncate_text(json.dumps(response_payload, ensure_ascii=False) if response_payload is not None else response.text),
                )
            except Exception as exc:
                # 抛给上层做部分失败处理：上层会决定哪些行记失败审计、watermark 是否保持不动。
                http_status = ""
                response = getattr(exc, "response", None)
                if response is not None and getattr(response, "status_code", None) is not None:
                    http_status = str(response.status_code)
                raise ProdTellusSyncPartialFailure(
                    f"Prod Tellus sync batch {batch_idx}/{total_batches} failed: {exc}",
                    sent_rows=sent_rows,
                    failed_rows=rows[i:],
                    http_status=http_status,
                ) from exc

        return sent_rows
