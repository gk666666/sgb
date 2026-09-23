import hashlib
import json
import logging
import time
from typing import Callable

import requests
from runtime_settings import (
    build_url,
    get_first_setting,
    get_float_setting,
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


class BaitedaSyncPartialFailure(Exception):
    def __init__(
        self,
        message: str,
        *,
        sent_rows: list[dict],
        failed_rows: list[dict],
        http_status: str = "",
    ):
        super().__init__(message)
        self.sent_rows = sent_rows
        self.failed_rows = failed_rows
        self.http_status = http_status


class BaitedaSync:
    def __init__(self):
        self.base_url = get_first_setting(
            "TRACEABILITY_DEFECT_API_BASE_URL",
            "INTEGRATION_BASE_URL",
        ) or get_integration_base_url()
        self.get_new_time_url = get_first_setting(
            "TRACEABILITY_DEFECT_API_GET_NEW_TIME_URL",
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
            "TRACEABILITY_DEFECT_API_SYNC_DATA_URL",
        )
        if not self.sync_data_url and self.base_url:
            self.sync_data_url = build_url(self.base_url, "/sgb-fm-extend/api/pub/sync/plc/defect/repor")
        self.app_id = get_first_setting(
            "TRACEABILITY_DEFECT_API_APP_ID",
            default=get_integration_env_setting("APP_ID"),
        )
        if not self.app_id:
            raise ValueError("Missing required setting: TRACEABILITY_DEFECT_API_APP_ID / INTEGRATION_*_APP_ID")
        self.app_secret = get_first_setting(
            "TRACEABILITY_DEFECT_API_APP_SECRET",
            default=get_integration_env_setting("APP_SECRET"),
        )
        if not self.app_secret:
            raise ValueError("Missing required setting: TRACEABILITY_DEFECT_API_APP_SECRET / INTEGRATION_*_APP_SECRET")
        self.tenant_id = get_first_setting(
            "TRACEABILITY_DEFECT_API_TENANT_ID",
            default=get_integration_env_setting("TENANT_ID", default="sekurit"),
        )
        self.timeout = get_int_setting(
            "TRACEABILITY_DEFECT_API_TIMEOUT",
            "INTEGRATION_API_TIMEOUT",
            default=60,
            minimum=1,
        )
        logger.info("BaitedaSync initialized with base URL: %s", self.base_url)

    def _get_timestamp(self) -> str:
        response = requests.get(self.get_new_time_url, timeout=(10, self.timeout))
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != 0:
            raise RuntimeError(f"Baiteda getNewTime failed: {payload}")
        timestamp = payload.get("timestamp")
        if timestamp is None:
            raise RuntimeError(f"Baiteda getNewTime missing timestamp: {payload}")
        return str(timestamp)

    def _sign(self, timestamp: str, biz_params: str) -> str:
        raw = f"{self.app_secret}{timestamp}{biz_params}"
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    def _validate_sync_response(self, response: requests.Response) -> dict | None:
        if not response.text.strip():
            logger.warning("Baiteda sync returned empty body with HTTP %s", response.status_code)
            return None

        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Baiteda sync returned non-JSON body: {_truncate_text(response.text)}"
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"Baiteda sync returned unexpected payload type: {type(payload).__name__}")

        code = payload.get("code")
        state = payload.get("state")
        success = payload.get("success")
        if code in (0, "0", "000000") or state == "SUCCESS" or success is True:
            return payload

        raise RuntimeError(f"Baiteda sync business failure: {_truncate_text(json.dumps(payload, ensure_ascii=False))}")

    def _filter_nine_grid(self, defects):
        if not defects:
            return []

        def _present(v):
            if v is None:
                return False
            if isinstance(v, str):
                s = v.strip()
                return s != "" and s.lower() not in ("nan", "none")
            return True

        def _norm(v):
            if v is None:
                return ""
            if not isinstance(v, str):
                v = str(v)
            return v.strip()

        def _is_8pa1(d):
            factory = d.get("factory")
            post = d.get("defect_report_post")
            if isinstance(factory, str) and factory.strip().upper().startswith("8PA1"):
                return True
            return factory == "8PA1" or (
                isinstance(post, str) and (post.startswith("8PA1-") or post.startswith("8pa1_miling"))
            )

        def _is_ws3(d):
            factory = d.get("factory")
            return isinstance(factory, str) and factory.strip().upper().startswith("WS3")

        def _group_key_8pa1(d, _idx):
            dmc = _norm(d.get("dmc_code"))
            rfid = _norm(d.get("rfid"))
            aura = _norm(d.get("aura_code"))
            pdlc = _norm(d.get("pdlc_code"))
            return f"k:{dmc}|{rfid}|{aura}|{pdlc}"

        def _group_key_ws3(d, _idx):
            dmc = _norm(d.get("dmc_code"))
            rfid = _norm(d.get("rfid"))
            if _present(dmc):
                return f"ws3:dmc:{dmc}"
            if _present(rfid):
                return f"ws3:rfid28:{rfid[:28]}"
            plc_id = d.get("plc_id")
            return f"ws3:id:{plc_id or idx}"

        def _group_key_default(d, idx):
            dmc = d.get("dmc_code")
            t = d.get("defect_report_time")
            plc_id = d.get("plc_id")

            t_part = t if isinstance(t, str) else (t.isoformat() if hasattr(t, "isoformat") else "")
            dmc_norm = str(dmc).strip() if dmc is not None else ""

            if _present(dmc_norm) and t:
                return f"dmc:{dmc_norm}|t:{t_part}"
            if _present(dmc_norm):
                return f"dmc:{dmc_norm}|id:{plc_id or idx}"
            if t:
                return f"t:{t_part}|id:{plc_id or idx}"
            return f"id:{plc_id or idx}"

        def _sort_key(d, idx):
            t = d.get("defect_report_time")
            return (str(t) if t is not None else "", idx)

        glass_groups = {}
        for idx, d in enumerate(defects):
            if _is_8pa1(d):
                key = _group_key_8pa1(d, idx)
            elif _is_ws3(d):
                key = _group_key_ws3(d, idx)
            else:
                key = _group_key_default(d, idx)
            glass_groups.setdefault(key, []).append(d)

        filtered = []
        for key, group in glass_groups.items():
            group_sorted = [r for _i, r in sorted(enumerate(group), key=lambda p: _sort_key(p[1], p[0]), reverse=True)]
            is_8pa1_group = _is_8pa1(group_sorted[0])
            if is_8pa1_group:
                defect_recs = [r for r in group_sorted if _present(r.get("defect_code"))]
                if defect_recs:
                    filtered.extend(defect_recs)
                else:
                    filtered.append(group_sorted[0])
            else:
                defect_recs = [
                    r
                    for r in group_sorted
                    if _present(r.get("defect_code")) and _present(r.get("defect_nine_grid"))
                ]
                if defect_recs:
                    filtered.extend(defect_recs)
                else:
                    filtered.append(group_sorted[0])

        return filtered

    def _normalize_group_value(self, value):
        if value is None:
            return ""
        if not isinstance(value, str):
            value = str(value)
        return value.strip()

    def _customer_group_key(self, defect):
        return (
            self._normalize_group_value(defect.get("dmc_code")),
            self._normalize_group_value(defect.get("rfid")),
            self._normalize_group_value(defect.get("aura_code")),
            self._normalize_group_value(defect.get("pdlc_code")),
        )

    def _build_send_batches(self, defects, batch_size, max_groups_per_batch):
        if not defects:
            return []

        grouped_defects = {}
        for defect in defects:
            grouped_defects.setdefault(self._customer_group_key(defect), []).append(defect)

        batches = []
        current_batch = []
        current_group_count = 0

        for group_rows in grouped_defects.values():
            for start in range(0, len(group_rows), batch_size):
                chunk = group_rows[start : start + batch_size]
                if current_batch and (
                    len(current_batch) + len(chunk) > batch_size
                    or (max_groups_per_batch > 0 and current_group_count + 1 > max_groups_per_batch)
                ):
                    batches.append(current_batch)
                    current_batch = []
                    current_group_count = 0

                current_batch.extend(chunk)
                current_group_count += 1

                if len(current_batch) >= batch_size:
                    batches.append(current_batch)
                    current_batch = []
                    current_group_count = 0

        if current_batch:
            batches.append(current_batch)

        return batches

    def sync_defects(self, defects, on_batch_success: Callable[[list[dict]], None] | None = None):
        if not defects:
            return []

        filtered_defects = self._filter_nine_grid(defects)
        logger.info(
            "Filtering defects for sync: original %s, after nine-grid filter %s",
            len(defects),
            len(filtered_defects),
        )

        filtered_ids = {d.get("plc_id") for d in filtered_defects}
        acknowledged_rows = [d for d in defects if d.get("plc_id") not in filtered_ids]

        if not filtered_defects:
            logger.info("No defects to sync after nine-grid filtering.")
            return acknowledged_rows

        def _present(v):
            if v is None:
                return False
            if isinstance(v, str):
                s = v.strip()
                return s != "" and s.lower() not in ("nan", "none")
            return True

        def _format_dt_seconds(dt):
            if dt is None:
                return None
            if hasattr(dt, "strftime"):
                return dt.strftime("%Y-%m-%dT%H:%M:%S")
            s = str(dt).strip()
            return s if s != "" else None

        def _require_local_report_time(defect):
            local_time = defect.get("defect_report_time_local")
            if local_time is None or (isinstance(local_time, str) and local_time.strip() == ""):
                raise ValueError(
                    "Missing defect_report_time_local for plc_id=%s source_topic=%s"
                    % (defect.get("plc_id"), defect.get("source_topic"))
                )
            return local_time

        def _null_if_blank(v):
            if not _present(v):
                return None
            if isinstance(v, str):
                s = v.strip()
                return s if s != "" else None
            return v

        batch_size = get_int_setting(
            "TRACEABILITY_DEFECT_API_BATCH_SIZE",
            "INTEGRATION_API_BATCH_SIZE",
            default=50,
            minimum=1,
        )
        max_groups_per_batch = get_int_setting(
            "TRACEABILITY_DEFECT_API_MAX_GROUPS_PER_BATCH",
            default=min(max(batch_size, 1), 20),
            minimum=1,
        )
        send_batches = self._build_send_batches(filtered_defects, batch_size, max_groups_per_batch)
        total_groups = len({self._customer_group_key(d) for d in filtered_defects})
        logger.info(
            "Prepared %s defects into %s send batches across %s customer groups (batch_size=%s, max_groups_per_batch=%s)",
            len(filtered_defects),
            len(send_batches),
            total_groups,
            batch_size,
            max_groups_per_batch,
        )

        for batch_idx, batch in enumerate(send_batches, start=1):
            batch_group_count = len({self._customer_group_key(d) for d in batch})
            logger.info(
                "Syncing batch %s/%s (size: %s, groups: %s)...",
                batch_idx,
                len(send_batches),
                len(batch),
                batch_group_count,
            )

            payload = {
                "datas": [
                    {
                        "plc_id": d.get("plc_id"),
                        "dmc_code": d.get("dmc_code"),
                        "rfid": d.get("rfid"),
                        "aura_code": d.get("aura_code"),
                        "pdlc_code": d.get("pdlc_code"),
                        "ng_or_reworkable": d.get("status_normalized") if _present(d.get("status_normalized")) else None,
                        "defect_report_time": _format_dt_seconds(_require_local_report_time(d)),
                        "defect_report_post": d.get("defect_report_post"),
                        "defect_code": _null_if_blank(d.get("defect_code")),
                        "defect_nine_grid": d.get("defect_nine_grid_normalized") if _present(d.get("defect_nine_grid_normalized")) else None,
                        "defect_surface": _null_if_blank(d.get("defect_surface")),
                        "mest_result": d.get("mest_result"),
                    }
                    for d in batch
                ]
            }
            biz_params = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

            batch_success = False
            last_err = None
            last_http_status = ""
            for attempt in range(3):
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
                    result = self._validate_sync_response(response)
                    batch_success = True
                    logger.info(
                        "Batch %s success. status=%s response=%s",
                        batch_idx,
                        response.status_code,
                        _truncate_text(json.dumps(result, ensure_ascii=False) if result is not None else response.text),
                    )
                    if on_batch_success is not None:
                        on_batch_success(batch)
                    acknowledged_rows.extend(batch)
                    break
                except requests.exceptions.Timeout as e:
                    last_err = e
                    response = getattr(e, "response", None)
                    if response is not None and getattr(response, "status_code", None) is not None:
                        last_http_status = str(response.status_code)
                    logger.warning("Batch %s timeout (attempt %s/3): %s", batch_idx, attempt + 1, e)
                except Exception as e:
                    last_err = e
                    response = getattr(e, "response", None)
                    if response is not None and getattr(response, "status_code", None) is not None:
                        last_http_status = str(response.status_code)
                    logger.error("Batch %s error: %s", batch_idx, e)
                    break

            if not batch_success:
                logger.error("Batch %s failed after retries. Error: %s", batch_idx, last_err)
                raise BaitedaSyncPartialFailure(
                    f"Batch {batch_idx}/{len(send_batches)} failed after retries: {last_err}",
                    sent_rows=acknowledged_rows,
                    failed_rows=batch,
                    http_status=last_http_status,
                )

            time.sleep(
                get_float_setting(
                    "TRACEABILITY_DEFECT_API_BATCH_SLEEP_SECONDS",
                    default=0.5,
                    minimum=0.0,
                )
            )

        return acknowledged_rows
