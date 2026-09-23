import json
import logging
import os
import threading
import time
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import azure.functions as func
import requests
from azure.identity import DefaultAzureCredential

from chemical_traceability_forward_sync import (
    ChemicalTraceabilitySync,
    ChemicalTraceabilitySyncPartialFailure,
)
from defect_forward_sync import BaitedaSync, BaitedaSyncPartialFailure
from glass_master_forward_sync import GlassMasterSync, GlassMasterSyncPartialFailure
from print_adx_export_to_blob import export_print_batch_to_blob
from print_blob_to_snowflake_loader import load_print_batch_from_manifest
from prod_output_forward_sync import ProdTellusSync, ProdTellusSyncPartialFailure
from runtime_settings import get_bool_setting, get_first_setting, get_int_setting, get_required_setting
from silk_screen_forward_sync import SilkScreenSync, parse_silk_screen_event_message
from watermark_store import WatermarkStore


app = func.FunctionApp()
CHEMICAL_TRACEABILITY_FORWARD_LOCK = threading.Lock()
GLASS_MASTER_FORWARD_LOCK = threading.Lock()
PROD_FORWARD_LOCK = threading.Lock()
PROD_ROLLUP_LOCK = threading.Lock()

# 缺陷发送默认走 v2 全量 equivalent 正式链路。
# 除非明确要回滚到旧版 5FN6 单链路，否则这里应保持与
# fn_defect_forward_candidates_scheme_b_v2 一致。
DEFAULT_QUERY = """
fn_defect_forward_candidates_scheme_b_v2(
    todatetime('{watermark}'),
    {overlap_minutes},
    {success_lookback_hours},
    '{forward_target}'
)
| order by watermark_time_utc asc, plc_id asc
""".strip()

PROD_FORWARD_QUERY = """
fn_prod_tellus_forward_candidates_with_watermark_v1(
    todatetime('{watermark}'),
    {overlap_minutes},
    '{forward_target}',
    {success_lookback_hours}
)
| order by metric_window_start asc, topic asc
""".strip()

GLASS_MASTER_FORWARD_QUERY = """
fn_glass_master_forward_candidates(
    todatetime('{watermark}'),
    {overlap_minutes},
    {success_lookback_hours},
    '{forward_target}'
)
| order by watermark_time_utc asc, master_id asc
""".strip()

CHEMICAL_TRACEABILITY_FORWARD_QUERY = """
fn_chemical_traceability_forward_candidates(
    todatetime('{watermark}'),
    {overlap_minutes},
    {success_lookback_hours},
    '{forward_target}'
)
| order by watermark_time_utc asc, plc_id asc
""".strip()

REQUIRED_COLUMNS = {
    "plc_id",
    "dmc_code",
    "rfid",
    "aura_code",
    "pdlc_code",
    "status",
    "defect_report_time",
    "defect_report_time_local",
    "defect_report_post",
    "defect_code",
    "defect_nine_grid",
    "defect_surface",
    "mest_result",
    "source_time",
    "watermark_time_utc",
}

GLASS_MASTER_REQUIRED_COLUMNS = {
    "master_id",
    "source_message_id",
    "payload_family",
    "factory",
    "watermark_time_utc",
}

CHEMICAL_TRACEABILITY_REQUIRED_COLUMNS = {
    "plc_id",
    "dmc_code",
    "data_time",
    "report_post",
    "watermark_time_utc",
}


def _get_credential() -> DefaultAzureCredential:
    return DefaultAzureCredential()


def _get_token(credential: DefaultAzureCredential, scope: str) -> str:
    return credential.get_token(scope).token


def _query_adx(cluster_url: str, database: str, query: str, scope: str, credential: DefaultAzureCredential | None = None) -> list[dict]:
    credential = credential or _get_credential()
    token = _get_token(credential, scope)
    response = requests.post(
        f"{cluster_url.rstrip('/')}/v2/rest/query",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"db": database, "csl": query},
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()

    if isinstance(payload, dict):
        tables = payload.get("Tables", [])
    elif isinstance(payload, list):
        tables = payload
    else:
        raise ValueError(f"Unexpected ADX response type: {type(payload).__name__}")

    primary = None
    for table in tables:
        if not isinstance(table, dict):
            continue
        if table.get("TableKind") == "PrimaryResult":
            primary = table
            break
    if primary is None:
        for table in tables:
            if isinstance(table, dict) and table.get("Rows") and table.get("Columns"):
                primary = table
                break
    if primary is None:
        return []

    columns = [column["ColumnName"] for column in primary.get("Columns", [])]
    rows = []
    for row in primary.get("Rows", []):
        item = {}
        for idx, col_name in enumerate(columns):
            item[col_name] = row[idx]
        rows.append(item)
    return rows


def _execute_adx_mgmt(
    cluster_url: str,
    database: str,
    command: str,
    scope: str,
    credential: DefaultAzureCredential | None = None,
) -> dict | list:
    credential = credential or _get_credential()
    token = _get_token(credential, scope)
    max_retries = int(os.getenv("ADX_MGMT_MAX_RETRIES", "3"))
    base_delay_seconds = float(os.getenv("ADX_MGMT_RETRY_BASE_SECONDS", "2"))
    last_response = None

    for attempt in range(max_retries + 1):
        response = requests.post(
            f"{cluster_url.rstrip('/')}/v1/rest/mgmt",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"db": database, "csl": command},
            timeout=60,
        )
        if response.status_code != 429:
            response.raise_for_status()
            if not response.text.strip():
                return {}
            return response.json()

        last_response = response
        if attempt >= max_retries:
            break

        retry_after_seconds = response.headers.get("Retry-After")
        if retry_after_seconds:
            try:
                sleep_seconds = float(retry_after_seconds)
            except Exception:
                sleep_seconds = base_delay_seconds * (2 ** attempt)
        else:
            retry_after_ms = response.headers.get("x-ms-retry-after-ms")
            if retry_after_ms:
                try:
                    sleep_seconds = float(retry_after_ms) / 1000.0
                except Exception:
                    sleep_seconds = base_delay_seconds * (2 ** attempt)
            else:
                sleep_seconds = base_delay_seconds * (2 ** attempt)

        logging.warning(
            "ADX mgmt command hit 429, retrying in %.1fs (attempt %s/%s): %s",
            sleep_seconds,
            attempt + 1,
            max_retries,
            command,
        )
        time.sleep(sleep_seconds)

    last_response.raise_for_status()
    return {}


def _validate_columns(rows: list[dict]):
    if not rows:
        return
    missing = REQUIRED_COLUMNS.difference(rows[0].keys())
    if missing:
        raise ValueError(f"ADX query result missing required columns: {', '.join(sorted(missing))}")


def _validate_glass_master_columns(rows: list[dict]):
    if not rows:
        return
    missing = GLASS_MASTER_REQUIRED_COLUMNS.difference(rows[0].keys())
    if missing:
        raise ValueError(f"ADX glass master query result missing required columns: {', '.join(sorted(missing))}")


def _validate_chemical_traceability_columns(rows: list[dict]):
    if not rows:
        return
    missing = CHEMICAL_TRACEABILITY_REQUIRED_COLUMNS.difference(rows[0].keys())
    if missing:
        raise ValueError(
            f"ADX chemical traceability query result missing required columns: {', '.join(sorted(missing))}"
        )


def _max_watermark(rows: list[dict]) -> str:
    values = [_parse_utc_datetime(row.get("watermark_time_utc")) for row in rows if row.get("watermark_time_utc")]
    values = [value for value in values if value is not None]
    if not values:
        raise ValueError("No watermark_time_utc values found in queried rows.")
    return _format_kusto_datetime(max(values))


def _max_prod_watermark(rows: list[dict]) -> str:
    values = [_parse_prod_metric_window_start_as_utc(row.get("metric_window_start")) for row in rows if row.get("metric_window_start") is not None]
    values = [value for value in values if value is not None]
    if not values:
        raise ValueError("No metric_window_start values found in queried prod rows.")
    return _format_kusto_datetime(max(values))


def _get_setting(*names: str, default: str | None = None) -> str | None:
    return get_first_setting(*names, default=default)


def _get_required_setting_alias(*names: str) -> str:
    return get_required_setting(*names)


def _get_int_setting(default: int, *names: str, minimum: int | None = None) -> int:
    return get_int_setting(*names, default=default, minimum=minimum)


def _get_bool_setting(default: bool, *names: str) -> bool:
    return get_bool_setting(*names, default=default)


def _parse_utc_datetime(value) -> datetime | None:
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


def _parse_prod_metric_window_start_as_utc(value) -> datetime | None:
    dt = _parse_utc_datetime(value)
    if dt is None:
        return None
    # prod_output_minute.metric_window_start is stored as Shanghai local business time.
    # Convert that local clock time into a real UTC watermark before saving to blob.
    if dt.tzinfo is None:
        return None
    naive_local = dt.replace(tzinfo=None)
    shanghai_tz = timezone(timedelta(hours=8))
    return naive_local.replace(tzinfo=shanghai_tz).astimezone(timezone.utc)


def _ceil_hours_from_seconds(total_seconds: float) -> int:
    if total_seconds <= 0:
        return 0
    return int((total_seconds + 3599) // 3600)


def _clamp_success_lookback_hours(raw_hours: int, *, min_hours: int, max_hours: int) -> int:
    value = max(min_hours, raw_hours)
    if max_hours > 0:
        value = min(value, max_hours)
    return value


def _compute_defect_success_lookback_hours(watermark: str, overlap_minutes: int) -> int:
    min_hours = _get_int_setting(6, "TRACEABILITY_DEFECT_SUCCESS_MIN_LOOKBACK_HOURS", minimum=1)
    max_hours = _get_int_setting(168, "TRACEABILITY_DEFECT_SUCCESS_MAX_LOOKBACK_HOURS", minimum=min_hours)
    buffer_hours = _get_int_setting(2, "TRACEABILITY_DEFECT_SUCCESS_BUFFER_HOURS", minimum=0)
    overlap_hours = _ceil_hours_from_seconds(max(overlap_minutes, 0) * 60.0)
    watermark_dt = _parse_utc_datetime(watermark)
    lag_hours = 0
    if watermark_dt is not None:
        lag_hours = _ceil_hours_from_seconds((datetime.now(timezone.utc) - watermark_dt).total_seconds())
    return _clamp_success_lookback_hours(
        lag_hours + overlap_hours + buffer_hours,
        min_hours=min_hours,
        max_hours=max_hours,
    )


def _compute_prod_success_lookback_hours(watermark: str, overlap_minutes: int) -> int:
    min_hours = _get_int_setting(6, "PRODUCTION_OUTPUT_SUCCESS_MIN_LOOKBACK_HOURS", minimum=1)
    max_hours = _get_int_setting(168, "PRODUCTION_OUTPUT_SUCCESS_MAX_LOOKBACK_HOURS", minimum=min_hours)
    buffer_hours = _get_int_setting(2, "PRODUCTION_OUTPUT_SUCCESS_BUFFER_HOURS", minimum=0)
    overlap_hours = _ceil_hours_from_seconds(max(overlap_minutes, 0) * 60.0)
    watermark_dt = _parse_utc_datetime(watermark)
    lag_hours = 0
    if watermark_dt is not None:
        lag_hours = _ceil_hours_from_seconds((datetime.now(timezone.utc) - watermark_dt).total_seconds())
    return _clamp_success_lookback_hours(
        lag_hours + overlap_hours + buffer_hours,
        min_hours=min_hours,
        max_hours=max_hours,
    )


def _compute_glass_master_success_lookback_hours(watermark: str, overlap_minutes: int) -> int:
    min_hours = _get_int_setting(6, "GLASS_MASTER_SUCCESS_MIN_LOOKBACK_HOURS", minimum=1)
    max_hours = _get_int_setting(168, "GLASS_MASTER_SUCCESS_MAX_LOOKBACK_HOURS", minimum=min_hours)
    buffer_hours = _get_int_setting(2, "GLASS_MASTER_SUCCESS_BUFFER_HOURS", minimum=0)
    overlap_hours = _ceil_hours_from_seconds(max(overlap_minutes, 0) * 60.0)
    watermark_dt = _parse_utc_datetime(watermark)
    lag_hours = 0
    if watermark_dt is not None:
        lag_hours = _ceil_hours_from_seconds((datetime.now(timezone.utc) - watermark_dt).total_seconds())
    return _clamp_success_lookback_hours(
        lag_hours + overlap_hours + buffer_hours,
        min_hours=min_hours,
        max_hours=max_hours,
    )


def _compute_chemical_traceability_success_lookback_hours(watermark: str, overlap_minutes: int) -> int:
    min_hours = _get_int_setting(6, "CHEMICAL_TRACEABILITY_SUCCESS_MIN_LOOKBACK_HOURS", minimum=1)
    max_hours = _get_int_setting(168, "CHEMICAL_TRACEABILITY_SUCCESS_MAX_LOOKBACK_HOURS", minimum=min_hours)
    buffer_hours = _get_int_setting(2, "CHEMICAL_TRACEABILITY_SUCCESS_BUFFER_HOURS", minimum=0)
    overlap_hours = _ceil_hours_from_seconds(max(overlap_minutes, 0) * 60.0)
    watermark_dt = _parse_utc_datetime(watermark)
    lag_hours = 0
    if watermark_dt is not None:
        lag_hours = _ceil_hours_from_seconds((datetime.now(timezone.utc) - watermark_dt).total_seconds())
    return _clamp_success_lookback_hours(
        lag_hours + overlap_hours + buffer_hours,
        min_hours=min_hours,
        max_hours=max_hours,
    )


def _normalize_rows(rows: list[dict]) -> list[dict]:
    normalized = []
    for row in rows:
        item = dict(row)
        for key in (
            "defect_report_time",
            "defect_report_time_local",
            "source_time",
            "effective_source_time",
            "adx_ingest_time",
            "watermark_time_utc",
            "watermark_time_local",
        ):
            value = item.get(key)
            if value is None or hasattr(value, "strftime"):
                continue
            try:
                item[key] = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except Exception:
                item[key] = value
        normalized.append(item)
    return normalized


def _json_log_default(value):
    if isinstance(value, datetime):
        return _format_kusto_datetime(value)
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _build_query(watermark: str, overlap_minutes: int, success_lookback_hours: int, forward_target: str) -> str:
    custom_query = _get_setting("TRACEABILITY_DEFECT_FORWARD_QUERY")
    template = custom_query.strip() if custom_query else DEFAULT_QUERY
    return template.format(
        watermark=watermark,
        overlap_minutes=overlap_minutes,
        success_lookback_hours=success_lookback_hours,
        forward_target=_escape_kusto_string(forward_target),
    )


def _build_glass_master_forward_query(watermark: str, overlap_minutes: int, success_lookback_hours: int, forward_target: str) -> str:
    custom_query = _get_setting("GLASS_MASTER_FORWARD_QUERY")
    template = custom_query.strip() if custom_query else GLASS_MASTER_FORWARD_QUERY
    return template.format(
        watermark=watermark,
        overlap_minutes=overlap_minutes,
        success_lookback_hours=success_lookback_hours,
        forward_target=_escape_kusto_string(forward_target),
    )


def _build_chemical_traceability_forward_query(
    watermark: str,
    overlap_minutes: int,
    success_lookback_hours: int,
    forward_target: str,
) -> str:
    custom_query = _get_setting("CHEMICAL_TRACEABILITY_FORWARD_QUERY")
    template = custom_query.strip() if custom_query else CHEMICAL_TRACEABILITY_FORWARD_QUERY
    return template.format(
        watermark=watermark,
        overlap_minutes=overlap_minutes,
        success_lookback_hours=success_lookback_hours,
        forward_target=_escape_kusto_string(forward_target),
    )


def _escape_kusto_string(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''").replace("\r", " ").replace("\n", " ")


def _format_kusto_datetime(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    return str(value)


def _format_local_display(value) -> str:
    dt = _parse_utc_datetime(value)
    if dt is None:
        return ""
    shanghai_tz = timezone(timedelta(hours=8))
    return dt.astimezone(shanghai_tz).isoformat()


def _format_kusto_value(value) -> str:
    if value is None:
        return "''"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, datetime):
        return f"datetime('{_escape_kusto_string(_format_kusto_datetime(value))}')"
    return f"'{_escape_kusto_string(value)}'"


def _append_table_rows(
    cluster_url: str,
    database: str,
    table_name: str,
    columns: list[tuple[str, str]],
    rows: list[dict],
    scope: str,
    credential: DefaultAzureCredential,
) -> None:
    if not rows:
        return
    max_rows_per_append = _get_int_setting(500, "ADX_APPEND_TABLE_MAX_ROWS", minimum=1)
    schema = ", ".join(f"{name}:{dtype}" for name, dtype in columns)
    for start in range(0, len(rows), max_rows_per_append):
        chunk = rows[start : start + max_rows_per_append]
        value_lines = []
        for row in chunk:
            rendered = ", ".join(_format_kusto_value(row.get(name)) for name, _dtype in columns)
            value_lines.append(rendered)
        values = ",\n".join(value_lines)
        command = f".set-or-append {table_name} <| datatable({schema})[\n{values}\n]"
        _execute_adx_mgmt(cluster_url=cluster_url, database=database, command=command, scope=scope, credential=credential)


def _build_prod_forward_query(watermark: str, overlap_minutes: int, forward_target: str, success_lookback_hours: int) -> str:
    custom_query = _get_setting("PRODUCTION_OUTPUT_FORWARD_QUERY")
    template = custom_query.strip() if custom_query else PROD_FORWARD_QUERY
    return template.format(
        watermark=watermark,
        overlap_minutes=overlap_minutes,
        forward_target=_escape_kusto_string(forward_target),
        success_lookback_hours=success_lookback_hours,
    )


def _normalize_prod_rows(rows: list[dict]) -> list[dict]:
    normalized = []
    for row in rows:
        item = dict(row)
        for key in ("metric_window_start", "rebuilt_at"):
            value = item.get(key)
            if value is None or hasattr(value, "strftime"):
                continue
            try:
                item[key] = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except Exception:
                item[key] = value
        normalized.append(item)
    return normalized


def _format_prod_forward_key_time(value) -> str:
    if isinstance(value, datetime):
        dt = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    text = str(value).strip()
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return text


def _build_prod_forward_key(row: dict) -> str:
    return f"{row.get('topic')}|{row.get('workcenter')}|{_format_prod_forward_key_time(row.get('metric_window_start'))}"


def _run_prod_rebuild_command(
    cluster_url: str,
    database: str,
    scope: str,
    credential: DefaultAzureCredential,
    command: str,
) -> None:
    logging.info("Running ADX prod rebuild command: %s", command)
    _execute_adx_mgmt(
        cluster_url=cluster_url,
        database=database,
        command=command,
        scope=scope,
        credential=credential,
    )


def _compute_prod_initial_watermark(lookback_hours: int) -> str:
    configured = _get_setting("PRODUCTION_OUTPUT_INITIAL_WATERMARK")
    if configured and configured.strip():
        return configured.strip()
    fallback = datetime.now(timezone.utc) - timedelta(hours=max(lookback_hours, 0))
    return _format_kusto_datetime(fallback)


def _compute_prod_rebuild_start_utc(watermark: str, overlap_minutes: int) -> datetime:
    watermark_dt = _parse_utc_datetime(watermark)
    if watermark_dt is None:
        watermark_dt = datetime.now(timezone.utc)
    rebuild_start = watermark_dt - timedelta(minutes=max(overlap_minutes, 0))
    today_local = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))
    today_local_start = today_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_utc = today_local_start.astimezone(timezone.utc)
    return min(today_start_utc, rebuild_start)


def _build_success_rows(
    acknowledged_rows: list[dict],
    forward_target: str,
    batch_id: str,
    watermark_before: str,
    watermark_after: str,
    function_instance_id: str,
) -> list[dict]:
    ack_time = datetime.now(timezone.utc)
    items = []
    for row in acknowledged_rows:
        items.append(
            {
                "plc_id": row.get("plc_id"),
                "forward_target": forward_target,
                "ack_time": ack_time,
                "batch_id": batch_id,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "function_instance_id": function_instance_id,
                "payload_family": row.get("payload_family"),
                "factory": row.get("factory"),
            }
        )
    return items


def _build_glass_master_success_rows(
    acknowledged_rows: list[dict],
    *,
    forward_target: str,
    batch_id: str,
    watermark_before: str,
    watermark_after: str,
    function_instance_id: str,
) -> list[dict]:
    ack_time = datetime.now(timezone.utc)
    items = []
    for row in acknowledged_rows:
        items.append(
            {
                "master_id": row.get("master_id"),
                "forward_target": forward_target,
                "ack_time": ack_time,
                "batch_id": batch_id,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "function_instance_id": function_instance_id,
                "payload_family": row.get("payload_family"),
                "factory": row.get("factory"),
            }
        )
    return items


def _build_audit_rows(
    rows: list[dict],
    *,
    forward_target: str,
    batch_id: str,
    status: str,
    watermark_before: str,
    watermark_after: str,
    function_instance_id: str,
    error_message: str = "",
    http_status: str = "",
) -> list[dict]:
    attempt_time = datetime.now(timezone.utc)
    row_count = len(rows)
    items = []
    for row in rows:
        items.append(
            {
                "plc_id": row.get("plc_id"),
                "forward_target": forward_target,
                "attempt_time": attempt_time,
                "batch_id": batch_id,
                "status": status,
                "http_status": http_status,
                "error_message": error_message,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "function_instance_id": function_instance_id,
                "rows_in_batch": row_count,
                "payload_family": row.get("payload_family"),
                "factory": row.get("factory"),
            }
        )
    return items


def _build_glass_master_audit_rows(
    rows: list[dict],
    *,
    forward_target: str,
    batch_id: str,
    status: str,
    watermark_before: str,
    watermark_after: str,
    function_instance_id: str,
    error_message: str = "",
    http_status: str = "",
) -> list[dict]:
    attempt_time = datetime.now(timezone.utc)
    row_count = len(rows)
    items = []
    for row in rows:
        items.append(
            {
                "master_id": row.get("master_id"),
                "forward_target": forward_target,
                "attempt_time": attempt_time,
                "batch_id": batch_id,
                "status": status,
                "http_status": http_status,
                "error_message": error_message,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "function_instance_id": function_instance_id,
                "rows_in_batch": row_count,
                "payload_family": row.get("payload_family"),
                "factory": row.get("factory"),
            }
        )
    return items


def _build_chemical_traceability_success_rows(
    acknowledged_rows: list[dict],
    *,
    forward_target: str,
    batch_id: str,
    watermark_before: str,
    watermark_after: str,
    function_instance_id: str,
) -> list[dict]:
    ack_time = datetime.now(timezone.utc)
    items = []
    for row in acknowledged_rows:
        items.append(
            {
                "plc_id": row.get("plc_id"),
                "forward_target": forward_target,
                "ack_time": ack_time,
                "batch_id": batch_id,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "function_instance_id": function_instance_id,
                "source_topic": row.get("source_topic"),
                "workcenter": row.get("workcenter"),
                "report_post": row.get("report_post"),
            }
        )
    return items


def _build_chemical_traceability_audit_rows(
    rows: list[dict],
    *,
    forward_target: str,
    batch_id: str,
    status: str,
    watermark_before: str,
    watermark_after: str,
    function_instance_id: str,
    error_message: str = "",
    http_status: str = "",
) -> list[dict]:
    attempt_time = datetime.now(timezone.utc)
    row_count = len(rows)
    items = []
    for row in rows:
        items.append(
            {
                "plc_id": row.get("plc_id"),
                "forward_target": forward_target,
                "attempt_time": attempt_time,
                "batch_id": batch_id,
                "status": status,
                "http_status": http_status,
                "error_message": error_message,
                "watermark_before": watermark_before,
                "watermark_after": watermark_after,
                "function_instance_id": function_instance_id,
                "rows_in_batch": row_count,
                "source_topic": row.get("source_topic"),
                "workcenter": row.get("workcenter"),
                "report_post": row.get("report_post"),
            }
        )
    return items


def _build_prod_success_rows(
    acknowledged_rows: list[dict],
    *,
    forward_target: str,
    batch_id: str,
    function_instance_id: str,
) -> list[dict]:
    ack_time = datetime.now(timezone.utc)
    items = []
    for row in acknowledged_rows:
        items.append(
            {
                "forward_key": _build_prod_forward_key(row),
                "forward_target": forward_target,
                "ack_time": ack_time,
                "batch_id": batch_id,
                "function_instance_id": function_instance_id,
                "topic": row.get("topic"),
                "workcenter": row.get("workcenter"),
                "metric_window_start": row.get("metric_window_start"),
                "output_value": row.get("output_value"),
            }
        )
    return items


def _build_prod_audit_rows(
    rows: list[dict],
    *,
    forward_target: str,
    batch_id: str,
    status: str,
    function_instance_id: str,
    error_message: str = "",
    http_status: str = "",
) -> list[dict]:
    attempt_time = datetime.now(timezone.utc)
    row_count = len(rows)
    items = []
    for row in rows:
        items.append(
            {
                "forward_key": _build_prod_forward_key(row),
                "forward_target": forward_target,
                "attempt_time": attempt_time,
                "batch_id": batch_id,
                "status": status,
                "http_status": http_status,
                "error_message": error_message,
                "function_instance_id": function_instance_id,
                "rows_in_batch": row_count,
                "topic": row.get("topic"),
                "workcenter": row.get("workcenter"),
                "metric_window_start": row.get("metric_window_start"),
                "output_value": row.get("output_value"),
            }
        )
    return items


SUCCESS_TABLE_COLUMNS = [
    ("plc_id", "string"),
    ("forward_target", "string"),
    ("ack_time", "datetime"),
    ("batch_id", "string"),
    ("watermark_before", "string"),
    ("watermark_after", "string"),
    ("function_instance_id", "string"),
    ("payload_family", "string"),
    ("factory", "string"),
]


GLASS_MASTER_SUCCESS_TABLE_COLUMNS = [
    ("master_id", "string"),
    ("forward_target", "string"),
    ("ack_time", "datetime"),
    ("batch_id", "string"),
    ("watermark_before", "string"),
    ("watermark_after", "string"),
    ("function_instance_id", "string"),
    ("payload_family", "string"),
    ("factory", "string"),
]


PROD_SUCCESS_TABLE_COLUMNS = [
    ("forward_key", "string"),
    ("forward_target", "string"),
    ("ack_time", "datetime"),
    ("batch_id", "string"),
    ("function_instance_id", "string"),
    ("topic", "string"),
    ("workcenter", "string"),
    ("metric_window_start", "datetime"),
    ("output_value", "real"),
]


PROD_AUDIT_TABLE_COLUMNS = [
    ("forward_key", "string"),
    ("forward_target", "string"),
    ("attempt_time", "datetime"),
    ("batch_id", "string"),
    ("status", "string"),
    ("http_status", "string"),
    ("error_message", "string"),
    ("function_instance_id", "string"),
    ("rows_in_batch", "int"),
    ("topic", "string"),
    ("workcenter", "string"),
    ("metric_window_start", "datetime"),
    ("output_value", "real"),
]


AUDIT_TABLE_COLUMNS = [
    ("plc_id", "string"),
    ("forward_target", "string"),
    ("attempt_time", "datetime"),
    ("batch_id", "string"),
    ("status", "string"),
    ("http_status", "string"),
    ("error_message", "string"),
    ("watermark_before", "string"),
    ("watermark_after", "string"),
    ("function_instance_id", "string"),
    ("rows_in_batch", "int"),
    ("payload_family", "string"),
    ("factory", "string"),
]


GLASS_MASTER_AUDIT_TABLE_COLUMNS = [
    ("master_id", "string"),
    ("forward_target", "string"),
    ("attempt_time", "datetime"),
    ("batch_id", "string"),
    ("status", "string"),
    ("http_status", "string"),
    ("error_message", "string"),
    ("watermark_before", "string"),
    ("watermark_after", "string"),
    ("function_instance_id", "string"),
    ("rows_in_batch", "int"),
    ("payload_family", "string"),
    ("factory", "string"),
]


CHEMICAL_TRACEABILITY_SUCCESS_TABLE_COLUMNS = [
    ("plc_id", "string"),
    ("forward_target", "string"),
    ("ack_time", "datetime"),
    ("batch_id", "string"),
    ("watermark_before", "string"),
    ("watermark_after", "string"),
    ("function_instance_id", "string"),
    ("source_topic", "string"),
    ("workcenter", "string"),
    ("report_post", "string"),
]


CHEMICAL_TRACEABILITY_AUDIT_TABLE_COLUMNS = [
    ("plc_id", "string"),
    ("forward_target", "string"),
    ("attempt_time", "datetime"),
    ("batch_id", "string"),
    ("status", "string"),
    ("http_status", "string"),
    ("error_message", "string"),
    ("watermark_before", "string"),
    ("watermark_after", "string"),
    ("function_instance_id", "string"),
    ("rows_in_batch", "int"),
    ("source_topic", "string"),
    ("workcenter", "string"),
    ("report_post", "string"),
]

def _get_platform_adx_cluster_url() -> str:
    return _get_required_setting_alias("ADX_CLUSTER_URL")


def _get_platform_adx_database() -> str:
    return _get_required_setting_alias("ADX_DATABASE")


def _get_platform_adx_scope() -> str:
    return _get_setting(
        "ADX_TOKEN_SCOPE",
        default="https://kusto.kusto.chinacloudapi.cn/.default",
    )


@app.function_name(name="function_traceability_defect_10m")
@app.timer_trigger(
    schedule=_get_setting("TRACEABILITY_DEFECT_SCHEDULE", default="0 */10 * * * *"),
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def function_traceability_defect_10m(mytimer: func.TimerRequest) -> None:
    _ = mytimer
    rows: list[dict] = []
    acknowledged_rows: list[dict] = []
    watermark = ""
    forward_target = ""
    batch_id = str(uuid4())
    function_instance_id = os.getenv("WEBSITE_INSTANCE_ID", os.getenv("HOSTNAME", "local"))
    try:
        cluster_url = _get_platform_adx_cluster_url()
        database = _get_platform_adx_database()
        scope = _get_platform_adx_scope()
        overlap_minutes = _get_int_setting(120, "TRACEABILITY_DEFECT_OVERLAP_MINUTES", minimum=0)
        forward_target = _get_setting("TRACEABILITY_DEFECT_FORWARD_TARGET", default="baiteda_uat")
        success_table = _get_setting("TRACEABILITY_DEFECT_SUCCESS_TABLE", default="defect_forward_success")
        audit_table = _get_setting("TRACEABILITY_DEFECT_AUDIT_TABLE", default="defect_forward_audit")
        initial_watermark = _get_setting("TRACEABILITY_DEFECT_INITIAL_WATERMARK", default="1970-01-01T00:00:00Z")
        dry_run = _get_bool_setting(False, "TRACEABILITY_DEFECT_DRY_RUN")
        write_audit = _get_bool_setting(True, "TRACEABILITY_DEFECT_WRITE_AUDIT")
        credential = _get_credential()

        state_store = WatermarkStore(
            account_url_setting="STATE_STORAGE_ACCOUNT_URL",
            container_name_setting=("TRACEABILITY_DEFECT_WATERMARK_CONTAINER_NAME", "WATERMARK_CONTAINER_NAME"),
            blob_name_setting=("TRACEABILITY_DEFECT_WATERMARK_BLOB_NAME",),
            default_container_name="adx-watermark",
            default_blob_name="defect-global.json",
        )
        state = state_store.load(initial_watermark)
        watermark = state["last_watermark"]
        success_lookback_hours = _compute_defect_success_lookback_hours(watermark, overlap_minutes)
        query = _build_query(watermark, overlap_minutes, success_lookback_hours, forward_target)

        logging.info(
            "Starting ADX defect forwarder from watermark utc=%s local=%s with success lookback %sh",
            watermark,
            _format_local_display(watermark),
            success_lookback_hours,
        )
        rows = _query_adx(cluster_url=cluster_url, database=database, query=query, scope=scope, credential=credential)
        logging.info("ADX returned %s rows", len(rows))

        if not rows:
            logging.info("No rows to forward in current timer execution.")
            return

        _validate_columns(rows)
        rows = _normalize_rows(rows)

        if dry_run:
            logging.info(
                "Dry run enabled. Queried rows: %s",
                json.dumps(rows[:5], ensure_ascii=False, default=_json_log_default),
            )
            return

        def _persist_defect_success_batch(batch_rows: list[dict]) -> None:
            nonlocal watermark
            batch_watermark = _max_watermark(batch_rows)
            _append_table_rows(
                cluster_url=cluster_url,
                database=database,
                table_name=success_table,
                columns=SUCCESS_TABLE_COLUMNS,
                rows=_build_success_rows(
                    batch_rows,
                    forward_target=forward_target,
                    batch_id=batch_id,
                    watermark_before=watermark,
                    watermark_after=batch_watermark,
                    function_instance_id=function_instance_id,
                ),
                scope=scope,
                credential=credential,
            )
            state_store.save(batch_watermark)
            if write_audit:
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=AUDIT_TABLE_COLUMNS,
                    rows=_build_audit_rows(
                        batch_rows,
                        forward_target=forward_target,
                        batch_id=batch_id,
                        status="success",
                        watermark_before=watermark,
                        watermark_after=batch_watermark,
                        function_instance_id=function_instance_id,
                    ),
                    scope=scope,
                    credential=credential,
                )
            watermark = batch_watermark

        acknowledged_rows = BaitedaSync().sync_defects(rows, on_batch_success=_persist_defect_success_batch)
        logging.info("Baiteda sync acknowledged %s rows", len(acknowledged_rows))

        if not acknowledged_rows:
            raise RuntimeError("No records were acknowledged as synced by BaitedaSync. Watermark will not advance.")
        logging.info(
            "Forward completed. Watermark moved to utc=%s local=%s",
            watermark,
            _format_local_display(watermark),
        )
    except BaitedaSyncPartialFailure as exc:
        try:
            cluster_url = _get_platform_adx_cluster_url()
            database = _get_platform_adx_database()
            scope = _get_platform_adx_scope()
            audit_table = _get_setting("TRACEABILITY_DEFECT_AUDIT_TABLE", default="defect_forward_audit")
            write_audit = _get_bool_setting(True, "TRACEABILITY_DEFECT_WRITE_AUDIT")
            credential = _get_credential()

            if exc.failed_rows and write_audit:
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=AUDIT_TABLE_COLUMNS,
                    rows=_build_audit_rows(
                        exc.failed_rows,
                        forward_target=forward_target or _get_setting("TRACEABILITY_DEFECT_FORWARD_TARGET", default="baiteda_uat"),
                        batch_id=batch_id,
                        status="failed",
                        watermark_before=watermark,
                        watermark_after="",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                        http_status=exc.http_status,
                    ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as partial_exc:
            logging.warning("Failed to persist partial defect forward state: %s", partial_exc)
        logging.exception(
            "ADX defect forward timer failed after partial success. Watermark remains at utc=%s local=%s: %s",
            watermark,
            _format_local_display(watermark),
            exc,
        )
        raise
    except Exception as exc:
        try:
            if rows and _get_bool_setting(True, "TRACEABILITY_DEFECT_WRITE_AUDIT"):
                cluster_url = _get_platform_adx_cluster_url()
                database = _get_platform_adx_database()
                scope = _get_platform_adx_scope()
                audit_table = _get_setting("TRACEABILITY_DEFECT_AUDIT_TABLE", default="defect_forward_audit")
                credential = _get_credential()
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=AUDIT_TABLE_COLUMNS,
                    rows=_build_audit_rows(
                        rows,
                        forward_target=forward_target or _get_setting("TRACEABILITY_DEFECT_FORWARD_TARGET", default="baiteda_uat"),
                        batch_id=batch_id,
                        status="failed",
                        watermark_before=watermark,
                        watermark_after="",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                    ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as audit_exc:
            logging.warning("Failed to append ADX forward audit rows: %s", audit_exc)
        logging.exception("ADX defect forward timer failed: %s", exc)
        raise


@app.function_name(name="function_glass_master_10m")
@app.timer_trigger(
    schedule=_get_setting("GLASS_MASTER_SCHEDULE", default="0 */10 * * * *"),
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def function_glass_master_10m(mytimer: func.TimerRequest) -> None:
    _ = mytimer
    if not GLASS_MASTER_FORWARD_LOCK.acquire(blocking=False):
        logging.warning("Glass master forward timer is already running, skipping current execution.")
        return

    rows: list[dict] = []
    acknowledged_rows: list[dict] = []
    watermark = ""
    forward_target = ""
    batch_id = str(uuid4())
    function_instance_id = os.getenv("WEBSITE_INSTANCE_ID", os.getenv("HOSTNAME", "local"))
    try:
        cluster_url = _get_platform_adx_cluster_url()
        database = _get_platform_adx_database()
        scope = _get_platform_adx_scope()
        overlap_minutes = _get_int_setting(120, "GLASS_MASTER_OVERLAP_MINUTES", minimum=0)
        forward_target = _get_setting("GLASS_MASTER_FORWARD_TARGET", default="glass_master_uat")
        success_table = _get_setting("GLASS_MASTER_SUCCESS_TABLE", default="glass_master_forward_success")
        audit_table = _get_setting("GLASS_MASTER_AUDIT_TABLE", default="glass_master_forward_audit")
        initial_watermark = _get_setting("GLASS_MASTER_INITIAL_WATERMARK", default="1970-01-01T00:00:00Z")
        dry_run = _get_bool_setting(False, "GLASS_MASTER_DRY_RUN")
        write_audit = _get_bool_setting(True, "GLASS_MASTER_WRITE_AUDIT")
        credential = _get_credential()

        state_store = WatermarkStore(
            account_url_setting="STATE_STORAGE_ACCOUNT_URL",
            container_name_setting=("GLASS_MASTER_WATERMARK_CONTAINER_NAME",),
            blob_name_setting=("GLASS_MASTER_WATERMARK_BLOB_NAME",),
            default_container_name=_get_setting("WATERMARK_CONTAINER_NAME", default="adx-watermark"),
            default_blob_name="glass-master-global.json",
        )
        state = state_store.load(initial_watermark)
        watermark = state["last_watermark"]
        success_lookback_hours = _compute_glass_master_success_lookback_hours(watermark, overlap_minutes)
        query = _build_glass_master_forward_query(watermark, overlap_minutes, success_lookback_hours, forward_target)

        logging.info(
            "Starting glass master forwarder from watermark utc=%s local=%s with success lookback %sh",
            watermark,
            _format_local_display(watermark),
            success_lookback_hours,
        )
        rows = _query_adx(cluster_url=cluster_url, database=database, query=query, scope=scope, credential=credential)
        logging.info("ADX returned %s glass master rows", len(rows))

        if not rows:
            logging.info("No glass master rows to forward in current timer execution.")
            return

        _validate_glass_master_columns(rows)
        rows = _normalize_rows(rows)

        if dry_run:
            logging.info(
                "Glass master dry run enabled. Queried rows: %s",
                json.dumps(rows[:5], ensure_ascii=False, default=_json_log_default),
            )
            return

        def _persist_glass_master_success_batch(batch_rows: list[dict]) -> None:
            nonlocal watermark
            batch_watermark = _max_watermark(batch_rows)
            _append_table_rows(
                cluster_url=cluster_url,
                database=database,
                table_name=success_table,
                columns=GLASS_MASTER_SUCCESS_TABLE_COLUMNS,
                rows=_build_glass_master_success_rows(
                    batch_rows,
                    forward_target=forward_target,
                    batch_id=batch_id,
                    watermark_before=watermark,
                    watermark_after=batch_watermark,
                    function_instance_id=function_instance_id,
                ),
                scope=scope,
                credential=credential,
            )
            state_store.save(batch_watermark)
            if write_audit:
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=GLASS_MASTER_AUDIT_TABLE_COLUMNS,
                    rows=_build_glass_master_audit_rows(
                        batch_rows,
                        forward_target=forward_target,
                        batch_id=batch_id,
                        status="success",
                        watermark_before=watermark,
                        watermark_after=batch_watermark,
                        function_instance_id=function_instance_id,
                    ),
                    scope=scope,
                    credential=credential,
                )
            watermark = batch_watermark

        acknowledged_rows = GlassMasterSync().sync_rows(rows, on_batch_success=_persist_glass_master_success_batch)
        logging.info("Glass master sync acknowledged %s rows", len(acknowledged_rows))

        if not acknowledged_rows:
            raise RuntimeError("No records were acknowledged as synced by GlassMasterSync. Watermark will not advance.")
        logging.info(
            "Glass master forward completed. Watermark moved to utc=%s local=%s",
            watermark,
            _format_local_display(watermark),
        )
    except GlassMasterSyncPartialFailure as exc:
        try:
            cluster_url = _get_platform_adx_cluster_url()
            database = _get_platform_adx_database()
            scope = _get_platform_adx_scope()
            audit_table = _get_setting("GLASS_MASTER_AUDIT_TABLE", default="glass_master_forward_audit")
            write_audit = _get_bool_setting(True, "GLASS_MASTER_WRITE_AUDIT")
            credential = _get_credential()

            if exc.failed_rows and write_audit:
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=GLASS_MASTER_AUDIT_TABLE_COLUMNS,
                    rows=_build_glass_master_audit_rows(
                        exc.failed_rows,
                        forward_target=forward_target or _get_setting("GLASS_MASTER_FORWARD_TARGET", default="glass_master_uat"),
                        batch_id=batch_id,
                        status="failed",
                        watermark_before=watermark,
                        watermark_after="",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                        http_status=exc.http_status,
                    ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as partial_exc:
            logging.warning("Failed to persist partial glass master forward state: %s", partial_exc)
        logging.exception(
            "ADX glass master forward timer failed after partial success. Watermark remains at utc=%s local=%s: %s",
            watermark,
            _format_local_display(watermark),
            exc,
        )
        raise
    except Exception as exc:
        try:
            if rows and _get_bool_setting(True, "GLASS_MASTER_WRITE_AUDIT"):
                cluster_url = _get_platform_adx_cluster_url()
                database = _get_platform_adx_database()
                scope = _get_platform_adx_scope()
                audit_table = _get_setting("GLASS_MASTER_AUDIT_TABLE", default="glass_master_forward_audit")
                credential = _get_credential()
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=GLASS_MASTER_AUDIT_TABLE_COLUMNS,
                    rows=_build_glass_master_audit_rows(
                        rows,
                        forward_target=forward_target or _get_setting("GLASS_MASTER_FORWARD_TARGET", default="glass_master_uat"),
                        batch_id=batch_id,
                        status="failed",
                        watermark_before=watermark,
                        watermark_after="",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                    ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as audit_exc:
            logging.warning("Failed to append glass master forward audit rows: %s", audit_exc)
        logging.exception("ADX glass master forward timer failed: %s", exc)
        raise
    finally:
        GLASS_MASTER_FORWARD_LOCK.release()


@app.function_name(name="function_chemical_traceability_5m")
@app.timer_trigger(
    schedule=_get_setting("CHEMICAL_TRACEABILITY_SCHEDULE", default="0 */5 * * * *"),
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def function_chemical_traceability_5m(mytimer: func.TimerRequest) -> None:
    _ = mytimer
    if not CHEMICAL_TRACEABILITY_FORWARD_LOCK.acquire(blocking=False):
        logging.warning("Chemical traceability forward timer is already running, skipping current execution.")
        return

    rows: list[dict] = []
    acknowledged_rows: list[dict] = []
    watermark = ""
    forward_target = ""
    batch_id = str(uuid4())
    function_instance_id = os.getenv("WEBSITE_INSTANCE_ID", os.getenv("HOSTNAME", "local"))
    try:
        cluster_url = _get_platform_adx_cluster_url()
        database = _get_platform_adx_database()
        scope = _get_platform_adx_scope()
        overlap_minutes = _get_int_setting(120, "CHEMICAL_TRACEABILITY_OVERLAP_MINUTES", minimum=0)
        forward_target = _get_setting("CHEMICAL_TRACEABILITY_FORWARD_TARGET", default="chemical_traceability_uat")
        success_table = "chemical_traceability_forward_success"
        audit_table = "chemical_traceability_forward_audit"
        initial_watermark = _get_setting("CHEMICAL_TRACEABILITY_INITIAL_WATERMARK", default="1970-01-01T00:00:00Z")
        dry_run = _get_bool_setting(False, "CHEMICAL_TRACEABILITY_DRY_RUN")
        write_audit = True
        credential = _get_credential()

        state_store = WatermarkStore(
            account_url_setting="STATE_STORAGE_ACCOUNT_URL",
            container_name_setting=("CHEMICAL_TRACEABILITY_WATERMARK_CONTAINER_NAME", "WATERMARK_CONTAINER_NAME"),
            blob_name_setting=("CHEMICAL_TRACEABILITY_WATERMARK_BLOB_NAME",),
            default_container_name=_get_setting("WATERMARK_CONTAINER_NAME", default="adx-watermark"),
            default_blob_name="chemical-traceability-global.json",
        )
        state = state_store.load(initial_watermark)
        watermark = state["last_watermark"]
        success_lookback_hours = _compute_chemical_traceability_success_lookback_hours(watermark, overlap_minutes)
        query = _build_chemical_traceability_forward_query(watermark, overlap_minutes, success_lookback_hours, forward_target)

        logging.info(
            "Starting chemical traceability forwarder from watermark utc=%s local=%s with success lookback %sh",
            watermark,
            _format_local_display(watermark),
            success_lookback_hours,
        )
        rows = _query_adx(cluster_url=cluster_url, database=database, query=query, scope=scope, credential=credential)
        logging.info("ADX returned %s chemical traceability rows", len(rows))

        if not rows:
            logging.info("No chemical traceability rows to forward in current timer execution.")
            return

        _validate_chemical_traceability_columns(rows)
        rows = _normalize_rows(rows)

        if dry_run:
            logging.info(
                "Chemical traceability dry run enabled. Queried rows: %s",
                json.dumps(rows[:5], ensure_ascii=False, default=_json_log_default),
            )
            return

        def _persist_chemical_traceability_success_batch(batch_rows: list[dict]) -> None:
            nonlocal watermark
            batch_watermark = _max_watermark(batch_rows)
            _append_table_rows(
                cluster_url=cluster_url,
                database=database,
                table_name=success_table,
                columns=CHEMICAL_TRACEABILITY_SUCCESS_TABLE_COLUMNS,
                rows=_build_chemical_traceability_success_rows(
                    batch_rows,
                    forward_target=forward_target,
                    batch_id=batch_id,
                    watermark_before=watermark,
                    watermark_after=batch_watermark,
                    function_instance_id=function_instance_id,
                ),
                scope=scope,
                credential=credential,
            )
            state_store.save(batch_watermark)
            if write_audit:
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=CHEMICAL_TRACEABILITY_AUDIT_TABLE_COLUMNS,
                    rows=_build_chemical_traceability_audit_rows(
                        batch_rows,
                        forward_target=forward_target,
                        batch_id=batch_id,
                        status="success",
                        watermark_before=watermark,
                        watermark_after=batch_watermark,
                        function_instance_id=function_instance_id,
                    ),
                    scope=scope,
                    credential=credential,
                )
            watermark = batch_watermark

        acknowledged_rows = ChemicalTraceabilitySync().sync_rows(
            rows,
            on_batch_success=_persist_chemical_traceability_success_batch,
        )
        logging.info("Chemical traceability sync acknowledged %s rows", len(acknowledged_rows))

        if not acknowledged_rows:
            raise RuntimeError("No records were acknowledged as synced by ChemicalTraceabilitySync. Watermark will not advance.")
        logging.info(
            "Chemical traceability forward completed. Watermark moved to utc=%s local=%s",
            watermark,
            _format_local_display(watermark),
        )
    except ChemicalTraceabilitySyncPartialFailure as exc:
        try:
            cluster_url = _get_platform_adx_cluster_url()
            database = _get_platform_adx_database()
            scope = _get_platform_adx_scope()
            credential = _get_credential()
            if exc.failed_rows and write_audit:
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name="chemical_traceability_forward_audit",
                    columns=CHEMICAL_TRACEABILITY_AUDIT_TABLE_COLUMNS,
                    rows=_build_chemical_traceability_audit_rows(
                        exc.failed_rows,
                        forward_target=forward_target or "chemical_traceability_uat",
                        batch_id=batch_id,
                        status="failed",
                        watermark_before=watermark,
                        watermark_after="",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                        http_status=exc.http_status,
                    ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as partial_exc:
            logging.warning("Failed to persist partial chemical traceability forward state: %s", partial_exc)
        logging.exception(
            "ADX chemical traceability forward timer failed after partial success. Watermark remains at utc=%s local=%s: %s",
            watermark,
            _format_local_display(watermark),
            exc,
        )
        raise
    except Exception as exc:
        try:
            if rows:
                cluster_url = _get_platform_adx_cluster_url()
                database = _get_platform_adx_database()
                scope = _get_platform_adx_scope()
                credential = _get_credential()
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name="chemical_traceability_forward_audit",
                    columns=CHEMICAL_TRACEABILITY_AUDIT_TABLE_COLUMNS,
                    rows=_build_chemical_traceability_audit_rows(
                        rows,
                        forward_target=forward_target or "chemical_traceability_uat",
                        batch_id=batch_id,
                        status="failed",
                        watermark_before=watermark,
                        watermark_after="",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                    ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as audit_exc:
            logging.warning("Failed to append chemical traceability audit rows: %s", audit_exc)
        logging.exception("ADX chemical traceability forward timer failed: %s", exc)
        raise
    finally:
        CHEMICAL_TRACEABILITY_FORWARD_LOCK.release()


@app.function_name(name="function_production_output_10m")
@app.timer_trigger(
    schedule=_get_setting("PRODUCTION_OUTPUT_SCHEDULE", default="0 */10 * * * *"),
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def function_production_output_10m(mytimer: func.TimerRequest) -> None:
    _ = mytimer
    # 同一时刻只允许一个产量推送任务运行，避免重复取数和重复写台账。
    if not PROD_FORWARD_LOCK.acquire(blocking=False):
        logging.warning("Prod output forward timer is already running, skipping current execution.")
        return

    rows: list[dict] = []
    forward_target = ""
    batch_id = str(uuid4())
    function_instance_id = os.getenv("WEBSITE_INSTANCE_ID", os.getenv("HOSTNAME", "local"))
    prod_watermark = ""
    prod_state_store = None
    try:
        # 先解析运行参数：ADX 连接、取数窗口、目标环境、台账表开关等都在这里收口。
        cluster_url = _get_platform_adx_cluster_url()
        database = _get_platform_adx_database()
        scope = _get_platform_adx_scope()
        lookback_hours = _get_int_setting(24, "PRODUCTION_OUTPUT_LOOKBACK_HOURS", minimum=0)
        overlap_minutes = _get_int_setting(60, "PRODUCTION_OUTPUT_OVERLAP_MINUTES", minimum=0)
        forward_target = _get_setting("PRODUCTION_OUTPUT_FORWARD_TARGET", default="tellus_uat")
        success_table = _get_setting("PRODUCTION_OUTPUT_SUCCESS_TABLE", default="prod_tellus_forward_success")
        audit_table = _get_setting("PRODUCTION_OUTPUT_AUDIT_TABLE", default="prod_tellus_forward_audit")
        write_audit = _get_bool_setting(True, "PRODUCTION_OUTPUT_WRITE_AUDIT")
        dry_run = _get_bool_setting(False, "PRODUCTION_OUTPUT_DRY_RUN")
        credential = _get_credential()
        # watermark blob 记录上次已经成功推送到接口的业务时间，用来做增量发送。
        prod_state_store = WatermarkStore(
            account_url_setting="STATE_STORAGE_ACCOUNT_URL",
            container_name_setting=("PRODUCTION_OUTPUT_WATERMARK_CONTAINER_NAME",),
            blob_name_setting=("PRODUCTION_OUTPUT_WATERMARK_BLOB_NAME",),
            default_container_name=_get_setting("WATERMARK_CONTAINER_NAME", default="adx-watermark"),
            default_blob_name="prod-output-global.json",
        )
        # 第一次运行没有状态时，会按 lookback_hours 回退一个默认起点。
        prod_state = prod_state_store.load(_compute_prod_initial_watermark(lookback_hours))
        prod_watermark = prod_state["last_watermark"]
        # success lookback 用来补查 success 台账，避免重叠窗口下重复推送同一分钟产量。
        success_lookback_hours = _compute_prod_success_lookback_hours(prod_watermark, overlap_minutes)
        # 产量分钟表不是实时明细表，这里会先从 watermark 往前回退一段时间重算分钟结果。
        prod_rebuild_start_utc = _compute_prod_rebuild_start_utc(prod_watermark, overlap_minutes)

        # 先重建 prod_output_minute，再从这张分钟结果表里筛待推送候选。
        _run_prod_rebuild_command(
            cluster_url=cluster_url,
            database=database,
            scope=scope,
            credential=credential,
            command=(
                ".set-or-replace prod_output_minute <| "
                f"fn_prod_counter_minute_rebuild_v1(datetime('{_escape_kusto_string(_format_kusto_datetime(prod_rebuild_start_utc))}'), now())"
            ),
        )

        query = _build_prod_forward_query(
            watermark=prod_watermark,
            overlap_minutes=overlap_minutes,
            forward_target=forward_target,
            success_lookback_hours=success_lookback_hours,
        )
        logging.info(
            "Starting prod Tellus forwarder from watermark %s with overlap %sm and success lookback %sh",
            prod_watermark,
            overlap_minutes,
            success_lookback_hours,
        )
        # 候选函数会基于 prod_output_minute、forward_target 和 success 台账做去重筛选。
        rows = _query_adx(cluster_url=cluster_url, database=database, query=query, scope=scope, credential=credential)
        rows = _normalize_prod_rows(rows)
        logging.info("Prod Tellus forward query returned %s rows", len(rows))

        if not rows:
            logging.info("No prod output rows to forward in current timer execution.")
            return

        if dry_run:
            logging.info("Prod dry run enabled. Queried rows: %s", json.dumps(rows[:5], ensure_ascii=False, default=str))
            return

        def _persist_prod_success_batch(batch_rows: list[dict]) -> None:
            nonlocal prod_watermark
            # 一批发送成功后，先把成功台账和审计台账写回 ADX，再推进 watermark。
            # 这样即使函数中途失败，也能保证“已成功发送的数据”和“当前水位”一致。
            batch_watermark = _max_prod_watermark(batch_rows)
            _append_table_rows(
                cluster_url=cluster_url,
                database=database,
                table_name=success_table,
                columns=PROD_SUCCESS_TABLE_COLUMNS,
                rows=_build_prod_success_rows(
                    batch_rows,
                    forward_target=forward_target,
                    batch_id=batch_id,
                    function_instance_id=function_instance_id,
                ),
                scope=scope,
                credential=credential,
            )
            prod_state_store.save(batch_watermark)
            if write_audit:
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=PROD_AUDIT_TABLE_COLUMNS,
                    rows=_build_prod_audit_rows(
                        batch_rows,
                        forward_target=forward_target,
                        batch_id=batch_id,
                        status="success",
                        function_instance_id=function_instance_id,
                    ),
                    scope=scope,
                    credential=credential,
                )
            prod_watermark = batch_watermark

        # ProdTellusSync 只负责组接口报文并调用客户接口；
        # 发送成功后的台账落库和 watermark 推进由上面的回调统一处理。
        acknowledged_rows = ProdTellusSync().sync_minutes(rows, on_batch_success=_persist_prod_success_batch)
        if not acknowledged_rows:
            raise RuntimeError("No prod rows were acknowledged as synced by Tellus. Success ledger will not be written.")
        logging.info("Prod Tellus sync finished, sent rows=%s, watermark moved to %s", len(acknowledged_rows), prod_watermark)
    except ProdTellusSyncPartialFailure as exc:
        try:
            cluster_url = _get_platform_adx_cluster_url()
            database = _get_platform_adx_database()
            scope = _get_platform_adx_scope()
            credential = _get_credential()
            audit_table = _get_setting("PRODUCTION_OUTPUT_AUDIT_TABLE", default="prod_tellus_forward_audit")
            write_audit = _get_bool_setting(True, "PRODUCTION_OUTPUT_WRITE_AUDIT")
            if exc.failed_rows and write_audit:
                  # 部分批次失败时，只给未成功部分补失败审计，不回退已成功批次的 success 台账。
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=PROD_AUDIT_TABLE_COLUMNS,
                      rows=_build_prod_audit_rows(
                        exc.failed_rows,
                        forward_target=forward_target or _get_setting("PRODUCTION_OUTPUT_FORWARD_TARGET", default="tellus_uat"),
                        batch_id=batch_id,
                        status="failed",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                        http_status=exc.http_status,
                      ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as audit_exc:
            logging.warning("Failed to append prod forward ledger rows after partial failure: %s", audit_exc)
        logging.exception("ADX prod output forward timer failed after partial Tellus send: %s", exc)
        raise
    except Exception as exc:
        try:
            if rows and _get_bool_setting(True, "PRODUCTION_OUTPUT_WRITE_AUDIT"):
                cluster_url = _get_platform_adx_cluster_url()
                database = _get_platform_adx_database()
                scope = _get_platform_adx_scope()
                audit_table = _get_setting("PRODUCTION_OUTPUT_AUDIT_TABLE", default="prod_tellus_forward_audit")
                credential = _get_credential()
                # 整批失败时，把本次查到但未成功发送的候选统一落失败审计，方便后续补数和排查。
                _append_table_rows(
                    cluster_url=cluster_url,
                    database=database,
                    table_name=audit_table,
                    columns=PROD_AUDIT_TABLE_COLUMNS,
                    rows=_build_prod_audit_rows(
                        rows,
                        forward_target=forward_target or _get_setting("PRODUCTION_OUTPUT_FORWARD_TARGET", default="tellus_uat"),
                        batch_id=batch_id,
                        status="failed",
                        function_instance_id=function_instance_id,
                        error_message=str(exc),
                    ),
                    scope=scope,
                    credential=credential,
                )
        except Exception as audit_exc:
            logging.warning("Failed to append prod forward audit rows: %s", audit_exc)
        logging.exception("ADX prod output forward timer failed: %s", exc)
        raise
    finally:
        PROD_FORWARD_LOCK.release()


@app.function_name(name="function_production_rollup_2h")
@app.timer_trigger(
    schedule=_get_setting("PRODUCTION_ROLLUP_SCHEDULE", default="30 0 */2 * * *"),
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def function_production_rollup_2h(mytimer: func.TimerRequest) -> None:
    _ = mytimer
    # 汇总任务和推送任务分开调度：这里专门负责把分钟结果再汇总成时/天/月表。
    if not PROD_ROLLUP_LOCK.acquire(blocking=False):
        logging.warning("Prod output rollup timer is already running, skipping current execution.")
        return

    try:
        cluster_url = _get_platform_adx_cluster_url()
        database = _get_platform_adx_database()
        scope = _get_platform_adx_scope()
        credential = _get_credential()

        # 这三个 set-or-replace 会按当前分钟表重建正式汇总表，不直接调用对外接口。
        _run_prod_rebuild_command(
            cluster_url=cluster_url,
            database=database,
            scope=scope,
            credential=credential,
            command=".set-or-replace prod_output_hour <| fn_prod_counter_hour_from_minute_v1(datetime_local_to_utc(startofday(datetime_utc_to_local(now(), 'Asia/Shanghai')), 'Asia/Shanghai'), now())",
        )
        _run_prod_rebuild_command(
            cluster_url=cluster_url,
            database=database,
            scope=scope,
            credential=credential,
            command=".set-or-replace prod_output_day <| fn_prod_counter_day_from_hour_v1(datetime_local_to_utc(startofday(datetime_utc_to_local(now(), 'Asia/Shanghai')), 'Asia/Shanghai'), now())",
        )
        _run_prod_rebuild_command(
            cluster_url=cluster_url,
            database=database,
            scope=scope,
            credential=credential,
            command=".set-or-replace prod_output_month <| fn_prod_counter_month_from_day_v1(datetime_local_to_utc(startofmonth(datetime_utc_to_local(now(), 'Asia/Shanghai')), 'Asia/Shanghai'), now())",
        )
        logging.info("Prod output rollup timer completed successfully.")
    except Exception as exc:
        logging.exception("ADX prod output rollup timer failed: %s", exc)
        raise
    finally:
        PROD_ROLLUP_LOCK.release()


@app.function_name(name="function_print_adx_export_1d")
@app.timer_trigger(
    schedule=_get_setting("PRINT_EXPORT_SCHEDULE", "EXPORT_SCHEDULE", default="0 0 1 * * *"),
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def function_print_adx_export_1d(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.warning("Print ADX export timer is running later than scheduled.")
    manifest = export_print_batch_to_blob()
    logging.info(
        "Print ADX export completed, batch_id=%s rows=%s files=%s",
        manifest["batch_id"],
        manifest["adx_export_row_count"],
        manifest["file_count"],
    )


@app.function_name(name="function_print_snowflake_loader_blob")
@app.blob_trigger(
    arg_name="manifest_blob",
    path="%SNOWFLAKE_EXPORT_CONTAINER%/%SNOWFLAKE_EXPORT_PREFIX%/{pipeline}/{year}/{month}/{day}/{batch_id}/manifest.json",
    connection="AzureWebJobsStorage",
)
def function_print_snowflake_loader_blob(manifest_blob: func.InputStream) -> None:
    logging.info("Triggered by print export manifest blob: %s", manifest_blob.name)
    load_print_batch_from_manifest(manifest_blob.read(), manifest_blob.name)


@app.function_name(name="function_silk_screen_status_eventhub")
@app.event_hub_message_trigger(
    arg_name="event",
    event_hub_name="%SILK_SCREEN_EVENTHUB_NAME%",
    connection="SILK_SCREEN_EVENTHUB_CONNECTION",
)
def function_silk_screen_status_eventhub(event: func.EventHubEvent) -> None:
    syncer = SilkScreenSync()
    row = parse_silk_screen_event_message(event.get_body())
    topic = str(row.get("topic") or "").strip()
    if not topic:
        raise ValueError("Missing topic in Event Hub message")
    if topic not in syncer.allowed_topics:
        logging.info(
            "Silk screen Event Hub message skipped, topic=%s message_id=%s",
            topic,
            row.get("message_id"),
        )
        return
    syncer.sync_row(row)
    logging.info(
        "Silk screen Event Hub message processed, topic=%s message_id=%s",
        topic,
        row.get("message_id"),
    )
