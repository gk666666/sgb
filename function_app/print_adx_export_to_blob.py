import json
import logging
from datetime import datetime, timedelta, timezone
from io import BytesIO
from uuid import uuid4

import pandas as pd
import requests
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from runtime_settings import get_bool_setting, get_int_setting, get_required_setting


def _get_credential() -> DefaultAzureCredential:
    return DefaultAzureCredential()


def _get_token(credential: DefaultAzureCredential, scope: str) -> str:
    return credential.get_token(scope).token


def _query_adx_rows(cluster_url: str, database: str, query: str, scope: str) -> list[dict]:
    credential = _get_credential()
    token = _get_token(credential, scope)
    response = requests.post(
        f"{cluster_url.rstrip('/')}/v2/rest/query",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"db": database, "csl": query},
        timeout=120,
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
        if isinstance(table, dict) and table.get("TableKind") == "PrimaryResult":
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


def _build_print_export_query(start_utc: datetime, end_utc: datetime) -> str:
    start_text = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_text = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    return f"""
print_snapshot_std
| where read_time >= datetime({start_text})
| where read_time < datetime({end_text})
| order by read_time asc
""".strip()


def _calculate_window_bounds(window_days: int) -> tuple[datetime, datetime]:
    end_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = end_utc - timedelta(days=window_days)
    return start_utc, end_utc


def _normalize_dataframe(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    if "read_time" in frame.columns:
        frame["read_time"] = pd.to_datetime(frame["read_time"], utc=True, errors="coerce")
        frame["read_time"] = frame["read_time"].dt.tz_convert(None)
    return frame


def _get_blob_service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(get_required_setting("AzureWebJobsStorage"))


def _build_batch_root(prefix: str, pipeline_name: str, batch_id: str, export_time_utc: datetime) -> str:
    date_path = export_time_utc.strftime("%Y/%m/%d")
    prefix_text = prefix.strip("/")
    if prefix_text:
        return f"{prefix_text}/{pipeline_name}/{date_path}/batch_{batch_id}"
    return f"{pipeline_name}/{date_path}/batch_{batch_id}"


def export_print_batch_to_blob() -> dict:
    cluster_url = get_required_setting("ADX_CLUSTER_URL")
    database = get_required_setting("ADX_DATABASE")
    scope = get_required_setting("ADX_TOKEN_SCOPE")
    container_name = get_required_setting("SNOWFLAKE_EXPORT_CONTAINER")
    prefix = get_required_setting("SNOWFLAKE_EXPORT_PREFIX")
    pipeline_name = get_required_setting("EXPORT_PIPELINE_NAME")
    window_days = get_int_setting("EXPORT_QUERY_WINDOW_DAYS", default=2, minimum=1)
    max_rows_per_file = get_int_setting("EXPORT_PARQUET_MAX_ROWS_PER_FILE", default=50000, minimum=1000)
    allow_empty_export = get_bool_setting("ALLOW_EMPTY_EXPORT", default=False)

    start_utc, end_utc = _calculate_window_bounds(window_days)
    query = _build_print_export_query(start_utc, end_utc)
    rows = _query_adx_rows(cluster_url, database, query, scope)
    frame = _normalize_dataframe(rows)
    export_time_utc = datetime.now(timezone.utc)

    if frame.empty and not allow_empty_export:
        raise ValueError("ADX export query returned 0 rows and ALLOW_EMPTY_EXPORT is false.")

    batch_id = export_time_utc.strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]
    batch_root = _build_batch_root(prefix, pipeline_name, batch_id, export_time_utc)
    blob_service = _get_blob_service_client()
    container = blob_service.get_container_client(container_name)
    try:
        container.create_container()
    except Exception:
        pass

    file_count = 0
    if not frame.empty:
        for start_idx in range(0, len(frame), max_rows_per_file):
            chunk = frame.iloc[start_idx:start_idx + max_rows_per_file].copy()
            buffer = BytesIO()
            chunk.to_parquet(buffer, index=False, engine="pyarrow")
            buffer.seek(0)
            file_count += 1
            blob_name = f"{batch_root}/part-{file_count:04d}.parquet"
            container.upload_blob(name=blob_name, data=buffer.getvalue(), overwrite=True)
            logging.info("Uploaded print parquet blob: %s", blob_name)

    manifest = {
        "pipeline": pipeline_name,
        "batch_id": batch_id,
        "business_date_from": start_utc.date().isoformat(),
        "business_date_to": (end_utc - timedelta(days=1)).date().isoformat(),
        "target_table": get_required_setting("SNOWFLAKE_TABLE"),
        "file_format": "parquet",
        "source_path": f"{batch_root}/",
        "source_export_time": export_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "file_count": file_count,
        "adx_export_row_count": int(len(frame)),
    }
    manifest_name = f"{batch_root}/manifest.json"
    container.upload_blob(
        name=manifest_name,
        data=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        overwrite=True,
    )
    logging.info("Uploaded print export manifest: %s", manifest_name)
    return manifest
