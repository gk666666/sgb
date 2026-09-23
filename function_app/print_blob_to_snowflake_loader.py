import json
import logging
import re
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import snowflake.connector
from azure.storage.blob import BlobServiceClient
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from runtime_settings import get_bool_setting, get_int_setting, get_required_setting


BUSINESS_COLUMNS = [
    "read_time",
    "topic",
    "workcenter",
    "equipment",
    "status",
    "dmc_code",
    "rfid_code",
    "print_cnt",
    "print_spd",
    "print_stroke",
    "ink_ret_spd",
    "ink_ret_stroke",
    "off_contact_hgt",
    "off_contact_ratio",
    "silk_screen_life",
    "silk_screen_status",
    "ir1_maxtemp",
    "ir1_mintemp",
    "ir1_curtemp",
    "ir2_maxtemp",
    "ir2_mintemp",
    "ir2_curtemp",
    "dry1_speed",
]

TARGET_COLUMNS = BUSINESS_COLUMNS


def _validate_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", name):
        raise ValueError(f"Unsupported identifier: {name}")
    return name


def _get_blob_service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(get_required_setting("AzureWebJobsStorage"))


def _parse_manifest(manifest_bytes: bytes) -> dict:
    payload = json.loads(manifest_bytes.decode("utf-8"))
    required_keys = {
        "pipeline",
        "batch_id",
        "business_date_from",
        "business_date_to",
        "target_table",
        "source_path",
        "source_export_time",
        "file_count",
        "adx_export_row_count",
    }
    missing = sorted(required_keys.difference(payload.keys()))
    if missing:
        raise ValueError(f"Manifest missing required fields: {', '.join(missing)}")
    return payload


def _download_parquet_frames(container_name: str, source_path: str) -> list[tuple[str, pd.DataFrame]]:
    blob_service = _get_blob_service_client()
    container = blob_service.get_container_client(container_name)
    frames = []
    for blob in container.list_blobs(name_starts_with=source_path):
        if not blob.name.endswith(".parquet"):
            continue
        payload = container.get_blob_client(blob.name).download_blob().readall()
        frame = pd.read_parquet(BytesIO(payload), engine="pyarrow")
        frames.append((blob.name, frame))
    return frames


def _validate_parquet_file_count(manifest: dict, parquet_frames: list[tuple[str, pd.DataFrame]]) -> None:
    expected_count = int(manifest["file_count"])
    actual_count = len(parquet_frames)
    if actual_count != expected_count:
        raise ValueError(
            "Manifest parquet file count mismatch: "
            f"expected={expected_count}, actual={actual_count}, source_path={manifest['source_path']}"
        )


def _normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    for column in BUSINESS_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None
    normalized = normalized[BUSINESS_COLUMNS]
    normalized["read_time"] = pd.to_datetime(normalized["read_time"], utc=True, errors="coerce")
    normalized["read_time"] = normalized["read_time"].dt.tz_convert(None)
    return normalized


def _enrich_frames(parquet_frames: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    enriched_parts = []

    for _, frame in parquet_frames:
        part = _normalize_frame(frame)
        enriched_parts.append(part[TARGET_COLUMNS])

    if not enriched_parts:
        return pd.DataFrame(columns=TARGET_COLUMNS)
    merged = pd.concat(enriched_parts, ignore_index=True)
    return merged.where(pd.notnull(merged), None)


def _load_private_key_bytes() -> bytes:
    pem = get_required_setting("SNOWFLAKE_PRIVATE_KEY_PEM").encode("utf-8")
    passphrase = get_required_setting("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE").encode("utf-8")
    private_key = serialization.load_pem_private_key(
        pem,
        password=passphrase,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _get_snowflake_connection():
    return snowflake.connector.connect(
        account=get_required_setting("SNOWFLAKE_ACCOUNT"),
        user=get_required_setting("SNOWFLAKE_USER"),
        private_key=_load_private_key_bytes(),
        warehouse=get_required_setting("SNOWFLAKE_WAREHOUSE"),
        database=get_required_setting("SNOWFLAKE_DATABASE"),
        schema=get_required_setting("SNOWFLAKE_SCHEMA"),
        role=get_required_setting("SNOWFLAKE_ROLE"),
        autocommit=False,
    )


def _build_insert_sql(table_name: str) -> str:
    placeholders = ", ".join(["%s"] * len(TARGET_COLUMNS))
    columns = ", ".join(TARGET_COLUMNS)
    return f"insert into {table_name} ({columns}) values ({placeholders})"


def _convert_param_value(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def _iter_rows(frame: pd.DataFrame) -> list[tuple]:
    rows = []
    for row in frame.itertuples(index=False, name=None):
        rows.append(tuple(_convert_param_value(value) for value in row))
    return rows



def _batch_already_succeeded(cursor, audit_table: str, batch_id: str) -> bool:
    cursor.execute(
        f"select 1 from {audit_table} where batch_id = %s and load_status = 'success' limit 1",
    )
    return cursor.fetchone() is not None


def _insert_audit(cursor, audit_table: str, record: dict):
    cursor.execute(
        f"""
insert into {audit_table} (
    pipeline_name,
    batch_id,
    target_table,
    business_date_from,
    business_date_to,
    manifest_path,
    blob_path,
    file_count,
    adx_export_row_count,
    snowflake_inserted_row_count,
    load_status,
    error_message,
    job_run_id,
    started_at,
    finished_at
) values (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
)
""",
        (
            record["pipeline_name"],
            record["batch_id"],
            record["target_table"],
            record["business_date_from"],
            record["business_date_to"],
            record["manifest_path"],
            record["blob_path"],
            record["file_count"],
            record["adx_export_row_count"],
            record["snowflake_inserted_row_count"],
            record["load_status"],
            record["error_message"],
            record["job_run_id"],
            record["started_at"],
            record["finished_at"],
        ),
    )


def load_print_batch_from_manifest(manifest_bytes: bytes, manifest_blob_name: str):
    manifest = _parse_manifest(manifest_bytes)
    container_name = get_required_setting("SNOWFLAKE_EXPORT_CONTAINER")
    target_table = _validate_identifier(get_required_setting("SNOWFLAKE_TABLE"))
    write_audit = get_bool_setting("SNOWFLAKE_WRITE_AUDIT", default=False)
    audit_table = None
    if write_audit:
        audit_table = _validate_identifier(get_required_setting("SNOWFLAKE_AUDIT_TABLE"))
    insert_chunk_size = get_int_setting("SNOWFLAKE_INSERT_CHUNK_SIZE", default=1000, minimum=100)
    allow_empty_export = get_bool_setting("ALLOW_EMPTY_EXPORT", default=False)

    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    parquet_frames = _download_parquet_frames(container_name, manifest["source_path"])
    _validate_parquet_file_count(manifest, parquet_frames)
    if not parquet_frames and not (allow_empty_export and int(manifest["adx_export_row_count"]) == 0):
        raise ValueError(f"No parquet files found under {manifest['source_path']}")

    enriched = _enrich_frames(parquet_frames)
    inserted_rows = int(len(enriched))

    connection = None
    try:
        connection = _get_snowflake_connection()
        with connection.cursor() as cursor:
            if write_audit and audit_table and _batch_already_succeeded(cursor, audit_table, manifest["batch_id"]):
                logging.info("Snowflake batch already succeeded, skip batch_id=%s", manifest["batch_id"])
                connection.rollback()
                return

            cursor.execute(
                f"delete from {target_table} where to_date(read_time) >= %s and to_date(read_time) <= %s",
                (manifest["business_date_from"], manifest["business_date_to"]),
            )

            if inserted_rows:
                insert_sql = _build_insert_sql(target_table)
                rows = _iter_rows(enriched)
                for start_idx in range(0, len(rows), insert_chunk_size):
                    chunk = rows[start_idx:start_idx + insert_chunk_size]
                    cursor.executemany(insert_sql, chunk)

            finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
            if write_audit and audit_table:
                _insert_audit(
                    cursor,
                    audit_table,
                    {
                        "pipeline_name": manifest["pipeline"],
                        "batch_id": manifest["batch_id"],
                        "target_table": manifest["target_table"],
                        "business_date_from": manifest["business_date_from"],
                        "business_date_to": manifest["business_date_to"],
                        "manifest_path": manifest_blob_name,
                        "blob_path": manifest["source_path"],
                        "file_count": int(manifest["file_count"]),
                        "adx_export_row_count": int(manifest["adx_export_row_count"]),
                        "snowflake_inserted_row_count": inserted_rows,
                        "load_status": "success",
                        "error_message": None,
                        "job_run_id": None,
                        "started_at": started_at,
                        "finished_at": finished_at,
                    },
                )
            connection.commit()
            logging.info("Snowflake print load completed, batch_id=%s inserted_rows=%s", manifest["batch_id"], inserted_rows)
    except Exception as exc:
        if connection is not None:
            connection.rollback()
        error_message = str(exc)[:4000]
        logging.exception("Snowflake print load failed, batch_id=%s", manifest.get("batch_id"))
        try:
            if write_audit and audit_table and connection is None:
                connection = _get_snowflake_connection()
            if write_audit and audit_table and connection is not None:
                with connection.cursor() as cursor:
                    _insert_audit(
                        cursor,
                        audit_table,
                        {
                            "pipeline_name": manifest["pipeline"],
                            "batch_id": manifest["batch_id"],
                            "target_table": manifest["target_table"],
                            "business_date_from": manifest["business_date_from"],
                            "business_date_to": manifest["business_date_to"],
                            "manifest_path": manifest_blob_name,
                            "blob_path": manifest["source_path"],
                            "file_count": int(manifest["file_count"]),
                            "adx_export_row_count": int(manifest["adx_export_row_count"]),
                            "snowflake_inserted_row_count": 0,
                            "load_status": "failed",
                            "error_message": error_message,
                            "job_run_id": None,
                            "started_at": started_at,
                            "finished_at": datetime.now(timezone.utc).replace(tzinfo=None),
                        },
                    )
                    connection.commit()
        except Exception:
            logging.exception("Failed to write Snowflake failure audit, batch_id=%s", manifest.get("batch_id"))
        raise
    finally:
        if connection is not None:
            connection.close()
