create or replace table snowflake_load_audit (
    pipeline_name        string,
    batch_id             string,
    target_table         string,
    business_date_from   date,
    business_date_to     date,
    manifest_path        string,
    blob_path            string,
    file_count           number,
    adx_export_row_count number,
    snowflake_inserted_row_count number,
    load_status          string,
    error_message        string,
    job_run_id           string,
    started_at           timestamp_ntz,
    finished_at          timestamp_ntz
);
