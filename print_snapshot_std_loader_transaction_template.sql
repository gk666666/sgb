-- 说明：
-- 1) 这不是需要人工在 Snowflake 单独执行的 ETL 脚本；
-- 2) 它表示 Function 在运行时应执行的目标表事务逻辑；
-- 3) Loader 先在内存中读取当前批次 parquet，并补齐标准字段；
-- 4) 再在 Snowflake 中执行“删窗口旧数据 + 插入当前批次”。

begin;

delete from print_snapshot_std
where business_date >= to_date('${BUSINESS_DATE_FROM}')
  and business_date <= to_date('${BUSINESS_DATE_TO}');

-- Loader 将当前批次数据直接插入 print_snapshot_std。
-- 下面字段顺序需与目标表保持一致。
insert into print_snapshot_std (
    read_time,
    topic,
    workcenter,
    equipment,
    status,
    dmc_code,
    rfid_code,
    prod_cnt,
    print_cnt,
    print_spd,
    print_stroke,
    ink_ret_spd,
    ink_ret_stroke,
    off_contact_hgt,
    off_contact_ratio,
    silk_screen_life,
    screen_count,
    silk_screen_status,
    ir1_maxtemp,
    ir1_mintemp,
    ir1_curtemp,
    ir2_maxtemp,
    ir2_mintemp,
    ir2_curtemp,
    dry1_speed,
    flood_stroke,
    flood_spd,
    screen_height,
    source_system,
    source_table,
    source_file_name,
    source_file_path,
    batch_id,
    job_run_id,
    record_hash,
    business_date,
    source_export_time,
    snowflake_load_time
)
values
    (${ROW_VALUES_PLACEHOLDER});

commit;
