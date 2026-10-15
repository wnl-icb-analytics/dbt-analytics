-- SDL pipeline metadata: 1:1 view of raw_sdl_wnl_meta_file_registry. See docs/slam-data-guide.md
-- for how these objects drive layout resolution and loading. created_datetime and sent_datetime
-- are held in UTC at source and exposed as Europe/London timestamp_tz.

select
    feed,
    file_id,
    batch_id,
    file_name,
    original_file_name,
    {{ utc_to_london('created_datetime') }}     as created_datetime,
    {{ utc_to_london('sent_datetime') }}        as sent_datetime,
    row_count,
    loader_job_name,
    financial_year,
    financial_month,
    upper(trim(split_part(file_name, '_', 2)))  as file_site_code,
    {{ clean_organisation_id("upper(trim(split_part(file_name, '_', 2)))") }}
                                                as dv_provider_code,
    try_to_number(regexp_substr(upper(file_name), '_V([0-9]+)_', 1, 1, 'e', 1))
                                                as file_version,
    last_refreshed_at

from {{ ref('raw_sdl_wnl_meta_file_registry') }}
