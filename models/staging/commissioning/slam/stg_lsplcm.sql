{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns',
        tags = ['sdl', 'slam', 'lsplcm']
    )
}}

-- Staging model for the SDL LSPLCM feed (SLAM PLD: provider patient-level
-- contract-monitoring submissions, spreadsheet/CSV uploads remapped via the
-- servicesdatalocal pipeline). Source: DATA_LAKE.SDL.LSPLCM — ~800M rows,
-- 580+ cols (superset of every historic layout), all TEXT.
--
-- Grain stays 1:1 with source. Columns <5% populated are dropped; raw values
-- for cleaned fields remain in the source/raw layer.
--
-- Shared cleaning lives in macros/transformations/parse_slam.sql:
--   * dv_financial_year / dv_financial_month: provider free text validated to
--     'YYYYYY' (e.g. '202627') and 1-12. Invalid values (e.g. '215551',
--     misaligned rows) -> NULL, with the file-name FY token
--     ('PLCM_2627_InformationStandard...') as fallback. Raw values retained.
--   * dv_dataset_created_at: DATE_AND_TIME_DATA_SET_CREATED multi-format
--     parsed (>99.99% of populated values; ISO-only parsing got ~96%).
--   * Costs/activity to NUMBER(38,6): strips currency symbols, thousands
--     commas, accounting negatives. 'TBC' etc -> NULL.
--
-- Incremental, pure append: new (file, batch) pairs only, mirroring the
-- upstream SDL loader's NOT EXISTS mechanics. No mutable columns here —
-- latest-submission resolution lives in stg_slam_latest_submission (rebuilt
-- fully each run) and is exposed per feed via the stg_*_latest views; SLAM
-- files are cumulative-YTD restatements, so consumers must use those (or join
-- the lookup) to avoid double-counting.

with
{{ slam_submission_slices('LSPLCM', 'LSPLCM') }}
{{ slam_incremental_slices('LSPLCM') }}

select
{{ slam_meta_columns() }},
{{ slam_submission_columns(period_date_feed='LSPLCM') }},
{{ slam_org_columns() }},
{{ slam_patient_columns() }},
{{ slam_service_columns() }},

        -- Feed-specific: service / tariff detail
        s.tariff_code                           as tariff_code,
        s.national_tariff_indicator             as national_tariff_indicator,
        s.local_sub_specialty_code              as local_sub_specialty_code,
        s.unbundled_episode_indicator           as unbundled_episode_indicator,

        -- Activity. Early layouts (pre-Sep 2021) used the plain column names,
        -- the standard uses *_CONTRACT_MONITORING - coalesce so the dates
        -- survive both eras.
        coalesce(
            {{ parse_uk_date('s.activity_start_date_contract_monitoring') }},
            {{ parse_uk_date('s.activity_start_date') }}
        )                                       as activity_start_date,
        coalesce(
            {{ parse_uk_date('s.activity_end_date_contract_monitoring') }},
            {{ parse_uk_date('s.activity_end_date') }}
        )                                       as activity_end_date,
        {{ parse_uk_date('s.activity_date') }}  as activity_date,
        try_to_number(s.adjusted_length_of_stay)
                                                as adjusted_length_of_stay,

        -- Linkage identifiers
        s.cds_unique_identifier                 as cds_unique_identifier,
        s.non_cds_unique_identifier             as non_cds_unique_identifier,
        s.out_patient_attendance_identifier     as out_patient_attendance_identifier,
        s.hospital_provider_spell_identifier    as hospital_provider_spell_identifier,

        -- Measures
        {{ parse_slam_number('s.activity_count_point_of_delivery') }}
                                                as dv_activity_count,
        {{ parse_slam_number('s.activity_unit_price') }}
                                                as dv_activity_unit_price,
        {{ parse_slam_number('s.total_cost') }} as dv_total_cost

from {{ ref('raw_sdl_wnl_lsplcm') }} as s
left join submission_slices_enriched as sl
{{ slam_slice_join() }}
{{ slam_incremental_where() }}
