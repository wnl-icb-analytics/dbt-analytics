{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns',
        tags = ['sdl', 'slam', 'lsacm']
    )
}}

-- Staging model for the SDL LSACM feed (SLAM aggregate contract monitoring:
-- provider plan/actual activity and price by POD, spreadsheet/CSV uploads
-- remapped via the servicesdatalocal pipeline). Source: DATA_LAKE.SDL.LSACM —
-- ~600M rows, all TEXT. Aggregate feed: no patient-level fields.
--
-- Grain stays 1:1 with source. Columns <5% populated are dropped.
-- Shared cleaning (dv_financial_year/month, dv_dataset_created_at,
-- dv_provider_code, dv_is_latest_submission) lives in
-- macros/transformations/parse_slam.sql — see stg_lsplcm for the rationale.

with
{{ slam_submission_slices('LSACM', 'LSACM') }}
{{ slam_incremental_slices('LSACM') }}

select
{{ slam_meta_columns() }},
{{ slam_submission_columns() }},
{{ slam_org_columns() }},
{{ slam_service_columns() }},

        -- Feed-specific: service / tariff detail
        s.tariff_code                           as tariff_code,
        s.national_tariff_indicator             as national_tariff_indicator,
        s.local_sub_specialty_code              as local_sub_specialty_code,

        -- Measures (plan vs actual)
        {{ parse_slam_number('s.contract_monitoring_planned_activity') }}
                                                as dv_planned_activity,
        {{ parse_slam_number('s.contract_monitoring_actual_activity') }}
                                                as dv_actual_activity,
        {{ parse_slam_number('s.contract_monitoring_planned_price') }}
                                                as dv_planned_price,
        {{ parse_slam_number('s.contract_monitoring_actual_price') }}
                                                as dv_actual_price,
        {{ parse_slam_number('s.contract_monitoring_planned_market_forces_factor') }}
                                                as dv_planned_market_forces_factor,
        {{ parse_slam_number('s.contract_monitoring_actual_market_forces_factor') }}
                                                as dv_actual_market_forces_factor

from {{ ref('raw_sdl_wnl_lsacm') }} as s
left join submission_slices_enriched as sl
{{ slam_slice_join() }}
{{ slam_incremental_where() }}
