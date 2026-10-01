{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns',
        tags = ['sdl', 'slam', 'lsdeplcm']
    )
}}

-- Staging model for the SDL LSDePLCM feed (SLAM high-cost devices PLD:
-- provider patient-level device contract-monitoring submissions, remapped via
-- the servicesdatalocal pipeline). Source: DATA_LAKE.SDL.LSDEPLCM — ~4M rows,
-- all TEXT.
--
-- Grain stays 1:1 with source. Columns <5% populated are dropped.
-- Shared cleaning (dv_financial_year/month, dv_dataset_created_at,
-- dv_provider_code, dv_is_latest_submission) lives in
-- macros/transformations/parse_slam.sql — see stg_lsplcm for the rationale.

with
{{ slam_submission_slices('LSDEPLCM', 'LSDePLCM') }}
{{ slam_incremental_slices('LSDEPLCM') }}

select
{{ slam_meta_columns() }},
{{ slam_submission_columns(period_date_feed='LSDEPLCM') }},
{{ slam_org_columns() }},
{{ slam_patient_columns() }},
{{ slam_service_columns() }},

        -- Feed-specific
        s.value_added_tax_charged_indicator_contract_monitoring
                                                as vat_charged_indicator,

        -- Device detail
        s.medical_device_name_high_cost_tariff_excluded_device
                                                as device_name,
        s.medical_device_quantity_high_cost_tariff_excluded_device
                                                as device_quantity_raw,
        {{ parse_slam_number('s.medical_device_quantity_high_cost_tariff_excluded_device') }}
                                                as dv_device_quantity,
        s.medical_device_size_high_cost_tariff_excluded_device
                                                as device_size,
        s.medical_device_manufacturer_high_cost_tariff_excluded_device
                                                as device_manufacturer,
        s.medical_device_serial_number_high_cost_tariff_excluded_device
                                                as device_serial_number,
        s.medical_device_procurement_route_high_cost_tariff_excluded_device
                                                as device_procurement_route,
        s.local_code_high_cost_tariff_excluded_device
                                                as local_device_code,
        s.high_level_code_high_cost_tariff_excluded_device
                                                as high_level_device_code,
        s.subsidiary_level_code_high_cost_tariff_excluded_device
                                                as subsidiary_device_code,
        {{ parse_uk_date('s.clinical_intervention_date_medical_device_implementation') }}
                                                as device_implantation_date,
        {{ parse_uk_date('s.device_insertion_date') }}
                                                as device_insertion_date,

        -- Linkage identifiers
        s.non_cds_unique_identifier             as non_cds_unique_identifier,
        s.out_patient_attendance_identifier     as out_patient_attendance_identifier,
        s.hospital_provider_spell_identifier    as hospital_provider_spell_identifier,

        -- Measures
        {{ parse_slam_number('s.unit_price_commissioner') }}
                                                as dv_unit_price_commissioner,
        {{ parse_slam_number('s.unit_price_supplier') }}
                                                as dv_unit_price_supplier,
        {{ parse_slam_number('s.total_cost') }} as dv_total_cost

from {{ ref('raw_sdl_wnl_lsdeplcm') }} as s
left join submission_slices_enriched as sl
{{ slam_slice_join() }}
{{ slam_incremental_where() }}
