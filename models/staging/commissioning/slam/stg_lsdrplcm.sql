{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns',
        tags = ['sdl', 'slam', 'lsdrplcm']
    )
}}

-- Staging model for the SDL LSDrPLCM feed (SLAM high-cost drugs PLD:
-- provider patient-level drug contract-monitoring submissions, remapped via
-- the servicesdatalocal pipeline). Source: DATA_LAKE.SDL.LSDRPLCM — ~52M rows,
-- all TEXT.
--
-- Grain stays 1:1 with source. Columns <5% populated are dropped.
-- Shared cleaning (dv_financial_year/month, dv_dataset_created_at,
-- dv_provider_code, dv_is_latest_submission) lives in
-- macros/transformations/parse_slam.sql — see stg_lsplcm for the rationale.

with
{{ slam_submission_slices('LSDRPLCM', 'LSDrPLCM') }}
{{ slam_incremental_slices('LSDRPLCM') }}

select
{{ slam_meta_columns() }},
{{ slam_submission_columns(period_date_feed='LSDRPLCM') }},
{{ slam_org_columns() }},
{{ slam_patient_columns() }},
{{ slam_service_columns() }},

        -- Feed-specific
        s.value_added_tax_charged_indicator_contract_monitoring
                                                as vat_charged_indicator,

        -- Drug detail
        s.drug_name_high_cost_tariff_excluded_drug
                                                as drug_name,
        s.drug_strength_high_cost_tariff_excluded_drug
                                                as drug_strength,
        s.drug_pack_size_high_cost_tariff_excluded_drug
                                                as drug_pack_size,
        s.drug_quantity_or_weight_proportion_high_cost_tariff_excluded_drug
                                                as drug_quantity_raw,
        {{ parse_slam_number('s.drug_quantity_or_weight_proportion_high_cost_tariff_excluded_drug') }}
                                                as dv_drug_quantity,
        s.dispensing_route_high_cost_tariff_excluded_drug
                                                as dispensing_route,
        s.high_cost_tariff_excluded_drug_code_snomed_ct_dm_plus_d
                                                as drug_code_dmd,
        s.dm_plus_d_taxonomy_code_high_cost_tariff_excluded_drug
                                                as dmd_taxonomy_code,
        s.therapeutic_indication_code_snomed_ct as therapeutic_indication_code,
        coalesce(
            s.route_of_administration_snomed_ct_dm_plus_d,
            s.route_of_administration_snomed_ct
        )                                       as route_of_administration_code,
        coalesce(
            s.unit_of_measurement_snomed_ct_dm_plus_d,
            s.unit_of_measurement_dm_plus_d
        )                                       as unit_of_measurement_code,
        {{ parse_uk_date('s.clinical_intervention_date_drug_dispensed') }}
                                                as drug_dispensed_date,
        {{ parse_uk_date('s.drug_delivery_date') }}
                                                as drug_delivery_date,

        -- Linkage identifiers
        s.non_cds_unique_identifier             as non_cds_unique_identifier,
        s.out_patient_attendance_identifier     as out_patient_attendance_identifier,
        s.hospital_provider_spell_identifier    as hospital_provider_spell_identifier,

        -- Measures
        {{ parse_slam_number('s.unit_price_commissioner') }}
                                                as dv_unit_price_commissioner,
        {{ parse_slam_number('s.unit_price_supplier') }}
                                                as dv_unit_price_supplier,
        {{ parse_slam_number('s.commissioner_support_charge') }}
                                                as dv_commissioner_support_charge,
        {{ parse_slam_number('s.total_cost') }} as dv_total_cost

from {{ ref('raw_sdl_wnl_lsdrplcm') }} as s
left join submission_slices_enriched as sl
{{ slam_slice_join() }}
{{ slam_incremental_where() }}
