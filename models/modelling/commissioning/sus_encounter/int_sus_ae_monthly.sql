{{
    config(materialized = 'table')
}}

--------------------------------------------------------------------------------
-- Migrated from [SUS].[AE].[EncounterDenormalised_DateRange] sandpit view.
--
-- Conversion notes:
--   * Source is now the staging model, so all inbound column names are snake_case.
--   * Output aliases snake_cased; bracket quoting removed (Snowflake folds
--     unquoted identifiers to uppercase, which matches the raw/stg convention).
--   * Lines tagged  -- [FN]  contain a function that needed translating from
--     T-SQL. Best-effort Snowflake equivalents are in place -- review/replace.
--   * One line remains tagged  -- [FN][TODO] : the ETL.udf_ACPFromActivityDate
--     scalar UDF, stubbed as NULL so the model still compiles.
--   * The bare CAST(<time col> AS DATETIME) columns are translated literally:
--     T-SQL anchors a bare time to 1900-01-01, so that base date is preserved.
--     The stg model does carry the real paired date for each of these
--     (dv_em_initial_assessment_date, dv_em_date_seen_for_treatment,
--     dv_em_attendance_conclusion_date, em_departure_date) -- swap to_date()
--     in if the sandpit consumers want true timestamps instead of parity.
--   * z_financial_year: T-SQL wrapped both halves of dv_fin_year in YEAR().
--     Rather than parse the second half, the end year is now start + 1, which
--     yields the same '2324' result whether dv_fin_year holds '2023' or
--     '2023/2024'. Confirm the format with: select distinct dv_fin_year.
--   * Reference data and banding logic extracted out of the model:
--       - commissioner in-area list -> seed ref_commissioning_access_codes
--       - age bands / patient type  -> macros age_range_derived, patient_type
--     Lines still tagged  -- [HARDCODE]  are fixed constants (schema version,
--     record type indicators) that are correct to keep inline.
--   * NAME COLLISION: T-SQL had both [SUS Version] (NULL) and SUSVersion
--     ('Current'). Both snake_case to sus_version; the second is currently
--     aliased sus_version_derived. Confirm which one downstream expects.
--------------------------------------------------------------------------------

with source as (

    select * from {{ ref('stg_sus_ae_monthly_encounterdenormalised_daterange') }}

),

commissioning_access_codes as (

    -- distinct guard: prevents fan-out if the seed ever gains duplicate codes
    select distinct commissioner_code
    from {{ ref('ref_commissioning_access_codes') }}
    where is_active

)

select
     a.age_at_cds_activity_date as age_at_cds_activity_date
    ,a.age_at_cds_activity_date as age_at_record_end
    ,a.age_at_cds_activity_date as age_at_record_start
    ,{{age_range_derived('a.age_at_cds_activity_date') }} as age_range_derived
    ,{{calculate_acp_from_activity_date('a.arrival_date') }} as applicable_costing_period                                        
    ,timestamp_ntz_from_parts(
        to_date(a.arrival_date),
        to_time(a.arrival_time)
     ) as applicable_date_and_time                                               
    ,round(a.dv_mff_index_applied, 6) as applied_mff_national                    
    ,null as applied_mff_non_mandatory
    ,null as area_code_derived
    ,a.arrival_date as arrival_date
    ,timestamp_ntz_from_parts(
        to_date('1900-01-01'),
        to_time(a.arrival_time)
     ) as arrival_time                                                           
    ,null as attendance_location_class
    ,a.provider_site_code as attendance_site_code
    ,null as bulk_replacement_cds_group
    ,a.carer_support_indicator as carer_support_indicator
    ,null as cds_activity_date
    ,null as cds_group_derived
    ,'4' as cds_group_indicator                                                  
    ,a.cds_record_type as cds_record_type
    ,'CDS062' as cds_schema_version                                             
    ,null as census_output_area_2001
    ,null as code_cleaning
    ,a.organisation_code_code_of_commissioner as commissioner_code_original_data
    ,a.commissioner_reference_no as commissioner_reference_no
    ,null as commissioner_site_code
    ,a.commissioning_serial_no_agreement_no as commissioning_serial_no_agreement_no
    ,null as confidentiality_category
    ,null as configurable_indicator
    ,null as consultant_code_type
    ,null as consultant_organisation_code
    ,a.core_hrg as core_hrg
    ,null as cost_period_spell_status_indicator
    ,null as costed_indicator
    ,null as costing_batch_sequence
    ,null as country
    ,null as county_code
    ,null as current_period_number
    ,a.age_at_cds_activity_date as derived_age
    ,null as derived_em_department_type
    ,null as diagnosis_scheme_in_use
    ,null as diagnosis_type
    ,null as ed_county_code
    ,null as ed_district_code
    ,null as electoral_ward_1998
    ,null as electoral_ward_division
    ,null as em_assessment_waiting_time
    ,null as em_attendance_category_id
    ,a.em_attendance_category as em_attendance_category
    ,timestamp_ntz_from_parts(
        to_date('1900-01-01'),
        to_time(a.em_attendance_conclusion_time)
     ) as em_attendance_conclusion_time                                          -- [FN]
    ,a.em_attendance_disposal as em_attendance_disposal
    ,a.em_attendance_number as em_attendance_number
    ,null as em_conclusion_waiting_time
    ,null as em_department_type_miu_indicator_derived
    ,a.em_department_type as em_department_type
    ,timestamp_ntz_from_parts(
        to_date('1900-01-01'),
        to_time(a.em_departure_time)
     ) as em_departure_time                                                      -- [FN]
    ,a.em_diagnosis_first as em_diagnosis_first
    ,a.em_diagnosis_second_1 as em_diagnosis_second_1
    ,a.em_diagnosis_second_2 as em_diagnosis_second_2
    ,a.em_diagnosis_second_3 as em_diagnosis_second_3
    ,a.em_diagnosis_second_4 as em_diagnosis_second_4
    ,a.em_diagnosis_second_5 as em_diagnosis_second_5
    ,a.em_diagnosis_second_6 as em_diagnosis_second_6
    ,a.em_diagnosis_second_7 as em_diagnosis_second_7
    ,a.em_diagnosis_second_8 as em_diagnosis_second_8
    ,a.em_diagnosis_second_9 as em_diagnosis_second_9
    ,a.em_diagnosis_second_10 as em_diagnosis_second_10
    ,a.em_diagnosis_second_11 as em_diagnosis_second_11
    ,a.em_diagnosis_second_12 as em_diagnosis_second_12
    ,a.em_duration_time as em_duration_time
    ,a.em_incident_location_type as em_incident_location_type
    ,timestamp_ntz_from_parts(
        to_date('1900-01-01'),
        to_time(a.em_initial_assessment_time)
     ) as em_initial_assessment_time                                             -- [FN]
    ,a.em_investigation_first as em_investigation_first
    ,a.em_investigation_second_1 as em_investigation_second_1
    ,a.em_investigation_second_2 as em_investigation_second_2
    ,a.em_investigation_second_3 as em_investigation_second_3
    ,a.em_investigation_second_4 as em_investigation_second_4
    ,a.em_investigation_second_5 as em_investigation_second_5
    ,a.em_investigation_second_6 as em_investigation_second_6
    ,a.em_investigation_second_7 as em_investigation_second_7
    ,a.em_investigation_second_8 as em_investigation_second_8
    ,a.em_investigation_second_9 as em_investigation_second_9
    ,a.em_investigation_second_10 as em_investigation_second_10
    ,a.em_investigation_second_11 as em_investigation_second_11
    ,a.em_investigation_second_12 as em_investigation_second_12
    ,case
        when a.em_mode_of_arrival = '-1' then null
        else a.em_mode_of_arrival
     end as em_mode_of_arrival                                                   -- [HARDCODE] sentinel '-1'
    ,a.em_patient_group as em_patient_group
    ,a.em_referral_source as em_referral_source
    ,a.em_staff_member_code as em_staff_member_code
    ,null as em_tariff_id
    ,timestamp_ntz_from_parts(
        to_date('1900-01-01'),
        to_time(a.em_time_seen_for_treatment)
     ) as em_time_seen_for_treatment                                             -- [FN]
    ,a.em_treatment_first as em_treatment_first
    ,a.em_treatment_second_1 as em_treatment_second_1
    ,a.em_treatment_second_2 as em_treatment_second_2
    ,a.em_treatment_second_3 as em_treatment_second_3
    ,a.em_treatment_second_4 as em_treatment_second_4
    ,a.em_treatment_second_5 as em_treatment_second_5
    ,a.em_treatment_second_6 as em_treatment_second_6
    ,a.em_treatment_second_7 as em_treatment_second_7
    ,a.em_treatment_second_8 as em_treatment_second_8
    ,a.em_treatment_second_9 as em_treatment_second_9
    ,a.em_treatment_second_10 as em_treatment_second_10
    ,a.em_treatment_second_11 as em_treatment_second_11
    ,a.em_treatment_second_12 as em_treatment_second_12
    ,null as em_treatment_wait_time
    ,left(a.ethnic_category_code, 1) as ethnic_category_code                     -- [FN]
    ,null as exclusion_reason
    ,null as extract_date
    ,a.pbr_final_tariff as final_tariff_applied
    ,null as finished_indicator
    ,null as first_gp_organisation_code
    ,null as first_referrer_organisation_code
    ,a.gender_code as gender_code
    ,a.generated_record_id as generated_record_id
    ,null as government_office_region_code
    ,null as gp_code_type
    ,null as gp_consortium_code
    ,null as gp_pct_type_derived
    ,case
        when length(a.gp_practice_code_original_data) > 6
            then left(a.gp_practice_code_original_data, 6)
        else a.gp_practice_code_original_data
     end as gp_practice_code_original_data                                       -- [FN] LEN -> LENGTH
    ,a.gp_practice_code as gp_practice_code
    ,null as gp_practice_derived_from_pds
    ,null as grouping_algorithm_version
    ,null as grouping_hrg_version
    ,null as grouping_reference_data_version
    ,null as hes_identifier
    ,null as hierarchy
    ,null as hrg_code_submitted
    ,null as hrg_code_version_calculated
    ,null as hrg_code_version_submitted
    ,null as hrg_dominant_grouping_variable_procedure
    ,null as hrg_dominant_grouping_variable
    ,null as hrg_procedure_scheme
    ,null as icd_10_primary_diagnosis
    ,null as intended_procedure_status
    ,null as interchange_id
    ,null as investigation_scheme_in_use
    ,null as lead_care_activity_indicator
    ,null as local_authority_code
    ,null as location_type_code
    ,null as marital_status
    ,null as market_forces_factor_id
    ,null as match_criterion_indicator
    ,null as maximum_episode_date
    ,null as mff_adjustment_non_mandatory
    ,a.applied_mff_national as mff_adjustment
    ,null as month_of_birth
    ,null as nhs_number_status_indicator
    ,null as nhs_rid_from_provider
    ,a.nhs_service_agreement_line_no as nhs_service_agreement_line_no
    ,a.number_diagnosis as number_diagnosis
    -- [FN] T-SQL used (ISNULL(x,1) - ISNULL(x-1,1)) as a non-null flag; rewritten as iff()
    ,  iff(a.em_investigation_first     is not null, 1, 0)
     + iff(a.em_investigation_second_1  is not null, 1, 0)
     + iff(a.em_investigation_second_2  is not null, 1, 0)
     + iff(a.em_investigation_second_3  is not null, 1, 0)
     + iff(a.em_investigation_second_4  is not null, 1, 0)
     + iff(a.em_investigation_second_5  is not null, 1, 0)
     + iff(a.em_investigation_second_6  is not null, 1, 0)
     + iff(a.em_investigation_second_7  is not null, 1, 0)
     + iff(a.em_investigation_second_8  is not null, 1, 0)
     + iff(a.em_investigation_second_9  is not null, 1, 0)
     + iff(a.em_investigation_second_10 is not null, 1, 0)
     + iff(a.em_investigation_second_11 is not null, 1, 0)
     + iff(a.em_investigation_second_12 is not null, 1, 0)
       as number_em_investigations
    -- [FN] same pattern as above
    ,  iff(a.em_treatment_first     is not null, 1, 0)
     + iff(a.em_treatment_second_1  is not null, 1, 0)
     + iff(a.em_treatment_second_2  is not null, 1, 0)
     + iff(a.em_treatment_second_3  is not null, 1, 0)
     + iff(a.em_treatment_second_4  is not null, 1, 0)
     + iff(a.em_treatment_second_5  is not null, 1, 0)
     + iff(a.em_treatment_second_6  is not null, 1, 0)
     + iff(a.em_treatment_second_7  is not null, 1, 0)
     + iff(a.em_treatment_second_8  is not null, 1, 0)
     + iff(a.em_treatment_second_9  is not null, 1, 0)
     + iff(a.em_treatment_second_10 is not null, 1, 0)
     + iff(a.em_treatment_second_11 is not null, 1, 0)
     + iff(a.em_treatment_second_12 is not null, 1, 0)
       as number_em_treatments
    ,a.number_procedures as number_procedures
    ,null as org_code_local_patient_identifier
    ,a.organisation_code_code_of_commissioner as organisation_code_code_of_commissioner
    ,left(a.organisation_code_code_of_provider, 3) as organisation_code_code_of_provider  -- [FN]
    ,a.organisation_code_pct_of_residence as organisation_code_pct_of_residence
    ,null as organisation_code_sender
    ,a.gp_practice_code as organisation_code_gp
    ,null as organisation_code_patient_pathway_identifier
    ,null as organisation_code_type_commissioner
    ,null as organisation_code_type_consultant
    ,null as organisation_code_type_gp
    ,null as organisation_code_type_location
    ,null as organisation_code_type_pct_of_residence
    ,null as organisation_code_type_prime_recipient
    ,null as organisation_code_type_provider
    ,null as organisation_code_type_referrer
    ,null as organisation_code_type_sender
    ,null as other_indicator
    ,null as patient_postcode_derived_pct_type
    ,a.patient_postcode_derived_pct as patient_postcode_derived_pct
    ,null as patient_postcode_electoral_ward
    ,{{ patient_type('a.age_at_cds_activity_date') }} as patient_type
    ,a.pbr_final_tariff as pbr_final_tariff
    ,null as pbr_generated_interchange_id
    ,null as pbr_spell_cost_id
    ,null as pbr_spell_cost_version_date
    ,null as pbr_spell_frozen_indicator
    ,null as pbr_spell_service_id_version
    ,null as pbr_spell_status_indicator
    ,a.pct_derived_from_gp_practice as pct_derived_from_gp_practice
    ,a.pct_derived_from_gp_practice as pct_derived_from_gp
    ,a.pct_of_residence_original as pct_of_residence_original
    ,a.pct_responsible as pct_responsible
    ,null as postcode_sector_of_usual_address
    ,null as primary_procedure_date
    ,null as primary_procedure
    ,null as prime_recipient
    ,null as procedure_date_of_first_treatment
    ,null as procedure_date_of_subsequent_treatments_1
    ,null as procedure_date_of_subsequent_treatments_2
    ,null as procedure_date_of_subsequent_treatments_3
    ,null as procedure_date_of_subsequent_treatments_4
    ,null as procedure_date_of_subsequent_treatments_5
    ,null as procedure_date_of_subsequent_treatments_6
    ,null as procedure_date_of_subsequent_treatments_7
    ,null as procedure_date_of_subsequent_treatments_8
    ,null as procedure_date_of_subsequent_treatments_9
    ,null as procedure_date_of_subsequent_treatments_10
    ,null as procedure_date_of_subsequent_treatments_11
    ,null as procedure_date_of_subsequent_treatments_12
    ,null as procedure_scheme_in_use
    ,null as protocol_identifier
    ,a.organisation_code_code_of_provider as provider_code_original_data
    ,null as provider_location
    ,a.provider_reference_no as provider_reference_no
    ,a.provider_site_code as provider_site_code
    ,null as pseudonymised_status
    ,null as query_date
    ,null as reason_access_provided
    ,null as record_extraction_indicator
    ,null as re_costing_requested_flag
    ,null as referrer_code_type
    ,a.registered_gmp_code_original_data as registered_gmp_code_original_data
    ,null as registered_gmp_code
    ,null as report_period_end_date
    ,null as report_period_start_date
    ,null as rtt_length_derived
    ,null as rtt_patient_pathway_identifier
    ,null as rtt_period_end_date
    ,null as rtt_period_start_date
    ,null as rtt_status
    ,null as secondary_diagnosis_code_1
    ,null as secondary_diagnosis_code_2
    ,null as secondary_diagnosis_code_3
    ,null as secondary_diagnosis_code_4
    ,null as secondary_diagnosis_code_5
    ,null as secondary_diagnosis_code_6
    ,null as secondary_diagnosis_code_7
    ,null as secondary_diagnosis_code_8
    ,null as secondary_diagnosis_code_9
    ,null as secondary_diagnosis_code_10
    ,null as secondary_diagnosis_code_11
    ,null as secondary_diagnosis_code_12
    ,null as secondary_procedure_code_1
    ,null as secondary_procedure_code_2
    ,null as secondary_procedure_code_3
    ,null as secondary_procedure_code_4
    ,null as secondary_procedure_code_5
    ,null as secondary_procedure_code_6
    ,null as secondary_procedure_code_7
    ,null as secondary_procedure_code_8
    ,null as secondary_procedure_code_9
    ,null as secondary_procedure_code_10
    ,null as secondary_procedure_code_11
    ,null as secondary_procedure_code_12
    ,null as secondary_procedure_date_1
    ,null as secondary_procedure_date_2
    ,null as secondary_procedure_date_3
    ,null as secondary_procedure_date_4
    ,null as secondary_procedure_date_5
    ,null as secondary_procedure_date_6
    ,null as secondary_procedure_date_7
    ,null as secondary_procedure_date_8
    ,null as secondary_procedure_date_9
    ,null as secondary_procedure_date_10
    ,null as secondary_procedure_date_11
    ,null as secondary_procedure_date_12
    ,null as sha_commissioner
    ,null as sha_from_gp_derived
    ,null as sha_from_patient_postcode
    ,null as sha_old_org_code
    ,null as sha_provider
    ,null as sha_type_from_gp_derived
    ,null as sha_type_from_patient_postcode
    ,null as spare_1
    ,null as spare_2
    ,null as spare_3
    ,null as spare_4
    ,null as spare_5
    ,null as spell_complete_indicator
    ,null as spell_const_version_no
    ,null as spell_error_status
    ,a.spell_identifier as spell_identifier
    ,case a.dv_is_pbr
        when 1 then '1'
        when 0 then '0'
     end as spell_in_pbr_not_in_pbr
    ,null as spell_version_as_at_date_and_time
    ,null as staging_loaded_date
    ,null as tariff_adjustment_future_use_1_national
    ,null as tariff_adjustment_future_use_1_non_mandatory
    ,null as tariff_adjustment_future_use_2_national
    ,null as tariff_adjustment_future_use_2_non_mandatory
    ,null as tariff_financial_adjustment_national
    ,null as tariff_financial_adjustment_non_mandatory
    ,a.tariff_initial_amount_national as tariff_initial_amount_national
    ,null as tariff_initial_amount_non_mandatory
    ,a.tariff_pre_mff_adjusted_national as tariff_pre_mff_adjusted_national
    ,null as tariff_pre_mff_adjusted_non_mandatory
    ,null as tariff_total_payment_local
    ,coalesce(a.tariff_total_payment_national, a.pbr_final_tariff)
        as tariff_total_payment_national                                         -- [FN] ISNULL -> COALESCE
    ,null as tariff_total_payment_non_mandatory
    ,null as tarrif_initial_amount_local
    ,null as temporary_cost_period_status
    ,null as test_indicator
    ,null as unique_booking_reference_number_converted
    ,a.unique_cds_identifier as unique_cds_identifier
    ,a.dv_query_id as unique_query_id
    ,null as update_type
    ,null as version_sequence_number
    ,year(a.arrival_date) - a.age_at_cds_activity_date as year_of_birth           -- [FN]
    ,null as z_arrival_datetime
    ,null as z_em_assessment_waiting_datetime
    ,null as z_em_attendance_conclusion_datetime
    ,null as z_em_conclusion_waiting_datetime
    ,null as z_em_datetime_seen_for_treatment
    ,null as z_em_departure_datetime
    ,null as z_em_duration_datetime
    ,null as z_em_initial_assessment_datetime
    ,null as z_em_treatment_wait_datetime
    ,iff(ca.commissioner_code is not null, 1, 0) as z_commissioning_access
    ,null as z_lsoa01
    ,null as z_nhs_number_pseudo
    ,null as z_pod_level_4
    ,null as z_provider_site_code
    ,null as z_sensitive_data_category
    ,null as z_ccg_code
    ,null as z_pct_code
    ,right(left(a.dv_fin_year, 4), 2)
        || right(to_varchar(left(a.dv_fin_year, 4)::int + 1), 2)
        as z_financial_year                                                      -- [FN] end year derived arithmetically rather than parsed -- see note in header
    ,null as z_care_home
    ,null as z_derived_price
    ,null as z_derived_price_flag
    ,null as z_derived_price_dt
    ,null as z_derived_price_row_id
    ,null as z_business_rule
    ,a.local_patient_identifier as z_local_patient_identifier
    ,a.dv_lsoa as z_lsoa11
    ,null as z_contract_type
    ,null as z_host_ccg_code
    ,null as z_nhse_category
    ,null as z_nhse_sub_category
    ,null as z_responsible_ccg_code
    ,null as z_cam_organisation_code
    ,a.local_patient_identifier as local_patient_identifier
    ,null as z_imd_2015_london_quintile
    ,null as z_imd_2015_national_decile
    ,null as z_nfa
    ,left(a.dv_fin_year, 4) || lpad(a.dv_fin_month::varchar, 2, '0')
        as z_financial_month                                                     
    ,a.sk_patient_id as sk_patient_id
    ,'CURRENT' as z_data_type                                                    
    ,a.sk_encounter_id as sk_encounter_id
    ,'Current' as sus_version                                         
from source a

left join commissioning_access_codes ca
    on a.organisation_code_code_of_commissioner = ca.commissioner_code
