{{ config(materialized='view') }}

-- The single inclusion union for IAPT clinical items. Every value is selected from a typed
-- fact, not derived here; a care activity yields up to three items.
with items as (
    select
        source_record_id as originating_source_record_id
        , 'fct_iapt_assessment_score' as source_model_name
        , source_table
        , 'assessment_score' as clinical_record_type
        , person_id
        , sk_patient_id
        , referral_id
        , care_contact_id
        , care_activity_id
        , parent_record_id
        , parent_record_type
        , parent_model_name
        , clinical_date
        , clinical_time
        , clinical_at
        , clinical_time_precision
        , clinical_time_basis
        , clinical_date_status
        , is_source_date_inconsistent
        , 'SNOMED CT' as coding_system
        , assessment_tool_code as clinical_code
        , observable_description as clinical_description
        , observable_label_status as clinical_label_status
        , null::varchar as clinical_qualifier_code
        , null::varchar as clinical_qualifier_description
        , assessment_tool_code as snomed_code
        , score_text as clinical_value
        , response_description as clinical_value_description
        , score_numeric_parsed as clinical_value_numeric
        , score_parse_status as clinical_value_parse_status
        , null::varchar as unit_of_measurement_code
        , null::varchar as unit_of_measurement_description
        , null::varchar as unit_of_measurement_symbol
        , assessment_tool_name
        , assessment_score_numeric
        , assessment_response_status
        , is_assessment_response_non_score
        , provider_organisation_code
        , provider_organisation_name
        , reporting_period_end_date
        , source_file_received_at
        , source_loaded_at
    from {{ ref('fct_iapt_assessment_score') }}

    union all

    select
        source_record_id
        , 'fct_iapt_health_condition'
        , source_table
        , condition_record_type
        , person_id
        , sk_patient_id
        , referral_id
        , null::varchar
        , null::varchar
        , parent_record_id
        , parent_record_type
        , parent_model_name
        , clinical_date
        , null::time
        , clinical_at
        , clinical_time_precision
        , clinical_time_basis
        , clinical_date_status
        , null::boolean
        , coding_system
        , submitted_code
        , code_description
        , code_label_status
        , null::varchar
        , null::varchar
        , snomed_code
        , null::varchar
        , null::varchar
        , null::number(38, 9)
        , null::varchar
        , null::varchar
        , null::varchar
        , null::varchar
        , null::varchar
        , null::number(38, 9)
        , null::varchar
        , null::boolean
        , provider_organisation_code
        , provider_organisation_name
        , reporting_period_end_date
        , source_file_received_at
        , source_loaded_at
    from {{ ref('fct_iapt_health_condition') }}
    -- ETOS v2.1.22 IDS603 row 24: an undated complaint later sent with a date ceases to be valid.
    -- The typed fact keeps it; the union excludes it so the complaint is not listed twice.
    where not coalesce(is_superseded_by_dated_record, false)

    union all

    select
        source_record_id
        , 'fct_iapt_care_activity'
        , source_table
        , 'procedure'
        , person_id
        , sk_patient_id
        , referral_id
        , care_contact_id
        , care_activity_id
        , parent_record_id
        , parent_record_type
        , parent_model_name
        , clinical_date
        , clinical_time
        , clinical_at
        , clinical_time_precision
        , clinical_time_basis
        , clinical_date_status
        , is_source_date_inconsistent
        , 'SNOMED CT'
        , procedure_expression
        , procedure_description
        , procedure_label_status
        , procedure_context_code
        , procedure_context_description
        , asserted_procedure_code
        , null::varchar
        , null::varchar
        , null::number(38, 9)
        , null::varchar
        , null::varchar
        , null::varchar
        , null::varchar
        , null::varchar
        , null::number(38, 9)
        , null::varchar
        , null::boolean
        , provider_organisation_code
        , provider_organisation_name
        , reporting_period_end_date
        , source_file_received_at
        , source_loaded_at
    from {{ ref('fct_iapt_care_activity') }}
    where has_procedure

    union all

    select
        source_record_id
        , 'fct_iapt_care_activity'
        , source_table
        , 'finding'
        , person_id
        , sk_patient_id
        , referral_id
        , care_contact_id
        , care_activity_id
        , parent_record_id
        , parent_record_type
        , parent_model_name
        , clinical_date
        , clinical_time
        , clinical_at
        , clinical_time_precision
        , clinical_time_basis
        , clinical_date_status
        , is_source_date_inconsistent
        , finding_coding_system
        , finding_code
        , finding_description
        , finding_label_status
        , null::varchar
        , null::varchar
        , finding_snomed_code
        , null::varchar
        , null::varchar
        , null::number(38, 9)
        , null::varchar
        , null::varchar
        , null::varchar
        , null::varchar
        , null::varchar
        , null::number(38, 9)
        , null::varchar
        , null::boolean
        , provider_organisation_code
        , provider_organisation_name
        , reporting_period_end_date
        , source_file_received_at
        , source_loaded_at
    from {{ ref('fct_iapt_care_activity') }}
    where has_finding

    union all

    select
        source_record_id
        , 'fct_iapt_care_activity'
        , source_table
        , 'observation'
        , person_id
        , sk_patient_id
        , referral_id
        , care_contact_id
        , care_activity_id
        , parent_record_id
        , parent_record_type
        , parent_model_name
        , clinical_date
        , clinical_time
        , clinical_at
        , clinical_time_precision
        , clinical_time_basis
        , clinical_date_status
        , is_source_date_inconsistent
        , 'SNOMED CT'
        , observation_code
        , observation_description
        , observation_label_status
        , null::varchar
        , null::varchar
        , observation_code
        , observation_value
        , null::varchar
        , observation_value_numeric
        , observation_value_parse_status
        , unit_of_measurement_code
        , unit_of_measurement_description
        , unit_of_measurement_symbol
        , null::varchar
        , null::number(38, 9)
        , null::varchar
        , null::boolean
        , provider_organisation_code
        , provider_organisation_name
        , reporting_period_end_date
        , source_file_received_at
        , source_loaded_at
    from {{ ref('fct_iapt_care_activity') }}
    where has_observation
)

select
    {{ dbt_utils.generate_surrogate_key(['source_model_name', 'originating_source_record_id', 'clinical_record_type']) }}
        as source_record_id
    , 'IAPT' as source_dataset
    , *
from items
