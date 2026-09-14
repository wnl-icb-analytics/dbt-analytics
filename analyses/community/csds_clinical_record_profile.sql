select
    source_table
    , clinical_record_type
    , count(*) as record_count
    , count(distinct source_record_id) as unique_items
    , sum(accepted_source_record_count) as represented_source_rows
    , count_if(sk_patient_id is null) as missing_patient_key
    , count_if(clinical_at is null) as missing_clinical_time
    , count_if(clinical_code is not null and clinical_description is null) as unlabelled_codes
    , count_if(clinical_value is not null and clinical_code is null) as values_without_codes
    , count_if(is_source_derived_date_inconsistent) as derived_date_disagreements
    , count_if(is_care_activity_linked = false) as missing_activity_parents
    , count_if(is_care_activity_person_consistent = false) as activity_person_disagreements
    , count_if(is_submitted_contact_person_consistent = false) as submitted_contact_person_disagreements
    , count_if(is_clinical_date_after_reporting_period) as dates_after_reporting_period
    , count_if(is_assessment_before_reporting_period) as assessments_before_reporting_period
    , count_if(is_assessment_code_in_activity_component) as assessment_codes_in_components
from {{ ref('fct_csds_clinical_record') }}
group by source_table, clinical_record_type;

select clinical_record_type, assessment_response_status, count(*) as record_count
from {{ ref('fct_csds_clinical_record') }}
where assessment_response_status is not null
group by clinical_record_type, assessment_response_status;

select clinical_code_system, clinical_label_status, count(*) as record_count
from {{ ref('fct_csds_clinical_record') }}
group by clinical_code_system, clinical_label_status;

select unit_of_measurement_match_type, count(*) as observation_count,
    count_if(unit_of_measurement_code is null) as missing_unit
from {{ ref('fct_csds_clinical_record') }}
where clinical_record_type = 'observation'
group by unit_of_measurement_match_type;


-- Code tokens remain inside Snowflake; only format and coverage aggregates leave it.
select clinical_record_type, clinical_code_system,
    count(*) as record_count,
    count_if(clinical_code is not null and clinical_description is null) as unlabelled_codes,
    count_if(clinical_code <> trim(clinical_code)) as padded_codes,
    count_if(clinical_description is null and regexp_like(clinical_code, '[0-9]+'))
        as unlabelled_numeric_codes
from {{ ref('fct_csds_clinical_record') }}
group by clinical_record_type, clinical_code_system;

select year(reporting_period_end_date) as reporting_year,
    count(*) as immunisation_count,
    count_if(clinical_description is null and regexp_like(clinical_code, '[0-9]+'))
        as unlabelled_numeric_codes
from {{ ref('fct_csds_clinical_record') }}
where clinical_record_type = 'immunisation' and clinical_code_system = 'Read v2'
group by reporting_year;

select f.clinical_record_type,
    case
        when f.clinical_value_numeric is null then 'not_numeric_or_out_of_range'
        when s.numeric_range_count = 1 and f.clinical_value_numeric not between
            s.minimum_numeric_value and s.maximum_numeric_value then 'outside_published_range'
        when s.numeric_range_count = 1
            and f.clinical_value_numeric <> round(f.clinical_value_numeric, s.decimal_places)
            then 'outside_published_precision'
        when s.numeric_range_count = 0 then 'enumeration_or_unpublished_range_unmatched'
        else 'other_unmatched'
    end as unmatched_reason,
    count(*) as record_count
from {{ ref('fct_csds_clinical_record') }} as f
left join {{ ref('csds_assessment_scale') }} as s on trim(f.clinical_code) = s.concept_code
where f.assessment_response_status = 'response_unmatched'
group by f.clinical_record_type, unmatched_reason;

-- Reconcile historical aliases without returning submitted codes or unit tokens.
select
    case alias_unit.source_data_item
        when 'C202D48' then 'weight'
        when 'C202D47' then 'height'
        when 'C201D49' then 'BMI'
    end as measurement
    , f.clinical_code_system
    , count(*) as alias_labels
    , count_if(upper(trim(f.unit_of_measurement_code)) = 'KG/M' || chr(178)) as superscript_bmi_aliases
from {{ ref('fct_csds_clinical_record') }} as f
inner join {{ ref('csds_observation_unit_alias') }} as alias_unit
    on upper(trim(f.unit_of_measurement_code)) = alias_unit.source_unit_upper
    and case when f.clinical_code_system = 'SNOMED CT' then trim(f.clinical_code)
        else f.dictionary_snomed_code end = alias_unit.snomed_code
where f.unit_of_measurement_match_type = 'csds_etos_alias'
group by measurement, f.clinical_code_system;

select
    count(*) as exact_reference_matches
    , count_if(f.unit_of_measurement_description is distinct from u.description
        or f.unit_of_measurement_symbol is distinct from u.unit_symbol
        or f.unit_of_measurement_match_type is distinct from u.match_type
        or f.unit_of_measurement_definition_source is distinct from u.definition_source) as changed_exact_labels
from {{ ref('fct_csds_clinical_record') }} as f
inner join {{ ref('clinical_unit_of_measurement') }} as u
    on trim(f.unit_of_measurement_code) = u.code;
