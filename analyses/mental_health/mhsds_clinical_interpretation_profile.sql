-- High-level aggregates only. Do not add submitted codes, values or patient identifiers.
select clinical_record_type, assessment_response_status, count(*) as clinical_records,
    count_if(assessment_tool_name is not null) as tool_labelled_records,
    count_if(clinical_value_description is not null) as response_labelled_records,
    count_if(assessment_score_numeric is not null) as interpreted_numeric_scores
from {{ ref('fct_mhsds_clinical_record') }}
where source_table in ('MHS606', 'MHS607')
group by clinical_record_type, assessment_response_status;

select unit_of_measurement_label_status, unit_of_measurement_match_type,
    unit_of_measurement_definition_source, count(*) as observations
from {{ ref('fct_mhsds_clinical_record') }}
where clinical_record_type = 'observation'
group by 1, 2, 3;

select clinical_record_type, count(*) as clinical_records,
    count(distinct clinical_record_id) as distinct_clinical_records,
    sum(accepted_source_record_count) as accepted_source_rows,
    count_if(person_id is null) as no_national_person_id,
    count_if(is_care_contact_person_consistent = false) as contact_person_conflicts,
    count_if(clinical_description is null) as no_clinical_label
from {{ ref('fct_mhsds_clinical_record') }}
group by clinical_record_type;

select clinical_record_type, assessment_tool_name, count(*) as unmatched_responses,
    count_if(clinical_value_numeric is null) as non_numeric_responses,
    count_if(clinical_value_numeric = 0) as zero_responses
from {{ ref('fct_mhsds_clinical_record') }}
where assessment_response_status = 'response_unmatched'
group by clinical_record_type, assessment_tool_name
having count(*) >= 1000;
