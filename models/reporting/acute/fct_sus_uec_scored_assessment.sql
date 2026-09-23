select
    {{ dbt_utils.generate_surrogate_key(["'ecds_scored_assessment'", 's.visit_occurrence_id', 's.source_sequence']) }} as assessment_id
    , s.visit_occurrence_id
    , s.source_sequence
    , s.source_row_id
    , s.dmic_import_log_id
    , e.sk_patient_id
    , e.visit_occurrence_id is not null as is_attendance_linked
    , e.start_date as attendance_date
    , e.organisation_id
    , e.organisation_name
    , e.site_id
    , e.site_name
    , s.assessment_tool_code
    , coalesce(r.preferred_term, c.preferred_term) as assessment_description
    , r.measurement_category
    , r.ecds_group1
    , case
        when r.preferred_term is not null then 'ecds_etos'
        when c.preferred_term is not null then 'snomed_ct'
        when nullif(trim(s.assessment_tool_code), '') is null then 'not_recorded'
        else 'unmatched'
    end as code_description_source
    , s.is_code_approved
    , r.source_file_name as code_reference_file
    , s.person_score
    , try_to_decimal(nullif(trim(s.person_score), ''), 38, 9) as person_score_numeric
    , case
        when nullif(trim(s.person_score), '') is null then 'not_recorded'
        when try_to_decimal(trim(s.person_score), 38, 9) is not null then 'numeric'
        else 'non_numeric_or_out_of_range'
    end as value_parse_status
    , s.validated_at

from {{ ref('stg_sus_ecds_clinical_coded_scored_assessments') }} as s
left join {{ ref('obt_encounter_uec') }} as e
    on s.visit_occurrence_id = e.visit_occurrence_id
left join {{ ref('ecds_measurement_code') }} as r
    on s.assessment_tool_code = r.snomed_code and r.record_type = 'scored_assessment'
left join {{ ref('snomed_concept') }} as c
    on s.assessment_tool_code = c.snomed_code
