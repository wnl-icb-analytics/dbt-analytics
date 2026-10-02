select
    {{ dbt_utils.generate_surrogate_key(["'ecds_observation'", 's.visit_occurrence_id', 's.source_sequence']) }} as observation_id
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
    , s.observation_code
    , coalesce(r.preferred_term, c.preferred_term) as observation_name
    , r.measurement_category
    , r.ecds_group1
    , case
        when r.preferred_term is not null then 'ecds_etos'
        when c.preferred_term is not null then 'snomed_ct'
        when nullif(trim(s.observation_code), '') is null then 'not_recorded'
        else 'unmatched'
    end as code_description_source
    , s.is_code_approved
    , r.source_file_name as code_reference_file
    , s.observation_value
    , try_to_decimal(nullif(trim(s.observation_value), ''), 38, 9) as observation_value_numeric
    , {{ numeric_parse_status("nullif(trim(s.observation_value), '')") }} as value_parse_status
    , s.observed_at
    , s.ucum_unit_code
    , u.unit_symbol as resolved_unit_symbol
    , u.definition_source as unit_definition_source
    , u.description as unit_name
    , u.quantity_name as unit_quantity
    , case
        when nullif(trim(s.ucum_unit_code), '') is null then 'not_recorded'
        when u.code is not null then u.match_type
        else 'unmatched'
    end as unit_match_status
    , response.response_name as categorical_value_name
from {{ ref('stg_sus_ecds_clinical_coded_observations') }} as s
left join {{ ref('obt_encounter_uec') }} as e
    on s.visit_occurrence_id = e.visit_occurrence_id
left join {{ ref('ecds_measurement_code') }} as r
    on s.observation_code = r.snomed_code and r.record_type = 'observation'
left join {{ ref('snomed_concept') }} as c
    on s.observation_code = c.snomed_code
left join {{ ref('clinical_unit_of_measurement') }} as u
    on trim(s.ucum_unit_code) = u.code
left join {{ ref('ecds_observation_response') }} as response
    on s.observation_code = response.observation_code
    and trim(s.observation_value) = response.response_value
