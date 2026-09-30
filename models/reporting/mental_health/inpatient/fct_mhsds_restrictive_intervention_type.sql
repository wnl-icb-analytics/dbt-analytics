with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_restrictive_int_type_id', 'iff(s.uniq_restrictive_int_type_id is null, s.mhs515_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_restrictive_int_type_id is null as is_identity_incomplete
    from {{ ref('stg_mhsds_restrictive_intervention_type') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs515_uniq_id desc) = 1
)
select
    s.entity_id as restrictive_intervention_type_id
    , s.mhs515_uniq_id as source_row_id
    , 'MHS515' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_restrictive_int_type_id as intervention_type_source_id
    , s.uniq_restrictive_int_inc_id as incident_source_id
    , s.uniq_hosp_prov_spell_id as hospital_provider_spell_source_record_id
    , s.restrictive_int_type as intervention_type_code
    , s.start_date_restrictive_int_type as intervention_start_date
    , s.start_time_restrictive_int_type as intervention_start_time
    , s.end_date_restrictive_int_type as intervention_end_date
    , s.end_time_restrictive_int_type as intervention_end_time
    , s.restraint_injury_patient as patient_injury_code
    , s.restraint_injury_care_pers as staff_injury_code
    , s.restraint_injury_other_pers as other_person_injury_code
    , type_label.description as intervention_type_description
    , patient_label.description as patient_injury_description
    , staff_label.description as staff_injury_description
    , other_label.description as other_person_injury_description
    , i.restrictive_intervention_incident_id
    , i.restrictive_intervention_incident_id is not null as is_incident_linked
    , s.person_id = i.person_id as is_incident_person_consistent
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.uniq_submission_id as submission_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.effective_from as source_file_received_at
    , s.n_accepted_source_records
    , s.first_submission_period_end_date
    , s.is_identity_incomplete
from selected as s
left join {{ ref('stg_mhsds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('mhsds_domain_code_lookup') }} as type_label
    on upper(trim(s.restrictive_int_type::varchar)) = type_label.code and type_label.code_set_name = 'restrictive_intervention_type'
left join {{ ref('mhsds_domain_code_lookup') }} as patient_label
    on upper(trim(s.restraint_injury_patient::varchar)) = patient_label.code and patient_label.code_set_name = 'restraint_injury'
left join {{ ref('mhsds_domain_code_lookup') }} as staff_label
    on upper(trim(s.restraint_injury_care_pers::varchar)) = staff_label.code and staff_label.code_set_name = 'restraint_injury'
left join {{ ref('mhsds_domain_code_lookup') }} as other_label
    on upper(trim(s.restraint_injury_other_pers::varchar)) = other_label.code and other_label.code_set_name = 'restraint_injury'
left join {{ ref('fct_mhsds_restrictive_intervention_incident') }} as i
    on s.uniq_restrictive_int_inc_id = i.incident_source_id and s.org_id_prov = i.provider_organisation_code
