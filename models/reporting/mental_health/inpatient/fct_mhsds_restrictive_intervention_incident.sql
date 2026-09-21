with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_restrictive_int_inc_id', 'iff(s.uniq_restrictive_int_inc_id is null, s.mhs505_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_restrictive_int_inc_id is null as is_identity_incomplete
    from {{ ref('stg_mhsds_restrictive_intervention_incident') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs505_uniq_id desc) = 1
)
select
    s.entity_id as restrictive_intervention_incident_id
    , s.mhs505_uniq_id as source_row_id
    , 'MHS505' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_restrictive_int_inc_id as incident_source_id
    , s.uniq_serv_req_id as referral_source_record_id
    , s.uniq_hosp_prov_spell_id as hospital_provider_spell_source_record_id
    , s.start_date_restrictive_int_inc as incident_start_date
    , s.start_time_restrictive_int_inc as incident_start_time
    , s.end_date_restrictive_int_inc as incident_end_date
    , s.end_time_restrictive_int_inc as incident_end_time
    , s.restrictive_int_reason as incident_reason_code
    , s.restrictive_int_pi_review_held_pat as patient_review_held_code
    , s.restrictive_int_pi_review_not_held_reas_pat as patient_review_not_held_reason_code
    , s.restrictive_int_pi_review_held_care_pers as staff_review_held_code
    , reason_label.description as incident_reason_description
    , patient_review_label.description as patient_review_held_description
    , no_review_label.description as patient_review_not_held_reason_description
    , staff_review_label.description as staff_review_held_description
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
left join {{ ref('mhsds_domain_code_lookup') }} as reason_label
    on upper(trim(s.restrictive_int_reason::varchar)) = reason_label.code and reason_label.code_set_name = 'restrictive_intervention_reason'
left join {{ ref('mhsds_domain_code_lookup') }} as patient_review_label
    on upper(trim(s.restrictive_int_pi_review_held_pat::varchar)) = patient_review_label.code and patient_review_label.code_set_name = 'post_incident_review_held'
left join {{ ref('mhsds_domain_code_lookup') }} as no_review_label
    on upper(trim(s.restrictive_int_pi_review_not_held_reas_pat::varchar)) = no_review_label.code and no_review_label.code_set_name = 'post_incident_review_not_held_reason'
left join {{ ref('mhsds_domain_code_lookup') }} as staff_review_label
    on upper(trim(s.restrictive_int_pi_review_held_care_pers::varchar)) = staff_review_label.code and staff_review_label.code_set_name = 'post_incident_review_held'
