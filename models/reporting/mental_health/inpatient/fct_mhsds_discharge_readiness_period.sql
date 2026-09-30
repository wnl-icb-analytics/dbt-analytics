with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_hosp_prov_spell_id', 's.start_date_clin_readyfor_disch', 's.clin_readyfor_disch_delay_reason', 'iff(s.uniq_hosp_prov_spell_id is null or s.start_date_clin_readyfor_disch is null or s.clin_readyfor_disch_delay_reason is null, s.mhs518_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_hosp_prov_spell_id is null or s.start_date_clin_readyfor_disch is null or s.clin_readyfor_disch_delay_reason is null as is_identity_incomplete
    from {{ ref('stg_mhsds_discharge_readiness') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs518_uniq_id desc) = 1
)
select
    s.entity_id as discharge_readiness_period_id
    , s.mhs518_uniq_id as source_row_id
    , 'MHS518' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_hosp_prov_spell_id as hospital_provider_spell_source_record_id
    , s.uniq_serv_req_id as referral_source_record_id
    , s.start_date_clin_readyfor_disch as readiness_start_date
    , s.end_date_clin_readyfor_disch as readiness_end_date
    , s.clin_readyfor_disch_delay_reason as delay_reason_code
    , s.attrib_to_indic as delay_attribution_code
    , s.org_id_resp_la_clin_readyfor_disch as responsible_organisation_code
    , reason_label.description as delay_reason_description
    , attribution_label.description as delay_attribution_description
    , responsible.organisation_name as responsible_organisation_name
    , iff(s.end_date_clin_readyfor_disch >= s.start_date_clin_readyfor_disch, datediff(day, s.start_date_clin_readyfor_disch, s.end_date_clin_readyfor_disch), null) as recorded_readiness_interval_days
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
    on upper(trim(s.clin_readyfor_disch_delay_reason::varchar)) = reason_label.code and reason_label.code_set_name = 'discharge_readiness_delay_reason'
left join {{ ref('mhsds_domain_code_lookup') }} as attribution_label
    on upper(trim(s.attrib_to_indic::varchar)) = attribution_label.code and attribution_label.code_set_name = 'discharge_readiness_attribution'
left join {{ ref('int_mhsds_organisation') }} as responsible on upper(s.org_id_resp_la_clin_readyfor_disch) = upper(responsible.organisation_code)
