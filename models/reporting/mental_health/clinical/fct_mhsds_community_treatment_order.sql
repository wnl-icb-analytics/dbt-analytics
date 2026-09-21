-- A CTO allows a detained patient to leave hospital for treatment in the community
-- under Mental Health Act conditions, with a power of recall to hospital.
-- MHS404 records the order period; MHS405 records recalls separately. An order
-- does not itself establish inpatient occupancy or continuous hospital detention.
with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_mh_act_episode_id', 's.start_date_comm_treat_ord', 'iff(s.uniq_mh_act_episode_id is null or s.start_date_comm_treat_ord is null, s.mhs404_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_mh_act_episode_id is null or s.start_date_comm_treat_ord is null as is_identity_incomplete
    from {{ ref('stg_mhsds_community_treatment_order') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs404_uniq_id desc) = 1
)
select
    s.entity_id as community_treatment_order_id
    , s.mhs404_uniq_id as source_row_id
    , 'MHS404' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_mh_act_episode_id as mental_health_act_period_id
    , s.start_date_comm_treat_ord as order_start_date
    , s.end_date_comm_treat_ord as order_end_date
    , s.expiry_date_comm_treat_ord as order_expiry_date
    , s.comm_treat_ord_end_reason as order_end_reason_code
    , reason_label.description as order_end_reason_description
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
    on upper(trim(s.comm_treat_ord_end_reason::varchar)) = reason_label.code and reason_label.code_set_name = 'community_treatment_order_end_reason'
