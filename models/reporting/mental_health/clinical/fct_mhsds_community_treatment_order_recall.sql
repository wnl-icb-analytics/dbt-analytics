-- A CTO recall is a return to hospital for treatment while a community treatment
-- order is in force. Each recall period is separate from the underlying order.
with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_mh_act_episode_id', 's.start_date_comm_treat_ord_recall', 's.start_time_comm_treat_ord_recall', 'iff(s.uniq_mh_act_episode_id is null or s.start_date_comm_treat_ord_recall is null, s.mhs405_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_mh_act_episode_id is null or s.start_date_comm_treat_ord_recall is null as is_identity_incomplete
    from {{ ref('stg_mhsds_community_treatment_order_recall') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs405_uniq_id desc) = 1
)
select
    s.entity_id as community_treatment_order_recall_id
    , s.mhs405_uniq_id as source_row_id
    , 'MHS405' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_mh_act_episode_id as mental_health_act_period_id
    , s.start_date_comm_treat_ord_recall as recall_start_date
    , s.start_time_comm_treat_ord_recall as recall_start_time
    , s.end_date_comm_treat_ord_recall as recall_end_date
    , s.end_time_comm_treat_ord_recall as recall_end_time
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
