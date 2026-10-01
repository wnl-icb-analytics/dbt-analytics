-- Identity mirrors the CSDS ETOS onward referral successor key (referral, date and
-- reason), qualified by provider and the time MHSDS also supplies.
with identified as (
    select s.*, {{ dbt_utils.generate_surrogate_key(['s.org_id_prov', 's.uniq_serv_req_id', 's.onward_refer_date', 's.onward_refer_time', 's.onward_refer_reason', 'iff(s.uniq_serv_req_id is null or s.onward_refer_date is null, s.mhs105_uniq_id::varchar, null)']) }} as entity_id
        , s.uniq_serv_req_id is null or s.onward_refer_date is null as is_identity_incomplete
    from {{ ref('stg_mhsds_onward_referral') }} as s
), selected as (
    select *
        , count(*) over (partition by entity_id) as n_accepted_source_records
        , min(reporting_period_end_date) over (partition by entity_id) as first_submission_period_end_date
    from identified
    qualify row_number() over (partition by entity_id order by reporting_period_end_date desc, effective_from desc nulls last, uniq_submission_id desc, row_number desc nulls last, mhs105_uniq_id desc) = 1
)
select
    s.entity_id as source_record_id
    , s.mhs105_uniq_id as source_row_id
    , 'MHS105' as source_table
    , s.person_id
    , b.sk_patient_id
    , s.uniq_serv_req_id as referral_source_record_id
    , r.source_record_id is not null as is_referral_linked
    , case
        when r.source_record_id is null or s.person_id is null or r.person_id is null then null
        else s.person_id = r.person_id
    end as is_referral_person_consistent
    , s.decision_to_refer_date
    , s.decision_to_refer_time
    , s.onward_refer_date as onward_referral_date
    , s.onward_refer_time as onward_referral_time
    , case
        when s.onward_refer_date is null then null
        when s.onward_refer_time is null then s.onward_refer_date::timestamp_ntz
        else timestamp_ntz_from_parts(s.onward_refer_date, s.onward_refer_time)
    end as onward_referral_at
    , case
        when s.onward_refer_date is null then null
        when s.onward_refer_time is null then 'date'
        else 'timestamp'
    end as onward_referral_time_precision
    , s.onward_refer_reason as onward_referral_reason_code
    , reason_label.description as onward_referral_reason_description
    , s.oat_reason as out_of_area_reason_code
    , oat_label.description as out_of_area_reason_description
    , s.org_id_receiving as receiving_organisation_code
    , receiving.organisation_name as receiving_organisation_name
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
left join {{ ref('fct_mhsds_referral') }} as r on s.uniq_serv_req_id = r.source_record_id
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('organisation') }} as receiving
    on s.org_id_receiving = receiving.organisation_code
left join {{ ref('mhsds_referral_code_lookup') }} as reason_label
    on s.onward_refer_reason = reason_label.code and reason_label.code_set_name = 'onward_referral_reason'
left join {{ ref('mhsds_referral_code_lookup') }} as oat_label
    on s.oat_reason = oat_label.code and oat_label.code_set_name = 'out_of_area_referral_reason'
