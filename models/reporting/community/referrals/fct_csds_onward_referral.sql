select
    {{ dbt_utils.generate_surrogate_key(['s.unique_service_request_identifier', 's.onward_referral_date', 's.onward_referral_reason']) }} as source_record_id
    , s.cyp105_unique_id as source_row_id
    , s.person_id
    , b.sk_patient_id
    , s.unique_service_request_identifier as referral_source_record_id
    , p.source_record_id is not null as is_referral_linked
    , case
        when p.source_record_id is null or s.person_id is null or p.person_id is null then null
        else s.person_id = p.person_id
    end as is_referral_person_consistent
    , s.onward_referral_date
    , s.onward_referral_reason as onward_referral_reason_code
    , reason_label.description as onward_referral_reason_name
    , s.organisation_identifier_receiving as receiving_organisation_code
    , receiving.organisation_name as receiving_organisation_name
    , {{ is_wnl_icb_code(['r.dm_icb_commissioner', 'r.dm_sub_icb_commissioner', 'r.organisation_code_code_of_commissioner']) }}
        as is_wnl_commissioner
    , s.organisation_code_provider as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.unique_submission_id as submission_id
    , s.reporting_period_start_date::date as reporting_period_start_date
    , s.reporting_period_end_date::date as reporting_period_end_date
    , s.effective_from as source_file_received_at
from {{ ref('stg_csds_onward_referral') }} as s
left join {{ ref('stg_csds_referral_history') }} as r
    on s.unique_submission_id = r.unique_submission_id
    and s.unique_service_request_identifier = r.unique_service_request_identifier
left join {{ ref('fct_csds_referral') }} as p on s.unique_service_request_identifier = p.source_record_id
left join {{ ref('stg_csds_bridging') }} as b on s.person_id = b.person_id
left join {{ ref('organisation') }} as provider
    on upper(trim(s.organisation_code_provider)) = provider.organisation_code
left join {{ ref('organisation') }} as receiving
    on s.organisation_identifier_receiving = receiving.organisation_code
left join {{ ref('csds_referral_code_lookup') }} as reason_label
    on reason_label.code_set_name = 'onward_referral_reason'
    and s.onward_referral_reason = reason_label.code
