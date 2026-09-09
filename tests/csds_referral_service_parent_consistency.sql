-- Keep the source occurrence even when the latest referral has changed person.
select t.source_record_id
from {{ ref('fct_csds_referral_service') }} as t
left join {{ ref('stg_csds_referral_history') }} as r
    on t.referral_source_row_id = r.cyp101_unique_id
    and t.submission_id = r.unique_submission_id
    and t.referral_id = r.unique_service_request_identifier
where t.referral_source_row_id is not null and r.cyp101_unique_id is null
