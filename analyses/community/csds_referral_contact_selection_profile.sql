-- Compare receipt-first selection with reporting-period-first selection.
with ranked as (
    select 'referral' as source_entity,
        row_number() over (partition by unique_service_request_identifier
            order by effective_from desc nulls last, unique_submission_id desc, cyp101_unique_id desc) as receipt_rank,
        row_number() over (partition by unique_service_request_identifier
            order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
                unique_submission_id desc, cyp101_unique_id desc) as period_rank
    from {{ ref('stg_csds_referral_history') }}
    union all
    select 'contact',
        row_number() over (partition by unique_service_request_identifier, unique_care_contact_identifier
            order by effective_from desc nulls last, unique_submission_id desc, cyp201_unique_id desc),
        row_number() over (partition by unique_service_request_identifier, unique_care_contact_identifier
            order by reporting_period_end_date desc nulls last, effective_from desc nulls last,
                unique_submission_id desc, cyp201_unique_id desc)
    from {{ ref('stg_csds_care_contact_history') }}
)
select source_entity, count_if(period_rank = 1) as logical_keys,
    count_if(receipt_rank = 1 and period_rank <> 1) as different_selected_rows
from ranked
group by source_entity
