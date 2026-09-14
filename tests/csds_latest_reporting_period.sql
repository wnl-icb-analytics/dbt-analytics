-- A late refresh of an older month must not replace a newer reported state.
with referral_latest as (
    select unique_service_request_identifier, max(reporting_period_end_date) as latest_period
    from {{ ref('stg_csds_referral_history') }}
    group by unique_service_request_identifier
), contact_latest as (
    select unique_service_request_identifier, unique_care_contact_identifier,
        max(reporting_period_end_date) as latest_period
    from {{ ref('stg_csds_care_contact_history') }}
    group by unique_service_request_identifier, unique_care_contact_identifier
)
select 'referral' as source_entity,
    coalesce(r.unique_service_request_identifier, h.unique_service_request_identifier) as referral_id,
    null::varchar as contact_id,
    r.reporting_period_end_date as selected_period, h.latest_period as expected_period
from {{ ref('stg_csds_cyp101referral') }} as r
full join referral_latest as h using (unique_service_request_identifier)
where r.unique_service_request_identifier is null or h.unique_service_request_identifier is null
    or r.reporting_period_end_date is distinct from h.latest_period
union all
select 'contact', coalesce(c.unique_service_request_identifier, h.unique_service_request_identifier),
    coalesce(c.unique_care_contact_identifier, h.unique_care_contact_identifier),
    c.reporting_period_end_date, h.latest_period
from {{ ref('stg_csds_cyp201carecontact') }} as c
full join contact_latest as h using (unique_service_request_identifier, unique_care_contact_identifier)
where c.unique_care_contact_identifier is null or h.unique_care_contact_identifier is null
    or c.reporting_period_end_date is distinct from h.latest_period
