-- Aggregate counts only. A changed source Person_ID does not alone prove a different patient.
with conflicts as (
    select
        c.referral_id
        , c.referral_source_row_id
        , c.submission_id
        , c.person_id
        , c.sk_patient_id
        , c.reporting_period_end_date as contact_reporting_period_end_date
        , c.csds_version as contact_csds_version
        , c.is_submitted_referral_linked
        , c.is_submitted_referral_person_consistent
        , latest.sk_patient_id as latest_referral_sk_patient_id
        , latest.source_row_id as latest_referral_source_row_id
        , latest.reporting_period_end_date as latest_referral_reporting_period_end_date
        , latest.csds_version as latest_referral_csds_version
    from {{ ref('fct_csds_care_contact') }} as c
    inner join {{ ref('fct_csds_referral') }} as latest
        on c.referral_id = latest.source_record_id
    where c.is_referral_person_consistent = false
), historical_people as (
    select h.unique_service_request_identifier as referral_id, h.person_id,
        max(h.reporting_period_end_date) as last_reporting_period_end_date
    from {{ ref('stg_csds_referral_history') }} as h
    inner join (select distinct referral_id, person_id from conflicts) as keys
        on h.unique_service_request_identifier = keys.referral_id
        and h.person_id = keys.person_id
    group by h.unique_service_request_identifier, h.person_id
), compared as (
    select c.*,
        submitted.local_patient_identifier_extended as submitted_local_patient_id,
        latest.local_patient_identifier_extended as latest_local_patient_id,
        submitted.organisation_code_provider as submitted_provider_code,
        latest.organisation_code_provider as latest_provider_code,
        h.last_reporting_period_end_date as person_last_reporting_period_end_date
    from conflicts as c
    left join {{ ref('stg_csds_referral_history') }} as submitted
        on c.referral_source_row_id = submitted.cyp101_unique_id
        and c.submission_id = submitted.unique_submission_id
        and c.referral_id = submitted.unique_service_request_identifier
    left join {{ ref('stg_csds_cyp101referral') }} as latest
        on c.latest_referral_source_row_id = latest.cyp101_unique_id
        and c.referral_id = latest.unique_service_request_identifier
    left join historical_people as h
        on c.referral_id = h.referral_id and c.person_id = h.person_id
)
select
    contact_csds_version
    , latest_referral_csds_version
    , count(*) as conflicting_contacts
    , count(distinct referral_id) as referral_identifiers
    , count(distinct submitted_provider_code) as providers
    , count_if(not is_submitted_referral_linked) as missing_submitted_referral
    , count_if(is_submitted_referral_person_consistent = true) as submitted_person_agrees
    , count_if(is_submitted_referral_person_consistent = false) as submitted_person_conflicts
    , count_if(is_submitted_referral_person_consistent is null) as submitted_person_unknown
    , count_if(sk_patient_id = latest_referral_sk_patient_id) as same_bridged_patient
    , count_if(sk_patient_id is not null and latest_referral_sk_patient_id is not null
        and sk_patient_id <> latest_referral_sk_patient_id) as different_bridged_patient
    , count_if(sk_patient_id is null or latest_referral_sk_patient_id is null) as unknown_bridged_patient
    , count_if(submitted_local_patient_id = latest_local_patient_id) as same_local_patient_identifier
    , count_if(submitted_local_patient_id is not null and latest_local_patient_id is not null
        and submitted_local_patient_id <> latest_local_patient_id) as different_local_patient_identifier
    , count_if(submitted_local_patient_id is null or latest_local_patient_id is null) as unknown_local_patient_identifier
    , count_if(submitted_provider_code <> latest_provider_code) as provider_changed
    , count_if(contact_reporting_period_end_date < latest_referral_reporting_period_end_date) as contact_in_earlier_period
    , count_if(person_last_reporting_period_end_date < latest_referral_reporting_period_end_date) as person_ends_before_latest_period
from compared
group by contact_csds_version, latest_referral_csds_version
order by contact_csds_version, latest_referral_csds_version
