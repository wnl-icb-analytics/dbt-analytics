with activity as (
    select * from {{ ref('fct_mhsds_care_activity') }}
)

, professional as (
    select * from {{ ref('dim_mhsds_care_professional_period') }}
)

, relationship as (
    select * from {{ ref('rel_mhsds_care_activity_staff') }}
)

, activity_parent_checks as (
    select
        a.mhs202_uniq_id
        , c.mhs201_uniq_id is not null as is_linked
        , a.person_id is distinct from c.person_id as has_person_mismatch
        , upper(a.org_id_prov) is distinct from upper(c.org_id_prov)
            as has_provider_mismatch
    from {{ ref('stg_mhsds_care_activity') }} as a
    left join {{ ref('stg_mhsds_carecontact') }} as c
        on a.uniq_submission_id = c.uniq_submission_id
        and a.uniq_serv_req_id = c.uniq_serv_req_id
        and a.uniq_care_cont_id = c.uniq_care_cont_id
)

, professionals_per_activity as (
    select
        care_activity_source_record_id
        , count(*) as professional_count
    from relationship
    group by care_activity_source_record_id
)

, accepted_activity_identifiers as (
    select
        org_id_prov
        , uniq_care_act_id
        , min(reporting_period_end_date) as first_reporting_period_end_date
        , max(reporting_period_end_date) as last_reporting_period_end_date
    from {{ ref('stg_mhsds_care_activity') }}
    group by org_id_prov, uniq_care_act_id
)

, accepted_professional_identifiers as (
    select
        org_id_prov
        , uniq_care_prof_local_id
        , min(reporting_period_end_date) as first_reporting_period_end_date
        , max(reporting_period_end_date) as last_reporting_period_end_date
    from {{ ref('stg_mhsds_staff_details') }}
    group by org_id_prov, uniq_care_prof_local_id
)

, missing_relationship_parent_history as (
    select
        r.source_table
        , r.is_care_activity_linked
        , r.is_professional_detail_linked
        , coalesce(
            ai.first_reporting_period_end_date <> r.reporting_period_end_date
            or ai.last_reporting_period_end_date <> r.reporting_period_end_date
            , false
        ) as is_activity_seen_in_another_period
        , coalesce(
            pi.first_reporting_period_end_date <> r.reporting_period_end_date
            or pi.last_reporting_period_end_date <> r.reporting_period_end_date
            , false
        ) as is_professional_seen_in_another_period
    from relationship as r
    left join accepted_activity_identifiers as ai
        on r.uniq_care_act_id = ai.uniq_care_act_id
        and upper(r.provider_organisation_code) = upper(ai.org_id_prov)
    left join accepted_professional_identifiers as pi
        on r.uniq_care_prof_local_id = pi.uniq_care_prof_local_id
        and upper(r.provider_organisation_code) = upper(pi.org_id_prov)
    where not r.is_care_activity_linked
        or not r.is_professional_detail_linked
)

select
    'activity' as profile_area
    , 'rows_by_version' as measure
    , mhsds_version as category
    , count(*) as row_count
from activity
group by mhsds_version

union all

select
    'output'
    , 'row_count'
    , 'care_activity'
    , count(*)
from activity

union all

select
    'output'
    , 'row_count'
    , 'care_professional_period'
    , count(*)
from professional

union all

select
    'output'
    , 'row_count'
    , 'care_activity_staff_relationship'
    , count(*)
from relationship

union all

select
    'activity'
    , 'care_contact_link'
    , iff(is_care_contact_linked, 'linked', 'missing')
    , count(*)
from activity
group by 3

union all

select
    'activity'
    , 'patient_bridge'
    , iff(sk_patient_id is null, 'missing', 'linked')
    , count(*)
from activity
group by 3

union all

select
    'activity'
    , 'clinical_item_count'
    , clinical_item_count::varchar
    , count(*)
from activity
group by clinical_item_count

union all

select
    'activity'
    , 'procedure_label'
    , procedure_label_status
    , count(*)
from activity
group by procedure_label_status

union all

select
    'activity'
    , 'finding_scheme_label'
    , case
        when finding_scheme_code is null then 'code_missing'
        when finding_scheme_description is null then 'code_unmatched'
        else 'labelled'
    end
    , count(*)
from activity
group by 3

union all

select
    'activity'
    , 'finding_label'
    , finding_label_status
    , count(*)
from activity
group by finding_label_status

union all

select
    'activity'
    , 'observation_scheme_label'
    , case
        when observation_scheme_code is null then 'code_missing'
        when observation_scheme_description is null then 'code_unmatched'
        else 'labelled'
    end
    , count(*)
from activity
group by 3

union all

select
    'activity'
    , 'observation_label'
    , observation_label_status
    , count(*)
from activity
group by observation_label_status

union all

select
    'activity'
    , 'unit_of_measurement_label'
    , unit_of_measurement_label_status
    , count(*)
from activity
group by unit_of_measurement_label_status

union all

select
    'activity'
    , 'duration_quality'
    , case
        when clinical_contact_duration_minutes is null then 'missing'
        when clinical_contact_duration_minutes < 0 then 'negative'
        when clinical_contact_duration_minutes > 1440 then 'over_24_hours'
        else 'zero_to_24_hours'
    end
    , count(*)
from activity
group by 3

union all

select
    'activity'
    , 'linked_parent_consistency'
    , case
        when not is_linked then 'not_linked'
        when has_person_mismatch and has_provider_mismatch then 'person_and_provider_mismatch'
        when has_person_mismatch then 'person_mismatch'
        when has_provider_mismatch then 'provider_mismatch'
        else 'consistent'
    end
    , count(*)
from activity_parent_checks
group by 3

union all

select
    'professional'
    , 'rows_by_version'
    , mhsds_version
    , count(*)
from professional
group by mhsds_version

union all

select
    'professional'
    , 'duplicate_source_state'
    , iff(has_duplicate_source_records, 'duplicate_resolved', 'single_source_row')
    , count(*)
from professional
group by 3

union all

select
    'professional'
    , 'staff_group_label'
    , case
        when staff_group_code is null then 'code_missing'
        when staff_group_description is null then 'code_unmatched'
        else 'labelled'
    end
    , count(*)
from professional
group by 3

union all

select
    'professional'
    , 'main_specialty_label'
    , case
        when main_specialty_code is null then 'code_missing'
        when main_specialty_description is null then 'code_unmatched'
        else 'labelled'
    end
    , count(*)
from professional
group by 3

union all

select
    'professional'
    , 'occupation_label'
    , case
        when occupation_code is null then 'code_missing'
        when occupation_description is null then 'code_unmatched'
        when is_occupation_code_currently_valid then 'labelled_current'
        else 'labelled_retired'
    end
    , count(*)
from professional
group by 3

union all

select
    'professional'
    , 'job_role_label'
    , case
        when job_role_code is null then 'code_missing'
        when job_role_description is null then 'code_unmatched'
        else 'labelled'
    end
    , count(*)
from professional
group by 3

union all

select
    'professional'
    , 'registration_body_label'
    , case
        when professional_registration_body_code is null then 'code_missing'
        when professional_registration_body_description is null then 'code_unmatched'
        else 'labelled'
    end
    , count(*)
from professional
group by 3

union all

select
    'relationship'
    , 'source_and_link_state'
    , source_table || ':'
        || iff(is_care_activity_linked, 'activity_linked', 'activity_missing') || ':'
        || iff(is_professional_detail_linked, 'professional_linked', 'professional_missing')
    , count(*)
from relationship
group by source_table, is_care_activity_linked, is_professional_detail_linked

union all

select
    'relationship'
    , 'missing_activity_history'
    , source_table || ':' || iff(
        is_activity_seen_in_another_period
        , 'seen_other_period'
        , 'never_seen'
    )
    , count(*)
from missing_relationship_parent_history
where not is_care_activity_linked
group by source_table, is_activity_seen_in_another_period

union all

select
    'relationship'
    , 'missing_professional_history'
    , source_table || ':' || iff(
        is_professional_seen_in_another_period
        , 'seen_other_period'
        , 'never_seen'
    )
    , count(*)
from missing_relationship_parent_history
where not is_professional_detail_linked
group by source_table, is_professional_seen_in_another_period

union all

select
    'relationship'
    , 'professionals_per_activity'
    , case
        when professional_count = 1 then '1'
        when professional_count between 2 and 5 then '2_to_5'
        when professional_count between 6 and 10 then '6_to_10'
        else 'over_10'
    end
    , count(*)
from professionals_per_activity
group by 3

order by profile_area, measure, category
