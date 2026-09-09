-- Source-quality observations are counts, not exclusion rules or clinical judgements.
with checks as (
    select 'referral' as source_entity, object_construct(
        'rows', count(*),
        'negative_source_age', count_if(age_at_referral < 0),
        'source_age_above_120', count_if(age_at_referral > 120),
        'discharge_before_referral', count_if(service_discharge_date < referral_received_date),
        'letter_before_discharge', count_if(discharge_letter_issued_date < service_discharge_date),
        'unlabelled_provider', count_if(provider_organisation_code is not null and provider_organisation_name is null),
        'unlabelled_referring_organisation', count_if(referring_organisation_code is not null and referring_organisation_name is null),
        'unlabelled_commissioner', count_if(submitted_commissioner_code is not null and submitted_commissioner_name is null),
        'unlabelled_referral_source', count_if(source_of_referral_code is not null and source_of_referral_name is null),
        'unlabelled_referral_reason', count_if(primary_reason_for_referral_code is not null and primary_reason_for_referral_name is null),
        'unlabelled_referring_staff_group', count_if(referring_professional_staff_group_code is not null and referring_professional_staff_group_name is null)
    ) as counts
    from {{ ref('fct_csds_referral') }}
    union all
    select 'contact', object_construct(
        'rows', count(*),
        'negative_source_age', count_if(age_at_contact < 0),
        'duration_above_one_day', count_if(clinical_contact_duration_minutes > 1440),
        'before_clinically_appropriate_date', count_if(care_contact_date < earliest_clinically_appropriate_date),
        'unlabelled_provider', count_if(provider_organisation_code is not null and provider_organisation_name is null),
        'padded_site_organisation', count_if(site_code_status = 'padded_organisation_code'),
        'unrecognised_site', count_if(site_code_status = 'unrecognised'),
        'padded_site_organisation_differs_provider', count_if(site_organisation_code <> upper(trim(provider_organisation_code))),
        'unlabelled_site', count_if(site_code is not null and site_name is null),
        'unlabelled_commissioner', count_if(submitted_commissioner_code is not null and submitted_commissioner_name is null),
        'unlabelled_attendance', count_if(attendance_code is not null and attendance_name is null),
        'unlabelled_source_attendance', count_if(source_attendance_status_code is not null and source_attendance_status_name is null),
        'unlabelled_contact_subject', count_if(care_contact_subject_code is not null and care_contact_subject_name is null),
        'unlabelled_group_therapy', count_if(group_therapy_code is not null and group_therapy_name is null)
    )
    from {{ ref('fct_csds_care_contact') }}
)
select c.source_entity, f.key::varchar as check_name, f.value::number as row_count
from checks as c, lateral flatten(input => c.counts) as f
