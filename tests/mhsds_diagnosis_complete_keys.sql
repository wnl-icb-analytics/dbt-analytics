-- A missing national person ID does not split complete provider-local keys by period.
select count(*) as repeated_complete_keys_without_person
from (
    select source_table, provider_organisation_code,
        coalesce(local_patient_id, referral_source_record_id) as source_parent_id,
        diagnosis_scheme_code, clinical_code, clinical_at, clinical_time_precision
    from {{ ref('int_mhsds_diagnosis') }}
    where person_id is null
        and coalesce(local_patient_id, referral_source_record_id) is not null
        and diagnosis_scheme_code is not null and clinical_code is not null
        and clinical_at is not null
    group by 1, 2, 3, 4, 5, 6, 7
    having count(*) > 1
)
having count(*) > 0
