select
    {{ dbt_utils.generate_surrogate_key([
        's.org_id_prov',
        's.reporting_period_end_date',
        's.uniq_care_prof_local_id'
    ]) }} as care_professional_period_id
    , 'MHSDS' as source_dataset
    , s.mhs901_uniq_id
    , s.uniq_care_prof_local_id
    , s.care_prof_local_id
    , s.org_id_care_prof_local_id as professional_identifier_organisation_code
    , professional_identifier_organisation.organisation_name
        as professional_identifier_organisation_name
    , s.prof_reg_body_code as professional_registration_body_code
    , registration_body.description as professional_registration_body_description
    , case
        when s.prof_reg_body_code is null then 'code_missing'
        when registration_body.code is null then 'code_unmatched'
        else 'labelled'
    end as professional_registration_body_label_status
    , s.staff_group_code
    , staff_group.description as staff_group_description
    , case
        when s.staff_group_code is null then 'code_missing'
        when staff_group.code is null then 'code_unmatched'
        else 'labelled'
    end as staff_group_label_status
    , s.main_specialty_code
    , main_specialty.description as main_specialty_description
    , case
        when s.main_specialty_code is null then 'code_missing'
        when main_specialty.code is null then 'code_unmatched'
        else 'labelled'
    end as main_specialty_label_status
    , s.occupation_code
    , occupation.occupation_code_name as occupation_description
    , occupation.main_staff_group as occupation_main_staff_group
    , occupation.broad_staff_group as occupation_broad_staff_group
    , occupation.detailed_staff_group as occupation_detailed_staff_group
    , occupation.occupation_level
    , occupation.care_setting_or_specialty as occupation_care_setting_or_specialty
    , occupation.is_currently_valid as is_occupation_code_currently_valid
    , case
        when s.occupation_code is null then 'code_missing'
        when occupation.occupation_code is null then 'code_unmatched'
        when occupation.is_currently_valid then 'labelled_current'
        else 'labelled_retired'
    end as occupation_label_status
    , s.job_role_code
    , job_role.description as job_role_description
    , case
        when s.job_role_code is null then 'code_missing'
        when job_role.code is null then 'code_unmatched'
        else 'labelled'
    end as job_role_label_status
    , s.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , s.source_record_count
    , s.has_duplicate_source_records
    , s.uniq_submission_id
    , s.uniq_month_id
    , s.reporting_period_start_date
    , s.reporting_period_end_date
    , s.mhsds_version
    , s.source_file_received_at
    , s.source_loaded_at
from {{ ref('stg_mhsds_staff_details') }} as s
left join {{ ref('mhsds_care_professional_code_lookup') }} as registration_body
    on upper(trim(s.prof_reg_body_code)) = registration_body.code
    and registration_body.code_set_name = 'professional_registration_body'
left join {{ ref('mhsds_care_professional_code_lookup') }} as staff_group
    on upper(trim(s.staff_group_code)) = staff_group.code
    and staff_group.code_set_name = 'staff_group'
left join {{ ref('mhsds_care_professional_code_lookup') }} as main_specialty
    on upper(trim(s.main_specialty_code)) = main_specialty.code
    and main_specialty.code_set_name = 'main_specialty'
left join {{ ref('nhs_occupation_codes') }} as occupation
    on upper(trim(s.occupation_code)) = occupation.occupation_code
left join {{ ref('mhsds_care_professional_code_lookup') }} as job_role
    on upper(trim(s.job_role_code)) = job_role.code
    and job_role.code_set_name = 'job_role'
left join {{ ref('int_mhsds_organisation') }} as provider
    on upper(s.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as professional_identifier_organisation
    on upper(s.org_id_care_prof_local_id)
        = upper(professional_identifier_organisation.organisation_code)
