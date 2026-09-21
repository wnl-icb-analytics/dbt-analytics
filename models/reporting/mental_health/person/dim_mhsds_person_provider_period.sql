with ranked as (
    select
        m.*
        , count(*) over (
            partition by person_id, org_id_prov, reporting_period_end_date
        ) as n_source_patient_records
        , count(distinct hash(ethnic_category, gender, gender_id_code, gender_same_at_birth,
            age_rep_period_end, lsoa2011, lsoa2021)) over (
            partition by person_id, org_id_prov, reporting_period_end_date
        ) > 1 as has_conflicting_demographic_records
    from {{ ref('stg_mhsds_mpi_history') }} as m
    where person_id is not null
    qualify row_number() over (
        partition by person_id, org_id_prov, reporting_period_end_date
        order by effective_from desc nulls last, uniq_submission_id desc,
            row_number desc nulls last, mhs001_uniq_id desc
    ) = 1
)
select
    {{ dbt_utils.generate_surrogate_key(['m.person_id', 'm.org_id_prov', 'm.reporting_period_end_date']) }}
        as person_provider_period_id
    , m.person_id
    , b.sk_patient_id
    , m.org_id_prov as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , m.reporting_period_start_date
    , m.reporting_period_end_date
    , m.ethnic_category as ethnicity_2001_code
    , ethnic.ethnicity_2001_detailed_description as ethnicity_2001_description
    , ethnic.ethnicity_2001_broad_group
    , m.gender as legacy_person_stated_gender_code
    , legacy_gender.description as legacy_person_stated_gender_description
    , m.gender_id_code as gender_identity_code
    , gender_identity.description as gender_identity_description
    , m.gender_same_at_birth as gender_same_at_birth_code
    , gender_birth.description as gender_same_at_birth_description
    , m.age_rep_period_start as source_age_at_period_start
    , m.age_rep_period_end as source_age_at_period_end
    , m.lsoa2011 as residence_lsoa_2011
    , m.lsoa2021 as residence_lsoa_2021
    , m.la_district_auth as residence_local_authority_code
    , imd19.imddecile as residence_imd_2019_decile
    , imd25.index_of_multiple_deprivation_decile as residence_imd_2025_decile
    , m.dm_icb_residence_submitted as residence_icb_code
    , residence_icb.organisation_name as residence_icb_name
    , m.dm_sub_icb_residence_submitted as residence_sub_icb_code
    , m.dm_icb_registration_submitted as registration_icb_code
    , registration_icb.organisation_name as registration_icb_name
    , m.dm_sub_icb_registration_submitted as registration_sub_icb_code
    , m.n_source_patient_records
    , m.has_conflicting_demographic_records
    , m.mhs001_uniq_id as source_row_id
    , m.uniq_submission_id as submission_id
    , m.effective_from as source_file_received_at
    , residence_sub_icb.organisation_name as residence_sub_icb_name
    , registration_sub_icb.organisation_name as registration_sub_icb_name
from ranked as m
left join {{ ref('stg_mhsds_bridging') }} as b on m.person_id = b.person_id
left join {{ ref('int_mhsds_organisation') }} as provider on upper(m.org_id_prov) = upper(provider.organisation_code)
left join {{ ref('nhs_ethnicity_2001') }} as ethnic on m.ethnic_category = ethnic.ethnicity_2001_code
left join {{ ref('mhsds_domain_code_lookup') }} as legacy_gender
    on m.gender::varchar = legacy_gender.code and legacy_gender.code_set_name = 'person_stated_gender'
left join {{ ref('mhsds_domain_code_lookup') }} as gender_identity
    on m.gender_id_code::varchar = gender_identity.code and gender_identity.code_set_name = 'gender_identity'
left join {{ ref('mhsds_domain_code_lookup') }} as gender_birth
    on m.gender_same_at_birth::varchar = gender_birth.code and gender_birth.code_set_name = 'gender_same_at_birth'
left join {{ ref('stg_reference_imd2019') }} as imd19 on m.lsoa2011 = imd19.lsoacode
left join {{ ref('stg_reference_imd2025') }} as imd25 on m.lsoa2021 = imd25.lsoa_code_2021
left join {{ ref('int_mhsds_organisation') }} as residence_icb
    on upper(m.dm_icb_residence_submitted) = upper(residence_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as registration_icb
    on upper(m.dm_icb_registration_submitted) = upper(registration_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as residence_sub_icb
    on upper(m.dm_sub_icb_residence_submitted) = upper(residence_sub_icb.organisation_code)
left join {{ ref('int_mhsds_organisation') }} as registration_sub_icb
    on upper(m.dm_sub_icb_registration_submitted) = upper(registration_sub_icb.organisation_code)
