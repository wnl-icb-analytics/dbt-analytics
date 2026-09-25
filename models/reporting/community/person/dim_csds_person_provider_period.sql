-- The newest CYP001 record for each person, provider and reporting period. A
-- provider can submit two local patient records for one person in a month;
-- both are counted and conflicting demographics flagged.
with ranked as (
    select
        m.*
        , count(*) over (
            partition by person_id, organisation_code_provider, reporting_period_end_date
        ) as n_source_patient_records
        , count(distinct hash(ethnic_category, person_stated_gender_code, ic_age_of_patient_at_rp_end,
            lower_super_output_area_residence, lower_super_output_area_residence_2011, person_death_date)) over (
            partition by person_id, organisation_code_provider, reporting_period_end_date
        ) > 1 as has_conflicting_demographic_records
    from {{ ref('stg_csds_mpi_history') }} as m
    where person_id is not null
    qualify row_number() over (
        partition by person_id, organisation_code_provider, reporting_period_end_date
        order by effective_from desc nulls last, unique_submission_id::number desc, cyp001_unique_id::number desc
    ) = 1
)

, lsoa_2021 as (
    select lsoa21_cd, lad26_cd, lad26_nm
    from {{ ref('stg_reference_geo_lsoa21_sicbl26_icb26_nhser26_lad26') }}
)

-- Some providers put retired 2011 codes in the 2021 LSOA field. Bridge each to
-- its 2021 successor. The UKHFD map gives one best-fit successor for almost every
-- code; the few with several keep only the local authority, stable across splits.
, lsoa_2011_bridge as (
    select
        b.old_lsoa_code as lsoa11_cd
        , iff(count(distinct b.new_lsoa_code) = 1, min(b.new_lsoa_code), null) as lsoa21_cd
        , min(l.lad26_cd) as lad26_cd
        , min(l.lad26_nm) as lad26_nm
    from {{ ref('stg_ukhfd_old_lsoa_to_new_lsoa_map') }} as b
    inner join lsoa_2021 as l on b.new_lsoa_code = l.lsoa21_cd
    group by b.old_lsoa_code
)

, resolved as (
    select
        ranked.*
        , coalesce(lad.lsoa21_cd, bridge.lsoa21_cd) as resolved_lsoa21_cd
        , case
            when lad.lsoa21_cd is not null then 'as_supplied'
            when bridge.lsoa21_cd is not null then 'bridged_from_2011'
        end as lsoa_resolution
        , coalesce(lad.lad26_cd, bridge.lad26_cd) as lad26_cd
        , coalesce(lad.lad26_nm, bridge.lad26_nm) as lad26_nm
        , case
            when lad.lad26_nm is not null then 'lsoa_2021'
            when bridge.lad26_nm is not null then 'lsoa_2011_in_2021_field'
        end as local_authority_basis
    from ranked
    left join lsoa_2021 as lad on ranked.lower_super_output_area_residence = lad.lsoa21_cd
    left join lsoa_2011_bridge as bridge
        on lad.lsoa21_cd is null and ranked.lower_super_output_area_residence = bridge.lsoa11_cd
)

select
    {{ dbt_utils.generate_surrogate_key(['m.person_id', 'm.organisation_code_provider', 'm.reporting_period_end_date::date']) }}
        as person_provider_period_id
    , m.person_id
    , b.sk_patient_id
    , m.organisation_code_provider as provider_organisation_code
    , provider.organisation_name as provider_organisation_name
    , m.reporting_period_start_date::date as reporting_period_start_date
    , m.reporting_period_end_date::date as reporting_period_end_date
    , m.ic_age_of_patient_at_rp_end as source_age_at_period_end
    , m.person_stated_gender_code
    , gender.description as person_stated_gender_name
    , m.ethnic_category as ethnicity_2001_code
    , ethnic.ethnicity_2001_detailed_description as ethnicity_2001_description
    , ethnic.ethnicity_2001_broad_group
    , m.lower_super_output_area_residence as residence_lsoa_2021_code
    , m.resolved_lsoa21_cd as residence_lsoa_2021_resolved_code
    , m.lsoa_resolution as residence_lsoa_2021_resolution
    , m.lower_super_output_area_residence_2011 as residence_lsoa_2011_code
    , m.lad26_cd as residence_local_authority_code
    , m.lad26_nm as residence_local_authority_name
    , m.local_authority_basis as residence_local_authority_basis
    , m.local_authority_district_unitary_authority as submitted_residence_local_authority_code
    , imd19.imddecile as residence_imd_2019_decile
    , imd25.index_of_multiple_deprivation_decile as residence_imd_2025_decile
    , m.organisation_identifier_icb_of_residence as residence_icb_code
    , residence_icb.organisation_name as residence_icb_name
    , m.organisation_identifier_sub_icb_location_of_residence as residence_sub_icb_code
    -- the derived residence fields start in July 2022; the submitted sub-ICB covers all years
    , {{ is_wnl_icb_code(['m.organisation_identifier_icb_of_residence', 'm.organisation_identifier_sub_icb_location_of_residence', 'm.dm_icb_residence_submitted', 'm.dm_sub_icb_residence_submitted']) }}
        as is_wnl_resident
    , residence_sub_icb.organisation_name as residence_sub_icb_name
    , m.person_death_date::date as person_death_date
    , case upper(trim(m.looked_after_child_indicator)) when 'Y' then true when 'N' then false end
        as is_looked_after_child
    , case upper(trim(m.safeguarding_vulnerability_factors_indicator)) when 'Y' then true when 'N' then false end
        as has_safeguarding_vulnerability_factors
    , m.n_source_patient_records
    , m.has_conflicting_demographic_records
    , m.cyp001_unique_id as source_row_id
    , m.unique_submission_id as submission_id
    , m.effective_from as source_file_received_at
from resolved as m
left join {{ ref('stg_csds_bridging') }} as b on m.person_id = b.person_id
left join {{ ref('organisation') }} as provider
    on upper(trim(m.organisation_code_provider)) = provider.organisation_code
left join {{ ref('mhsds_domain_code_lookup') }} as gender
    on gender.code_set_name = 'person_stated_gender' and upper(trim(m.person_stated_gender_code)) = upper(gender.code)
left join {{ ref('nhs_ethnicity_2001') }} as ethnic on trim(m.ethnic_category) = ethnic.ethnicity_2001_code
left join {{ ref('stg_reference_imd2019') }} as imd19 on m.lower_super_output_area_residence_2011 = imd19.lsoacode
left join {{ ref('stg_reference_imd2025') }} as imd25 on m.resolved_lsoa21_cd = imd25.lsoa_code_2021
left join {{ ref('organisation') }} as residence_icb
    on upper(trim(m.organisation_identifier_icb_of_residence)) = residence_icb.organisation_code
left join {{ ref('organisation') }} as residence_sub_icb
    on upper(trim(m.organisation_identifier_sub_icb_location_of_residence)) = residence_sub_icb.organisation_code
