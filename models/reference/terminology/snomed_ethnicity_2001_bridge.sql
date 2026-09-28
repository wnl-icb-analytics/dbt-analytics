with seed_rows as (
    select
        upper(trim(snomed_code)) as snomed_code,
        upper(trim(subcategory)) as subcategory,
        upper(nullif(trim(bk_ethnicity_code), '')) as seed_bk_ethnicity_code
    from {{ ref('ethnicity_snomed_ONS_NHS') }}
    where nullif(trim(snomed_code), '') is not null
),
candidate_codes as (
    select
        snomed_code,
        case
            when subcategory in ('REFUSED', 'NOT STATED') then 'Z'
            when subcategory in ('RECORDED NOT KNOWN', 'NOT RECORDED', 'UNKNOWN')
                then '99'
            when seed_bk_ethnicity_code = '0' then '99'
            else seed_bk_ethnicity_code
        end as ethnicity_2001_code_candidate
    from seed_rows
),
mapped_codes as (
    select
        snomed_code,
        count(distinct ethnicity_2001_code_candidate) as distinct_ethnicity_2001_code_count,
        max(ethnicity_2001_code_candidate) as ethnicity_2001_code_candidate
    from candidate_codes
    group by snomed_code
),
resolved_codes as (
    select
        snomed_code,
        distinct_ethnicity_2001_code_count,
        case
            when distinct_ethnicity_2001_code_count = 1 then ethnicity_2001_code_candidate
        end as ethnicity_2001_code
    from mapped_codes
)
select
    rc.snomed_code,
    nhs.ethnicity_2001_code,
    nhs.ethnicity_2001_detailed_description,
    nhs.ethnicity_2001_broad_group,
    rc.distinct_ethnicity_2001_code_count,
    case
        when rc.distinct_ethnicity_2001_code_count > 1 then 'ambiguous_seed_mapping'
        when nhs.ethnicity_2001_code is null then 'unmapped_seed_code'
        else 'mapped'
    end as mapping_status
from resolved_codes as rc
left join {{ ref('nhs_ethnicity_2001') }} as nhs
    on rc.ethnicity_2001_code = nhs.ethnicity_2001_code
