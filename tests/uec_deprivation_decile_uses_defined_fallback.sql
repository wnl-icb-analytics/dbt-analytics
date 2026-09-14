-- The reporting decile uses an unambiguous IMD 2025 result first, then a valid
-- submitted ECDS decile. This test also covers missing and ambiguous LSOA
-- mappings.
with lsoa_imd_2025 as (
    select
        bridge.old_lsoa_code
        , case
            when count(*) = count(imd.index_of_multiple_deprivation_decile)
              and count(distinct imd.index_of_multiple_deprivation_decile) = 1
                then min(imd.index_of_multiple_deprivation_decile)
          end as unambiguous_imd_2025_decile
    from {{ ref('stg_ukhfd_old_lsoa_to_new_lsoa_map') }} as bridge
    left join {{ ref('stg_reference_imd2025') }} as imd
        on bridge.new_lsoa_code = imd.lsoa_code_2021
    group by bridge.old_lsoa_code
),

comparison as (
    select
        encounter.visit_occurrence_id
        , encounter.deprivation_decile_at_event
        , case
            when lsoa.unambiguous_imd_2025_decile is not null
                then lsoa.unambiguous_imd_2025_decile
            when try_to_number(encounter.imd_at_event) between 1 and 10
                then try_to_number(encounter.imd_at_event)
          end as expected_decile
    from {{ ref('int_sus_uec_encounter') }} as encounter
    left join lsoa_imd_2025 as lsoa
        on encounter.lsoa_11_at_event = lsoa.old_lsoa_code
)

select
    visit_occurrence_id
    , deprivation_decile_at_event
    , expected_decile
from comparison
where not equal_null(deprivation_decile_at_event, expected_decile)
