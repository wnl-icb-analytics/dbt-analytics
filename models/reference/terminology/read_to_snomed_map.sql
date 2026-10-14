with read_v2 as (
    select
        'read_v2' as coding_system,
        read_code,
        term_code as read_term_code,
        term_code = '00' as is_preferred_term,
        snomed_concept_id,
        snomed_description_id,
        is_assured,
        map_effective_date
    from {{ ref('stg_ukhfd_read_v2_to_snomed_map') }}
    qualify map_effective_date = max(map_effective_date) over (partition by read_code, term_code)
        and is_current_map
), ctv3 as (
    select
        'ctv3' as coding_system,
        ctv3_code as read_code,
        ctv3_term_id as read_term_code,
        ctv3_term_type = 'P' as is_preferred_term,
        snomed_concept_id,
        snomed_description_id,
        is_assured,
        map_effective_date
    from {{ ref('stg_ukhfd_ctv3_to_snomed_map') }}
    qualify map_effective_date = max(map_effective_date) over (partition by ctv3_code, ctv3_term_id)
        and is_current_map
)
select * from read_v2
union all
select * from ctv3
