{{
    config(
        materialized = 'table',
        tags=['mhsds']
    )
}}

with accepted_records as (
    select * from {{ ref('stg_mhsds_primdiag') }}
)

, snomed_to_icd10 as (
    select
        referenced_component_id
        , map_target
    from {{ ref('stg_dictionary_snomed_ref_set_complex_map') }}
    where ref_set_id = '999002271000000101'
        and active = true
    qualify row_number() over (
        partition by referenced_component_id
        order by map_group, map_priority
    ) = 1
)

, deduplicated as (
    select
        d.mhs604_uniq_id
        , d.uniq_serv_req_id
        , d.person_id
        , d.coded_diag_timestamp
        , d.diag_scheme_in_use
        , d.prim_diag
        , d.org_id_prov
        , d.record_number as source_record_number
        , d.row_number as source_row_number
        , d.uniq_submission_id
        , d.reporting_period_end_date
        , d.effective_from
    from accepted_records as d
    where d.coded_diag_timestamp is not null
    qualify row_number() over (
        partition by d.uniq_serv_req_id, d.coded_diag_timestamp
        order by
            d.reporting_period_end_date desc
            , d.effective_from desc nulls last
            , d.uniq_submission_id desc
            , d.record_number
            , d.row_number
            , d.mhs604_uniq_id
    ) = 1
)

, resolved as (
    -- map SNOMED-coded rows after selection: the ordering above reads only
    -- MHS604 columns, and the map holds at most one row per component id,
    -- so joining the survivors cannot change which row wins
    select
        d.*
        , case
            when d.diag_scheme_in_use = '02'
                and left(d.prim_diag, 1) in ('F', 'G', 'Q', 'R') then d.prim_diag
            when d.diag_scheme_in_use = '06' then s.map_target
        end as icd10_code
    from deduplicated as d
    left join snomed_to_icd10 as s
        on to_varchar(d.prim_diag) = to_varchar(s.referenced_component_id)
)

select
    mhs604_uniq_id
    , uniq_serv_req_id
    , person_id
    , coded_diag_timestamp
    , diag_scheme_in_use
    , prim_diag
    , icd10_code
    , left(replace(replace(icd10_code, '.', ''), 'X', '0'), 3) as icd10_3
    , org_id_prov
    , source_record_number
    , source_row_number
    , uniq_submission_id
    , reporting_period_end_date
    , effective_from
from resolved
