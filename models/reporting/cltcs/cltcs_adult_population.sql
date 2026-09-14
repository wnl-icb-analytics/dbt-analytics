{{ config(materialized='table', tags=['cltcs']) }}

/*
Shared adult-registrant population for the C-LTCS sub-score models.

One row per living adult sk_patient_id from dim_person_demographics_basic (PDS as
the source of truth for registrant data), enriched with:
- person_id: OLIDS person key via dim_person_pseudo, resolved to one row per
  sk_patient_id with a latest-registered-record tiebreak.
- neighbourhood_code: C-LTCS local neighbourhood
  (stg_cltcs_emis_cltcs_local_mapping_nh_gp), used as the z-score scaling group in
  the sub-score models. Null for practices outside the Haringey/Camden local mapping.

NOTE: the sk_patient_id <-> person_id resolution here is a local stopgap. It should
be handled centrally (e.g. in dim_person_pseudo or a shared bridge/macro), and it
does NOT handle fragmented records well: it silently keeps one person_id and discards
the others' data, and does nothing for the reverse case (one person_id spanning
multiple sk_patient_ids). See flag-and-exclude in cltcs_patient_list and
aggregate-to-sk_patient_id in int_resource_index_olids_profile for robust approaches.

Downstream: cltcs_score_activation / _coordination / _treatment / _frailty.
*/

with source_pop as (
select
    db.sk_patient_id
    , pp.person_id
    , nh.neighbourhood_code
    , db.practice_code
    , db.practice_name
from {{ ref('dim_person_demographics_basic') }} db -- PDS as the source of truth for registrant data
left join {{ ref('dim_person_pseudo') }} pp
    on db.sk_patient_id = pp.sk_patient_id
left join {{ ref('dim_person_demographics') }} reg
    on pp.person_id = reg.person_id
left join {{ ref('stg_cltcs_emis_cltcs_local_mapping_nh_gp') }} nh
    on db.practice_code = nh.practice_code
where (reg.is_deceased = false or reg.is_deceased is null or db.date_of_death is null) -- living patients only
    and db.date_of_birth < date_trunc('month', dateadd(year, -18, current_date)) -- adults only
    and db.sk_patient_id is not null
    and db.sk_patient_id != '1'
    and pp.person_id is not null
    and nh.neighbourhood_code is not null
    and (reg.is_active=true and flag_current_registered = true) -- must be active registrant
-- Latest-record tiebreak when an sk_patient_id maps to >1 person_id: keep the most
-- recently registered person record (person_id as a stable final tiebreak).
qualify row_number() over (
    partition by db.sk_patient_id
    order by reg.registration_start_date desc nulls last, pp.person_id
) = 1 ),

potentially_fragmented_sk_patient_ids as (
    SELECT
    p.sk_patient_id,
    COUNT(DISTINCT pp.person_id) as person_count,
    ARRAY_AGG(DISTINCT pp.person_id::VARCHAR) as person_ids
    FROM {{ ref('stg_olids_patient') }} p
    JOIN {{ ref('stg_olids_patient_person') }} pp ON p.id = pp.patient_id
    GROUP BY p.sk_patient_id
    HAVING COUNT(DISTINCT pp.person_id) > 1
    ORDER BY person_count DESC
),

potentially_fragmented_person_ids as (
    SELECT
        pp.person_id,
        COUNT(DISTINCT p.sk_patient_id) as patient_count,
        ARRAY_AGG(DISTINCT p.sk_patient_id::VARCHAR) as sk_patient_ids
    FROM {{ ref('stg_olids_patient') }} p
    JOIN {{ ref('stg_olids_patient_person') }} pp ON p.id = pp.patient_id
    GROUP BY pp.person_id
    HAVING COUNT(DISTINCT p.sk_patient_id) > 1
    ORDER BY patient_count DESC
)

select erl.sk_patient_id
    , erl.neighbourhood_code
    , erl.practice_code
    , erl.person_id
    , pc.total_conditions
    , fr.category 
    , ccms.cambridge_comorbidity_score
    , zeroifnull(aea.ae_tot_12mo) as ae_tot_12mo
    , lcs.overall_risk_group
    , rm.unique_active_ingredient_count_12mo
    , frr.latest_frailty_severity
    ,case when erl.sk_patient_id in (select sk_patient_id from potentially_fragmented_sk_patient_ids) then 1 else 0 end as fragmented_sk_patient_id_flag -- poor mapping of multiple person_ids to one sk_patient_id
    ,case when erl.person_id in (select person_id from potentially_fragmented_person_ids) then 1 else 0 end as fragmented_person_id_flag -- poor mapping of multiple sk_patient_ids to one person_id
from source_pop erl
left join {{ ref('dim_person_conditions')}} pc
    on erl.person_id = pc.person_id
left join {{ref('fct_person_sus_uec_recent')}} aea
    on erl.sk_patient_id  = aea.sk_patient_id
left join {{ref('fct_person_sus_apc_recent')}} apca
    on erl.sk_patient_id  = apca.sk_patient_id
left join {{ref('fct_person_medications_recent')}} rm
    on erl.person_id = rm.person_id
left join {{ref('fct_person_efi2')}} fr
    on erl.person_id = fr.person_id
left join {{ref('fct_person_frailty_register')}} frr
    on erl.person_id = frr.person_id
left join {{ref('dim_person_ccms')}} ccms
    on erl.person_id = ccms.person_id
left join {{ref('fct_person_ltc_lcs_risk_summary')}} lcs
    on erl.person_id = lcs.person_id
where
    pc.total_conditions > 2 
    and (
        -- clinical complexity
        ccms.cambridge_comorbidity_score > 1.5
        or lcs.overall_risk_group in ('HRC', 'HR')
        -- frailty
        or fr.category in ('MODERATE FRAILTY', 'SEVERE FRAILTY')
        or (frr.moderate_frailty_count > 0 or frr.severe_frailty_count > 0)
        -- emergency use
        or aea.ae_tot_12mo > 3 -- 4+ AE visits within 12 months
        or apca.acs_nel_12mo > 1 -- 2+ NEL for ASC conditions within 12 months
        )
        
