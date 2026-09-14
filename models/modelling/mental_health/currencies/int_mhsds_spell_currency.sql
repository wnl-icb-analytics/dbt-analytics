-- A person occupies at most one MH bed per night, but spell ids are
-- provider-scoped, so one admission can carry several records: trust mergers
-- re-register long-stay patients under new ids with the original admission
-- date (BEH/C&I -> NLFT), NHS and independent-sector providers both submit
-- the same placement, and shifted-date copies partially overlap. Three rules
-- restore single occupancy:
--   1. one spell per person and admission date (latest submission evidence wins)
--   2. drop spells wholly contained inside a longer spell
--   3. discharge-forward: a later admission ends any spell still open at
--      that date (end_date_source 'superseded')
with deduplicated as (
    select
        s.uniq_hosp_prov_spell_num
        , s.uniq_serv_req_id
        , s.person_id
        , e.sk_patient_id
        , s.org_id_prov
        , s.start_date_hosp_prov_spell
        , e.end_date
        , e.end_date_source
        , s.disch_date_hosp_prov_spell
        , s.age_hosp_start_date
        , s.reporting_period_end_date as last_submission_period_end
        , s.dm_icb_commissioner
    from {{ ref('stg_mhsds_spell') }} as s
    -- inner: every staged spell has an encounters row today, and if the
    -- encounters model ever drops duplicate spells this model must follow
    inner join {{ ref('int_mhsds_spell_encounters') }} as e
        on s.uniq_hosp_prov_spell_num = e.encounter_id
    qualify row_number() over (
        partition by coalesce(s.person_id, s.uniq_hosp_prov_spell_num)
            , s.start_date_hosp_prov_spell
        order by s.reporting_period_end_date desc nulls last
            , s.uniq_hosp_prov_spell_num
    ) = 1
)

, uncontained as (
    select d.*
    from deduplicated as d
    where not exists (
        select 1
        from deduplicated as o
        where o.person_id = d.person_id
            and o.uniq_hosp_prov_spell_num != d.uniq_hosp_prov_spell_num
            and o.start_date_hosp_prov_spell <= d.start_date_hosp_prov_spell
            and coalesce(o.end_date, current_date) >= coalesce(d.end_date, current_date)
            and (o.start_date_hosp_prov_spell < d.start_date_hosp_prov_spell
                or coalesce(o.end_date, current_date) > coalesce(d.end_date, current_date))
    )
)

, base as (
    select
        * exclude (end_date, end_date_source, next_start_date)
        , iff(next_start_date < coalesce(end_date, current_date)
            , next_start_date, end_date) as end_date
        , iff(next_start_date < coalesce(end_date, current_date)
            , 'superseded', end_date_source) as end_date_source
    from (
        select
            u.*
            , lead(start_date_hosp_prov_spell) over (
                partition by coalesce(person_id, uniq_hosp_prov_spell_num)
                order by start_date_hosp_prov_spell, uniq_hosp_prov_spell_num
            ) as next_start_date
        from uncontained as u
    )
)

, latest_ward_stay as (
    select
        uniq_hosp_prov_spell_num
        , mh_admitted_patient_class
    from {{ ref('stg_mhsds_mhs502wardstay') }}
    qualify row_number() over (
        partition by uniq_hosp_prov_spell_num
        order by coalesce(end_date_ward_stay, '9999-12-31'::date) desc
            , start_date_ward_stay desc
    ) = 1
)

, latest_diagnosis as (
    select
        b.uniq_hosp_prov_spell_num
        , d.icd10_3
    from base as b
    -- end_date closes orphaned spells at their last submission evidence, so
    -- later-recorded diagnoses cannot reclassify them; open spells fall to today
    left join {{ ref('int_mhsds_currency_primary_diagnosis') }} as d
        on b.uniq_serv_req_id = d.uniq_serv_req_id
        and d.coded_diag_timestamp <= coalesce(b.end_date, current_date)
    qualify row_number() over (
        partition by b.uniq_hosp_prov_spell_num
        order by d.coded_diag_timestamp desc nulls last
    ) = 1
)

, categorised as (
    select
        b.*
        -- A missing age falls to adult, which the cascade below then uses to
        -- pick the population group. has_known_age_at_admission keeps that
        -- default visible; most spells carry no recorded age.
        , coalesce(b.age_hosp_start_date < 18, false) as is_cyp
        , b.age_hosp_start_date is not null as has_known_age_at_admission
        , d.icd10_3
        , ig.population_category as diagnosis_category
        , dg.available_to_cyp as diagnosis_available_to_cyp
        , w.mh_admitted_patient_class
        , bt.population_category as bed_category
        , bg.available_to_cyp as bed_available_to_cyp
        , bt.setting_code
        , bt.setting_name
        , r.prim_reason_referral_mh
        , rr.population_category as referral_reason_category
        , rg.available_to_cyp as referral_reason_available_to_cyp
        , st.serv_team_type_ref_to_mh
        , tt.population_category as team_type_category
    from base as b
    left join latest_diagnosis as d
        on b.uniq_hosp_prov_spell_num = d.uniq_hosp_prov_spell_num
    left join {{ ref('nhse_mh_currency_icd10_groups_2627') }} as ig
        on d.icd10_3 between ig.icd10_range_start and ig.icd10_range_end
    left join {{ ref('nhse_mh_currency_population_groups_2627') }} as dg
        on ig.population_category = dg.population_category
    left join latest_ward_stay as w
        on b.uniq_hosp_prov_spell_num = w.uniq_hosp_prov_spell_num
    left join {{ ref('nhse_mh_currency_bed_types_2627') }} as bt
        on w.mh_admitted_patient_class = bt.mh_admitted_patient_class
    left join {{ ref('nhse_mh_currency_population_groups_2627') }} as bg
        on bt.population_category = bg.population_category
    left join {{ ref('stg_mhsds_referral') }} as r
        on b.uniq_serv_req_id = r.uniq_serv_req_id
    left join {{ ref('nhse_mh_currency_referral_reasons_2627') }} as rr
        on r.prim_reason_referral_mh = rr.prim_reason_referral_mh
    left join {{ ref('nhse_mh_currency_population_groups_2627') }} as rg
        on rr.population_category = rg.population_category
    left join {{ ref('stg_mhsds_servicetype') }} as st
        on b.uniq_serv_req_id = st.uniq_serv_req_id
    left join {{ ref('nhse_mh_currency_team_types_2627') }} as tt
        on st.serv_team_type_ref_to_mh = tt.serv_team_type
)

, classified as (
    select
        *
        , case
            when is_cyp and diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'diagnosis'
            when is_cyp and bed_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'bed_type'
            when is_cyp and referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'referral_reason'
            when is_cyp then 'unclassified'
            when diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'diagnosis'
            when bed_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'bed_type'
            when referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'referral_reason'
            else 'unclassified'
        end as winning_tier
        , case
            when is_cyp and diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z')
                then iff(diagnosis_available_to_cyp, diagnosis_category, 'MCG')
            when is_cyp and bed_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z')
                then iff(bed_available_to_cyp, bed_category, 'MCG')
            when is_cyp and referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z')
                then iff(referral_reason_available_to_cyp, referral_reason_category, 'MCG')
            when is_cyp then 'MCG'
            when diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then diagnosis_category
            when bed_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then bed_category
            when referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then referral_reason_category
            else 'MBU'
        end as winning_category
    from categorised
)

select
    c.uniq_hosp_prov_spell_num
    , c.uniq_serv_req_id
    , c.person_id
    , c.sk_patient_id
    , c.org_id_prov
    , c.start_date_hosp_prov_spell
    , c.end_date
    , c.end_date_source
    , c.age_hosp_start_date
    , c.is_cyp
    , c.has_known_age_at_admission
    , c.icd10_3
    , c.diagnosis_category
    , c.mh_admitted_patient_class
    , c.bed_category
    , c.setting_code
    , c.setting_name
    , c.prim_reason_referral_mh
    , c.referral_reason_category
    , c.serv_team_type_ref_to_mh
    , c.team_type_category
    , c.last_submission_period_end
    , c.dm_icb_commissioner
    -- MHSDS commissioner code schemes vary by table; resolve to a canonical
    -- ICB via the WNL lookup, passing unknown Q-prefixed (ICB) codes through
    , coalesce(comm.icb_code, iff(left(c.dm_icb_commissioner, 1) = 'Q', c.dm_icb_commissioner, null)) as commissioner_icb_code
    , c.winning_tier
    , coalesce(pg.currency_group, c.winning_category) as currency_group
    , case
        -- NHSE keeps cross-cutting crisis in family 99 even when the
        -- activity is an inpatient spell. The family 99 suffixes A-D name
        -- crisis service settings, not inpatient bed types, so a spell takes
        -- Z; its bed setting stays in setting_code.
        when coalesce(pg.currency_group, c.winning_category) = 'MAZ'
            then 'MAZ99Z'
        else coalesce(pg.currency_group, c.winning_category) || '98' || coalesce(c.setting_code, 'Z')
    end as currency_code
from classified as c
left join {{ ref('nhse_mh_currency_population_groups_2627') }} as pg
    on c.winning_category = pg.population_category
left join {{ ref('wnl_commissioner_icb_lookup') }} as comm
    on c.dm_icb_commissioner = comm.commissioner_code
