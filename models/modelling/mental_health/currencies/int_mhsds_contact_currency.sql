with eligible_contacts as (
    select
        c.*
    from {{ ref('int_mhsds_latest_care_contact') }} as c
    where not exists (
        select 1
        from {{ ref('stg_mhsds_spell') }} as s
        inner join {{ ref('int_mhsds_spell_encounters') }} as e
            on s.uniq_hosp_prov_spell_num = e.encounter_id
        where c.uniq_serv_req_id = s.uniq_serv_req_id
            and c.care_cont_date between s.start_date_hosp_prov_spell and coalesce(e.end_date, current_date)
    )
)

-- uniq_care_cont_id alone is not unique: providers reuse local contact ids
-- across referrals, so the event grain is (uniq_serv_req_id, uniq_care_cont_id)
, latest_diagnosis as (
    select
        c.uniq_serv_req_id
        , c.uniq_care_cont_id
        , d.icd10_3
    from eligible_contacts as c
    left join {{ ref('int_mhsds_currency_primary_diagnosis') }} as d
        on c.uniq_serv_req_id = d.uniq_serv_req_id
        and d.coded_diag_timestamp <= c.care_cont_date
    qualify row_number() over (
        partition by c.uniq_serv_req_id, c.uniq_care_cont_id
        order by d.coded_diag_timestamp desc nulls last
    ) = 1
)

, categorised as (
    select
        c.uniq_care_cont_id
        , c.uniq_serv_req_id
        , c.person_id
        , b.sk_patient_id
        , c.org_id_prov
        , c.care_cont_date
        , c.age_care_cont_date
        -- A missing age falls to adult, which the cascade below then uses to
        -- pick the population group. has_known_age_at_contact keeps that
        -- default visible.
        , coalesce(c.age_care_cont_date < 18, false) as is_cyp
        , c.age_care_cont_date is not null as has_known_age_at_contact
        , c.dm_icb_commissioner
        , c.attend_status
        , c.cons_mechanism_mh
        , c.act_loc_type_code
        , d.icd10_3
        , ig.population_category as diagnosis_category
        , dg.available_to_cyp as diagnosis_available_to_cyp
        , st.serv_team_type_ref_to_mh
        , tt.population_category as team_type_category
        , tg.available_to_cyp as team_type_available_to_cyp
        , tt.setting_group
        , tt.setting_code
        , tt.setting_name
        , r.prim_reason_referral_mh
        , rr.population_category as referral_reason_category
        , rg.available_to_cyp as referral_reason_available_to_cyp
        , {{ mhsds_is_crisis_referral('tt', 'r.clin_resp_priority_type') }}
            as is_crisis_referral
    from eligible_contacts as c
    left join {{ ref('stg_mhsds_bridging') }} as b
        on c.person_id = b.person_id
    left join latest_diagnosis as d
        on c.uniq_serv_req_id = d.uniq_serv_req_id
        and c.uniq_care_cont_id = d.uniq_care_cont_id
    left join {{ ref('nhse_mh_currency_icd10_groups_2627') }} as ig
        on d.icd10_3 between ig.icd10_range_start and ig.icd10_range_end
    left join {{ ref('nhse_mh_currency_population_groups_2627') }} as dg
        on ig.population_category = dg.population_category
    left join {{ ref('stg_mhsds_servicetype') }} as st
        on c.uniq_serv_req_id = st.uniq_serv_req_id
    left join {{ ref('nhse_mh_currency_team_types_2627') }} as tt
        on st.serv_team_type_ref_to_mh = tt.serv_team_type
    left join {{ ref('nhse_mh_currency_population_groups_2627') }} as tg
        on tt.population_category = tg.population_category
    left join {{ ref('stg_mhsds_referral') }} as r
        on c.uniq_serv_req_id = r.uniq_serv_req_id
    left join {{ ref('nhse_mh_currency_referral_reasons_2627') }} as rr
        on r.prim_reason_referral_mh = rr.prim_reason_referral_mh
    left join {{ ref('nhse_mh_currency_population_groups_2627') }} as rg
        on rr.population_category = rg.population_category
)

, classified as (
    select
        *
        , case
            when is_cyp and team_type_category = 'S' then 'mhst'
            when is_cyp and diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'diagnosis'
            when is_cyp and team_type_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'team_type'
            when is_cyp and referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z') then 'referral_reason'
            when is_cyp then 'unclassified'
            when diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'Y', 'Z') then 'diagnosis'
            -- MCS is CYP-only: adult contacts under MHST teams (category S) fall through
            when team_type_category in ('A', 'B', 'C', 'E', 'F', 'Y', 'Z') then 'team_type'
            when referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'Y', 'Z') then 'referral_reason'
            else 'unclassified'
        end as winning_tier
        , case
            when is_cyp and team_type_category = 'S' then 'S'
            when is_cyp and diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z')
                then iff(diagnosis_available_to_cyp, diagnosis_category, 'MCG')
            when is_cyp and team_type_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z')
                then iff(team_type_available_to_cyp, team_type_category, 'MCG')
            when is_cyp and referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'S', 'Y', 'Z')
                then iff(referral_reason_available_to_cyp, referral_reason_category, 'MCG')
            when is_cyp then 'MCG'
            when diagnosis_category in ('A', 'B', 'C', 'E', 'F', 'Y', 'Z') then diagnosis_category
            when team_type_category in ('A', 'B', 'C', 'E', 'F', 'Y', 'Z') then team_type_category
            when referral_reason_category in ('A', 'B', 'C', 'E', 'F', 'Y', 'Z') then referral_reason_category
            else 'MBU'
        end as winning_category
    from categorised
)

, currency_codes as (
    select
        c.*
        , coalesce(pg.currency_group, c.winning_category) as currency_group
    from classified as c
    left join {{ ref('nhse_mh_currency_population_groups_2627') }} as pg
        on c.winning_category = pg.population_category
)

select
    c.uniq_care_cont_id
    , c.uniq_serv_req_id
    , c.person_id
    , c.sk_patient_id
    , c.org_id_prov
    , c.care_cont_date
    , c.age_care_cont_date
    , c.is_cyp
    , c.has_known_age_at_contact
    , c.dm_icb_commissioner
    , coalesce(comm.icb_code, iff(left(c.dm_icb_commissioner, 1) = 'Q', c.dm_icb_commissioner, null)) as commissioner_icb_code
    , c.attend_status
    , c.cons_mechanism_mh
    , c.act_loc_type_code
    , c.icd10_3
    , c.diagnosis_category
    , c.serv_team_type_ref_to_mh
    , c.team_type_category
    , c.setting_group
    , c.setting_code
    , c.setting_name
    , c.prim_reason_referral_mh
    , c.referral_reason_category
    , c.is_crisis_referral
    , c.winning_tier
    , c.currency_group
    , case
        when c.currency_group = 'MCS' then 'MCS99Z'
        when c.currency_group = 'MAZ' then 'MAZ99' || iff(c.setting_group = 'crisis', c.setting_code, 'Z')
        when c.setting_group = 'community' then c.currency_group || '96' || c.setting_code
        when c.setting_group = 'crisis' then c.currency_group || '97' || c.setting_code
        when c.is_crisis_referral then c.currency_group || '97Z'
        else c.currency_group || '96Z'
    end as currency_code
from currency_codes as c
left join {{ ref('wnl_commissioner_icb_lookup') }} as comm
    on c.dm_icb_commissioner = comm.commissioner_code
