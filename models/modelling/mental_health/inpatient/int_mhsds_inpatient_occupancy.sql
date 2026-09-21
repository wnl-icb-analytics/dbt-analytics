with spells as (
    select
        s.*
        , coalesce(s.disch_date_hosp_prov_spell, current_date) as fallback_end
        , max(s.reporting_period_end_date) over () as latest_period_end
    from {{ ref('stg_mhsds_spell') }} as s
)

, classified as (
    select
        spells.*
        , case
            when disch_date_hosp_prov_spell is not null then 'discharged'
            when reporting_period_end_date >= dateadd(month, -2, latest_period_end) then 'open'
            else 'last_submission'
        end as end_date_source
        , case
            when disch_date_hosp_prov_spell is not null then disch_date_hosp_prov_spell
            when reporting_period_end_date >= dateadd(month, -2, latest_period_end) then null
            else greatest(reporting_period_end_date, dateadd(day, 1, start_date_hosp_prov_spell))
        end as end_date
    from spells
)

-- Single occupancy rule 1: one spell per person and admission date, keeping
-- the record with the latest submission evidence.
, deduplicated as (
    select *
    from classified
    qualify row_number() over (
        partition by coalesce(person_id, uniq_hosp_prov_spell_num)
            , start_date_hosp_prov_spell
        order by reporting_period_end_date desc nulls last
            , uniq_hosp_prov_spell_num
    ) = 1
)

-- Rule 2: drop spells wholly contained inside a longer spell for the same
-- person (strict containment on at least one side, so exact duplicates on
-- both dates are not mutually eliminated).
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

-- Rule 3: discharge-forward supersession. A later admission ends any spell
-- still open at that date (end_date_source 'superseded').
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

select
    s.uniq_hosp_prov_spell_num
    , s.uniq_hosp_prov_spell_id
    , s.uniq_serv_req_id
    , s.person_id
    , s.org_id_prov
    , s.start_date_hosp_prov_spell
    , s.end_date
    , s.end_date_source
    , s.disch_date_hosp_prov_spell
    , s.reporting_period_end_date
    , s.latest_period_end
    , s.age_hosp_start_date
    , s.dm_icb_commissioner
    , s.source_adm_mh_hosp_prov_spell
    , s.meth_adm_mh_hosp_prov_spell
    , b.sk_patient_id
    , current_date as calculation_date
from base as s
left join {{ ref('stg_mhsds_bridging') }} as b
    on s.person_id = b.person_id
