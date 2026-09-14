/*
Mental Health Services encounters (spells) from MHSDS

Clinical Purpose:
- Establishing use of mental health inpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

End-date derivation (end_date_source):
- 'discharged'      - a real discharge date was recorded.
- 'open'            - no discharge date, but the spell appears in an active
                      submission within 2 reporting periods of the latest period
                      in the dataset, so it is treated as genuinely open and
                      accrues to current_date. MHSDS requires providers to
                      resubmit every active spell each period.
- 'last_submission' - no discharge date and the spell stopped appearing in
                      submissions (orphaned record, e.g. system cutovers or the
                      2024 BEH/C&I -> NLFT merger); closed at the end of the last
                      reporting period it appeared in. ~95% of undischarged
                      spells fall here; treating them as open would accrue
                      thousands of phantom bed days each.
- 'superseded'      - a later admission for the same person started before this
                      spell's derived end date; the spell is closed at that
                      admission date (see single occupancy below).
Estimated discharge dates are NOT used for closure: 81% of them post-date the
last submission evidence for the spell.

Single occupancy:
A person occupies at most one MH bed per night, but spell ids are
provider-scoped, so one admission can carry several records: trust mergers
re-register long-stay patients under new ids with the original admission date
(BEH/C&I -> NLFT), NHS and independent-sector providers both submit the same
placement, and shifted-date copies partially overlap. Three rules restore
single occupancy (together they removed ~13% of bed-day history):
1. one spell per person and admission date (latest submission evidence wins)
2. drop spells wholly contained inside a longer spell
3. discharge-forward: a later admission ends any spell still open at that
   date (end_date_source 'superseded')

Proxy costing:
- Bed-day prices from the NHSE 26/27 indicative price schedule (24/25 NCC),
  MBU98 Other/Unclassified inpatient family, split by care setting where the
  spell's MHSDS ward stays identify one (dominant setting by bed days):
  Acute & PICU (MBU98A), Rehab (MBU98B), Specialist (MBU98C). Forensic (MBU98D)
  is specialised/out of NCC scope and unclassified spells have no usable ward
  stay data; both fall back to the MBU98Z price.
- Adjusted by the provider's 26/27 Market Forces Factor (1.0 where unknown).
- Rebased from 26/27 prices to each bed day's own fiscal year with the GDP
  deflator (uk_cost_indices), splitting spells across fiscal years. Years
  outside the index's coverage are clamped to its earliest/latest rows.
*/

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

-- Dominant care setting per spell from ward stays, weighted by bed days.
-- mh_admitted_patient_class codes take precedence; bed type names catch rows
-- where only the name is populated (case varies across submissions).
, ward_settings as (
    select
        uniq_hosp_prov_spell_num
        , case
            when mh_admitted_patient_class in ('206', '207', '208', '303', '304')
                or hospital_bed_type_name ilike '%secure%' then 'forensic'
            when mh_admitted_patient_class in ('200', '201', '202', '205', '309')
                or hospital_bed_type_name ilike '%acute%'
                or hospital_bed_type_name ilike '%intensive care%' then 'acute_picu'
            when mh_admitted_patient_class = '212'
                or hospital_bed_type_name ilike '%rehab%' then 'rehab'
            when mh_admitted_patient_class is not null
                or hospital_bed_type_name is not null then 'specialist'
        end as setting_key
        , sum(greatest(datediff(day, start_date_ward_stay,
            coalesce(end_date_ward_stay, current_date)), 1)) as bed_days
    from {{ ref('stg_mhsds_mhs502wardstay') }}
    where mh_admitted_patient_class is not null
        or hospital_bed_type_name is not null
    group by 1, 2
    qualify row_number() over (
        partition by uniq_hosp_prov_spell_num
        order by bed_days desc, setting_key
    ) = 1
)

-- Fiscal-year ranges for deflation; the first and last rows are extended so
-- activity outside the index's coverage clamps to the nearest available year.
, fiscal_years as (
    select
        fiscal_year_start
        , gdp_deflator
        , case when fiscal_year_start = min(fiscal_year_start) over ()
            then '1900-01-01'::date
            else date_from_parts(fiscal_year_start, 4, 1)
        end as fy_range_start
        , case when fiscal_year_start = max(fiscal_year_start) over ()
            then '2099-12-31'::date
            else date_from_parts(fiscal_year_start + 1, 3, 31)
        end as fy_range_end
    from {{ ref('uk_cost_indices') }}
)

-- Both lookups are aggregated so they always return exactly one row. Bare
-- selects filtered on a literal would return zero rows if a seed refresh ever
-- dropped the row, and the cross joins below would then collapse `costed` for
-- every spell, silently zeroing the whole model's proxy_cost.
, price_base_deflator as (
    -- Deflator for the price schedule's own fiscal year (26/27)
    select max(gdp_deflator) as base_gdp_deflator
    from {{ ref('uk_cost_indices') }}
    where fiscal_year_start = 2026
)

, unclassified_price as (
    select max(bed_day_price_gbp) as fallback_price_gbp
    from {{ ref('nhse_mh_bed_day_prices_2627') }}
    where setting_key = 'unclassified'
)

-- One row per spell x fiscal year it spans; bed days (nights) attributed to
-- the fiscal year in which each night starts, so they sum to the spell duration.
, costed as (
    select
        c.uniq_hosp_prov_spell_num
        , sum(
            datediff(day
                , greatest(c.start_date_hosp_prov_spell, fy.fy_range_start)
                , least(coalesce(c.end_date, current_date), dateadd(day, 1, fy.fy_range_end))
            )
            * coalesce(p.bed_day_price_gbp, up.fallback_price_gbp)
            * coalesce(mff.mff_factor, 1.0)
            * fy.gdp_deflator / pb.base_gdp_deflator
        ) as proxy_cost
    from base as c
    cross join price_base_deflator as pb
    cross join unclassified_price as up
    join fiscal_years as fy
        on fy.fy_range_start <= coalesce(c.end_date, current_date)
        and fy.fy_range_end >= c.start_date_hosp_prov_spell
        and datediff(day
            , greatest(c.start_date_hosp_prov_spell, fy.fy_range_start)
            , least(coalesce(c.end_date, current_date), dateadd(day, 1, fy.fy_range_end))
        ) > 0
    left join ward_settings as ws
        on c.uniq_hosp_prov_spell_num = ws.uniq_hosp_prov_spell_num
    left join {{ ref('nhse_mh_bed_day_prices_2627') }} as p
        on coalesce(ws.setting_key, 'unclassified') = p.setting_key
    left join {{ ref('provider_market_forces_factor_2026_27') }} as mff
        on c.org_id_prov = mff.provider_code
    group by 1
)

select
    c.uniq_hosp_prov_spell_num as encounter_id
    , b.sk_patient_id
    , c.org_id_prov
    , c.start_date_hosp_prov_spell as start_date
    , c.source_adm_mh_hosp_prov_spell as admission_source_code
    , s.source_of_admission_name as admission_source
    , c.meth_adm_mh_hosp_prov_spell as admission_method_code
    , m.admission_method_name as admission_method
    , c.end_date
    , c.end_date_source
    , coalesce(ws.setting_key, 'unclassified') as care_setting
    , datediff(day, c.start_date_hosp_prov_spell, coalesce(c.end_date, current_date)) as duration_to_date
    , coalesce(costed.proxy_cost, 0) as proxy_cost
    , 'MHSDS' as source
from
    base as c
left join
    {{ ref('stg_mhsds_bridging') }} as b
    on c.person_id = b.person_id
left join
    ward_settings as ws
    on c.uniq_hosp_prov_spell_num = ws.uniq_hosp_prov_spell_num
left join
    costed
    on c.uniq_hosp_prov_spell_num = costed.uniq_hosp_prov_spell_num
left join
    {{ ref('stg_dictionary_ip_sourceofadmissions') }} as s
    on c.source_adm_mh_hosp_prov_spell = s.bk_source_of_admission_code
left join
    {{ ref('stg_dictionary_ip_admissionmethods') }} as m
    on c.meth_adm_mh_hosp_prov_spell = m.bk_admission_method_code
