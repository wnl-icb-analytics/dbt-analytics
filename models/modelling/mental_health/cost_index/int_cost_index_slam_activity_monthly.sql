/*
Patient-level SLAM activity & actual cost — the spine of the Aligning
Resource Index model.

Grain: sk_patient_id x activity_month x commissioner x provider x
registered practice x service hierarchy (service_grouping > service) x POD x
HRG x is_patient_attributable x cost_basis.

Source: stg_lsplcm_latest (latest-submission LSPLCM PLD — actual provider
cost dv_total_cost, NOT national tariff). Covers the WNL footprint
(NCL + NWL merged). Carries history from 2021-04-01 to the last complete
month. The latest submitted month is excluded while it is still in SLAM flex
(see bounds). Rolling windows are applied in reporting facts.

Cost basis: 'actual' = SLAM agreed actual cost. Future sources will carry
'proxy', 'nominal', or 'tariff'.

Service hierarchy:
  service          = governed POD group (stg_reference_pod_group_mapping,
                     maintained in the POD Group Manager app — the same
                     taxonomy as WNL contracting reporting). Tiered join:
                     exact (POD, local POD, description) with equal_null
                     semantics (~96% of window cost), then unambiguous
                     (POD, local POD), then unambiguous POD-only — LSPLCM
                     local descriptions are free-text variants of the
                     LSACM-curated keys. Residual 'Unmapped' ~1.5% of window
                     cost: PODs whose group genuinely depends on a local code
                     the app has not seen (BLOCK / DRUG / OTHER variants).
  service_grouping = Crisis / Community / Planned / Excluded / Unmapped lens
                     over the POD groups (pod_group_service_lens seed), which
                     also carries is_patient_attributable: filter it for
                     per-patient resource cuts (drops CQUIN / transport /
                     block-and-adjustment-dominated groups) while provider
                     totals keep all rows.

Period: dv_financial_year 'YYYYYY' + dv_financial_month (1=April..12=March)
converted to the first of the calendar month. Rows with no parsed financial
period are dropped (period unknown).
*/

with base as (
    select
        s.sk_patient_id
        , date_from_parts(
            left(s.dv_financial_year, 4)::int + iff(s.dv_financial_month >= 10, 1, 0)
            , mod(s.dv_financial_month + 2, 12) + 1
            , 1
          )                                             as activity_month
        , s.commissioner_code
        , s.dv_provider_code                            as provider_code
        -- responsibility (not registration) code is the populated practice
        -- field in current SLAM: gp_practice_code_registration is ~0%,
        -- gp_practice_responsibility_code ~98%.
        , s.gp_practice_responsibility_code             as registered_practice_code
        , nullif(upper(trim(s.point_of_delivery_code)), '')              as pod_code
        , nullif(upper(trim(s.local_point_of_delivery_code)), '')        as local_pod_code
        , nullif(upper(trim(s.local_point_of_delivery_description)), '') as local_pod_description
        , s.tariff_code                                 as hrg_code
        , s.dv_total_cost
        , s.dv_activity_count
    from {{ ref('stg_lsplcm_latest') }} as s
    where s.dv_financial_year is not null
      and s.dv_financial_month between 1 and 12
)

-- Anchor the rolling window on the latest month with material volume, not
-- max(activity_month): a handful of mis-stated rows carry future financial
-- periods (e.g. 2027-03) that would otherwise shrink the window. Months with
-- a real submission have ~1.7M rows; junk/partial months have <1k.
-- Then lag one month: the latest submitted month is still in SLAM flex and
-- comes in ~15% light, so anchor on the last fully-submitted month to give a
-- clean window of *complete* months (re-matures forward each refresh).
, bounds as (
    select dateadd('month', -1, max(activity_month)) as end_month
    from (
        select activity_month
        from base
        group by activity_month
        having count(*) > 100000
    )
)

, windowed as (
    select b.*
    from base as b
    cross join bounds
    where b.activity_month >= '2021-04-01'
      and b.activity_month <= bounds.end_month
)

-- fallback tiers: only usable where the coarser key is unambiguous
, map_pod_local as (
    select pod_code, local_pod_code, min(pod_group) as pod_group
    from {{ ref('stg_reference_pod_group_mapping') }}
    group by pod_code, local_pod_code
    having count(distinct pod_group) = 1
)

, map_pod as (
    select pod_code, min(pod_group) as pod_group
    from {{ ref('stg_reference_pod_group_mapping') }}
    group by pod_code
    having count(distinct pod_group) = 1
)

, mapped as (
    select
        r.*
        , coalesce(m3.pod_group, m2.pod_group, m1.pod_group, 'Unmapped') as pod_group
    from windowed as r
    left join {{ ref('stg_reference_pod_group_mapping') }} as m3
        on  equal_null(r.pod_code, m3.pod_code)
        and equal_null(r.local_pod_code, m3.local_pod_code)
        and equal_null(r.local_pod_description, m3.local_pod_description)
    left join map_pod_local as m2
        on  equal_null(r.pod_code, m2.pod_code)
        and equal_null(r.local_pod_code, m2.local_pod_code)
    left join map_pod as m1
        on r.pod_code = m1.pod_code
)

select
    r.sk_patient_id
    , r.activity_month
    , r.commissioner_code
    , r.provider_code
    , r.registered_practice_code
    , coalesce(l.service_grouping, 'Unmapped')          as service_grouping
    , r.pod_group                                       as service
    , r.pod_code
    , r.hrg_code
    -- Lens coverage is enforced by the relationships test on
    -- stg_reference_pod_group_mapping.pod_group (a new app category fails the
    -- build until the lens seed decides it), so this fallback never fires in
    -- practice; true mirrors the seed's Unmapped row (real patient activity).
    -- Demographics deliberately not carried: cut dimensions come from the
    -- person spine (dim_person_demographics_basic), not SLAM's event fields.
    , coalesce(l.is_patient_attributable, true)         as is_patient_attributable
    , 'actual'                                          as cost_basis
    -- measures
    , sum(r.dv_total_cost)                              as total_cost
    , sum(r.dv_activity_count)                          as total_activity
    , count(*)                                          as line_count
from mapped as r
left join {{ ref('pod_group_service_lens') }} as l
    on l.pod_group = r.pod_group
group by
    r.sk_patient_id
    , r.activity_month
    , r.commissioner_code
    , r.provider_code
    , r.registered_practice_code
    , service_grouping
    , r.pod_group
    , r.pod_code
    , r.hrg_code
    , is_patient_attributable
