/*
Person-level cost and factor profile for supplementary resource-to-need
outputs.

Grain: one person with WNL registration exposure in the latest 12-month SLAM
window. Costs include patient-attributable SLAM actuals and MHSDS/CSDS proxy
costs. EPD is excluded because its coverage does not reach the current window.

Two complete splits of total_cost_12m, each summing back to it:
  by feed    - slam_cost_12m + mhsds_proxy_cost_12m + csds_proxy_cost_12m
  by service - crisis + planned + community + mental_health + unmapped
The mh_* and csds_* columns break the mental health and community lines down
further and are not additive with the five service lines.

Use additive cost and exposure columns for aggregate rates:
sum(total_cost_12m) / sum(weighted_person_years). Do not average person rates.
*/

{{
    config(
        materialized='table',
        meta={
            'custom_message': 'LTC columns are derived from OLIDS - for secondary use, INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED ON person_id to apply National Data Opt-Out and Type 1 opt-out filtering.'
        }
    )
}}

with bounds as (
    select
        max(activity_month) as window_end_month,
        dateadd(month, -11, max(activity_month)) as window_start_month
    from {{ ref('fct_person_cost_index_monthly') }}
    where cost_source = 'SLAM'
),

cost_12m as (
    select
        c.sk_patient_id,
        sum(c.total_cost) as total_cost_12m,
        sum(iff(c.cost_source = 'SLAM', c.total_cost, 0)) as slam_cost_12m,
        sum(iff(c.cost_source = 'MHSDS', c.total_cost, 0)) as mhsds_proxy_cost_12m,
        sum(iff(c.cost_source = 'CSDS', c.total_cost, 0)) as csds_proxy_cost_12m,
        sum(iff(c.service_grouping = 'Crisis', c.total_cost, 0)) as crisis_cost_12m,
        sum(iff(c.service_grouping = 'Planned', c.total_cost, 0)) as planned_cost_12m,
        sum(iff(c.service_grouping = 'Community', c.total_cost, 0)) as community_cost_12m,
        sum(iff(
            c.service_grouping = 'Mental Health',
            c.total_cost,
            0
        )) as mental_health_cost_12m,
        sum(iff(
            coalesce(c.service_grouping, 'Unmapped') = 'Unmapped',
            c.total_cost,
            0
        )) as unmapped_cost_12m,
        sum(iff(
            c.cost_source = 'MHSDS' and c.service = 'MH Inpatient',
            c.total_cost,
            0
        )) as mh_inpatient_cost_12m,
        sum(iff(
            c.cost_source = 'MHSDS' and c.service = 'MH Crisis Contact',
            c.total_cost,
            0
        )) as mh_crisis_cost_12m,
        sum(iff(
            c.cost_source = 'MHSDS' and c.service = 'MH Community Contact',
            c.total_cost,
            0
        )) as mh_community_cost_12m,
        sum(iff(
            c.cost_source = 'SLAM' and c.service_grouping = 'Crisis',
            c.total_activity,
            0
        )) as crisis_activity_12m,
        sum(iff(
            c.cost_source = 'MHSDS' and c.activity_unit = 'bed_day',
            c.total_activity,
            0
        )) as mh_bed_days_12m,
        sum(iff(
            c.cost_source = 'MHSDS' and c.service = 'MH Crisis Contact',
            c.total_activity,
            0
        )) as mh_crisis_contacts_12m,
        sum(iff(
            c.cost_source = 'MHSDS' and c.service = 'MH Community Contact',
            c.total_activity,
            0
        )) as mh_community_contacts_12m,
        sum(iff(c.cost_source = 'CSDS', c.total_activity, 0)) as csds_contacts_12m
    from {{ ref('fct_person_cost_index_monthly') }} as c
    cross join bounds as b
    where c.activity_month between b.window_start_month and b.window_end_month
      and c.is_patient_attributable
      and c.sk_patient_id is not null
      and (
          (c.cost_source = 'SLAM' and c.cost_basis = 'actual')
          or (c.cost_source in ('MHSDS', 'CSDS') and c.cost_basis = 'proxy')
      )
    group by c.sk_patient_id
),

profile as (
    select
        p.sk_patient_id,
        p.age,
        p.age_band,
        case
            when p.age < 65 then '<65'
            when p.age >= 65 then '65+'
            else 'Not recorded'
        end as factor_age_group,
        case
            when p.gender in ('Female', 'Male') then p.gender
            else 'Not recorded'
        end as gender,
        case upper(coalesce(p.ethnicity, ''))
            when 'WHITE' then 'White'
            when 'ASIAN' then 'Asian'
            when 'BLACK' then 'Black'
            when 'MIXED' then 'Mixed'
            when 'OTHER' then 'Other'
            else 'Not recorded'
        end as ethnicity_group,
        case
            when upper(coalesce(p.ethnicity, '')) = 'WHITE' then 'White'
            when upper(coalesce(p.ethnicity, '')) in ('ASIAN', 'BLACK', 'MIXED', 'OTHER')
                then 'Non-White'
            else 'Not recorded'
        end as factor_ethnicity_group,
        case
            when upper(coalesce(p.ethnicity, '')) in ('WHITE', 'ASIAN', 'BLACK', 'MIXED', 'OTHER')
                then initcap(p.ethnicity) || ' - ' || coalesce(d.ethnicity_detail, 'Unspecified')
            else 'Not recorded'
        end as ethnicity_detail,
        p.residence_imd_decile,
        p.residence_imd_quintile,
        case
            when p.residence_imd_quintile = 1 then 'Most deprived (Q1)'
            when p.residence_imd_quintile between 2 and 5 then 'Q2-Q5'
            else 'Not recorded'
        end as factor_deprivation_group,
        p.residence_borough,
        p.residence_ward_2025_name,
        p.residence_lsoa_2021_code,
        p.practice_code,
        p.registered_borough,
        p.registered_neighbourhood_name,
        p.registered_sub_icb_code,
        p.registered_sub_icb_name,
        p.months_registered,
        p.months_registered / 12.0 as person_years,
        p.practice_weighted_ratio,
        p.weighted_months_12m / 12.0 as weighted_person_years,
        p.weighted_ratio_imputed,
        coalesce(o.has_olids_record, false) as has_olids_record,
        iff(
            coalesce(o.has_olids_record, false),
            coalesce(o.ltc_count, 0),
            null
        ) as ltc_count,
        iff(
            coalesce(o.has_olids_record, false),
            coalesce(o.ltc_count, 0) >= 1,
            null
        ) as has_one_or_more_ltcs,
        coalesce(c.total_cost_12m, 0) as total_cost_12m,
        coalesce(c.slam_cost_12m, 0) as slam_cost_12m,
        coalesce(c.mhsds_proxy_cost_12m, 0) as mhsds_proxy_cost_12m,
        coalesce(c.csds_proxy_cost_12m, 0) as csds_proxy_cost_12m,
        coalesce(c.crisis_cost_12m, 0) as crisis_cost_12m,
        coalesce(c.planned_cost_12m, 0) as planned_cost_12m,
        coalesce(c.community_cost_12m, 0) as community_cost_12m,
        coalesce(c.mental_health_cost_12m, 0) as mental_health_cost_12m,
        coalesce(c.unmapped_cost_12m, 0) as unmapped_cost_12m,
        coalesce(c.mh_inpatient_cost_12m, 0) as mh_inpatient_cost_12m,
        coalesce(c.mh_crisis_cost_12m, 0) as mh_crisis_cost_12m,
        coalesce(c.mh_community_cost_12m, 0) as mh_community_cost_12m,
        coalesce(c.crisis_activity_12m, 0) as crisis_activity_12m,
        coalesce(c.mh_bed_days_12m, 0) as mh_bed_days_12m,
        coalesce(c.mh_crisis_contacts_12m, 0) as mh_crisis_contacts_12m,
        coalesce(c.mh_community_contacts_12m, 0) as mh_community_contacts_12m,
        coalesce(c.csds_contacts_12m, 0) as csds_contacts_12m,
        coalesce(c.crisis_activity_12m, 0) > 0 as had_emergency_care,
        coalesce(c.mh_crisis_contacts_12m, 0) > 0 as had_mh_crisis_contact,
        coalesce(c.mh_bed_days_12m, 0) > 0 as had_mh_inpatient_stay,
        b.window_start_month,
        b.window_end_month
    from {{ ref('fct_person_resource_index') }} as p
    cross join bounds as b
    left join cost_12m as c
        on p.sk_patient_id = c.sk_patient_id
    left join {{ ref('dim_person_demographics_basic') }} as d
        on p.sk_patient_id = d.sk_patient_id
    left join {{ ref('fct_person_resource_index_olids') }} as o
        on p.sk_patient_id = o.sk_patient_id
),

ranked as (
    select
        p.*,
        ntile(100) over (order by p.total_cost_12m desc, p.sk_patient_id) as cost_percentile
    from profile as p
)

select
    r.*,
    iff(r.cost_percentile = 1, 'Top 1%', 'Other 99%') as cost_cohort,
    div0(r.total_cost_12m, r.person_years) as cost_per_head,
    div0(r.total_cost_12m, r.weighted_person_years) as cost_per_weighted_head,
    ln(1 + greatest(div0(r.total_cost_12m, r.person_years), 0)) as log_cost_per_head,
    r.factor_age_group != 'Not recorded'
        and r.gender != 'Not recorded'
        and r.factor_ethnicity_group != 'Not recorded'
        and r.factor_deprivation_group != 'Not recorded'
        as is_complete_factor_case,
    r.factor_age_group || ' | '
        || r.factor_ethnicity_group || ' | '
        || r.gender || ' | '
        || r.factor_deprivation_group as factor_cell,
    'SLAM actual + MHSDS proxy + CSDS proxy' as cost_scope
from ranked as r
