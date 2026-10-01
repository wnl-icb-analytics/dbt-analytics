{{
    config(
        materialized='table',
        cluster_by=['month_end_date']
    )
}}

-- LTC LCS: person-level risk stratification position at each month end.
-- One row per person per month_end_date they were on the LTC LCS MOC base
-- population, using the SCD2 snapshot to answer "what was their risk group
-- as of this month end". Feeds the monthly roll-up and movement models.
--
-- Table (not incremental): the snapshot has hard_deletes='invalidate' and sits
-- downstream of risk-stratification logic that gets corrected often, so a closed
-- month's answer can legitimately shift between runs. Revisit once snapshot
-- history is long enough that a full rebuild is no longer cheap.

with snapshot as (
    select * from {{ ref('fct_person_ltc_lcs_risk_summary_snapshot') }}
),

date_spine as (
    -- Completed months only; the current in-progress month is excluded.
    select * from {{ ref('int_date_spine') }}
    where month_end_date < date_trunc('month', current_date)
)

select
    snapshot.person_id,
    date_spine.month_end_date,
    date_spine.year_number,
    date_spine.month_number,
    date_spine.quarter_number,
    date_spine.month_year_label,
    date_spine.financial_year_label as financial_year,
    date_spine.financial_quarter_label as financial_quarter,
    snapshot.af_risk_group,
    snapshot.asthma_adult_risk_group,
    snapshot.asthma_cyp_risk_group,
    snapshot.chd_risk_group,
    snapshot.ckd_risk_group,
    snapshot.copd_risk_group,
    snapshot.diabetes_risk_group,
    snapshot.hf_risk_group,
    snapshot.hypertension_risk_group,
    snapshot.nafld_risk_group,
    snapshot.pad_risk_group,
    snapshot.stroke_tia_risk_group,
    snapshot.overall_risk_group,
    snapshot.overall_risk_rank,
    snapshot.in_any_risk_group
from snapshot
inner join date_spine
    -- dbt_valid_from/to are timestamps; compare against the instant after month_end_date's
    -- day (not midnight at its start) so a change recorded later on month-end day still counts.
    on snapshot.dbt_valid_from < dateadd(day, 1, date_spine.month_end_date)
    and (snapshot.dbt_valid_to is null or snapshot.dbt_valid_to >= dateadd(day, 1, date_spine.month_end_date))
