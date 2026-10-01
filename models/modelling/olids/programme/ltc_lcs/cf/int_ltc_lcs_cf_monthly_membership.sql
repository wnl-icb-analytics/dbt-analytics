{{
    config(
        materialized='table',
        cluster_by=['month_end_date']
    )
}}

{% set indicators = [
    ('AF', 'af_61'), ('AF', 'af_62'),
    ('HF', 'hf_61'),
    ('CKD', 'ckd_61'), ('CKD', 'ckd_62'), ('CKD', 'ckd_63'), ('CKD', 'ckd_64'),
    ('CVD', 'cvd_61'), ('CVD', 'cvd_62'), ('CVD', 'cvd_63'), ('CVD', 'cvd_64'), ('CVD', 'cvd_65'), ('CVD', 'cvd_66'),
    ('DM', 'dm_61'), ('DM', 'dm_62'), ('DM', 'dm_63'), ('DM', 'dm_64'), ('DM', 'dm_65'), ('DM', 'dm_66'),
    ('HTN', 'htn_61'), ('HTN', 'htn_62'), ('HTN', 'htn_63'), ('HTN', 'htn_65'), ('HTN', 'htn_66'),
    ('CYP Asthma', 'cyp_ast_61')
] %}

-- LTC LCS: case-finding indicator membership at each month end, long format.
-- One row per person per month per case-finding indicator they were in, derived
-- from the SCD2 snapshot. Feeds the monthly cf summary and movement models.
--
-- Table (not incremental): see int_ltc_lcs_rs_monthly_position.sql for rationale
-- (snapshot uses hard_deletes and sits downstream of frequently-corrected logic).

with snapshot as (
    select * from {{ ref('fct_person_ltc_lcs_case_finding_snapshot') }}
),

date_spine as (
    -- Completed months only; the current in-progress month is excluded.
    select * from {{ ref('int_date_spine') }}
    where month_end_date < date_trunc('month', current_date)
),

position as (
    select
        snapshot.*,
        date_spine.month_end_date,
        date_spine.year_number,
        date_spine.month_number,
        date_spine.quarter_number,
        date_spine.month_year_label,
        date_spine.financial_year_label as financial_year,
        date_spine.financial_quarter_label as financial_quarter
    from snapshot
    inner join date_spine
        -- dbt_valid_from/to are timestamps; compare against the instant after
        -- month_end_date's day so a change on month-end day still counts.
        on snapshot.dbt_valid_from < dateadd(day, 1, date_spine.month_end_date)
        and (snapshot.dbt_valid_to is null or snapshot.dbt_valid_to >= dateadd(day, 1, date_spine.month_end_date))
)

{%- for condition, indicator in indicators %}
select
    person_id,
    month_end_date,
    year_number,
    month_number,
    quarter_number,
    month_year_label,
    financial_year,
    financial_quarter,
    '{{ condition }}' as condition,
    '{{ indicator }}' as indicator_code
from position
where in_{{ indicator }}
{%- if not loop.last %}
union all
{%- endif %}
{%- endfor %}
