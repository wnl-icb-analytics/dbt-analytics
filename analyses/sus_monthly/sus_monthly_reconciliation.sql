with expected as (

    select
        'OP' as dataset,
        dv_fin_year,
        dv_fin_month,
        count(*) as row_count
    from {{ ref('stg_sus_op_monthly_encounterdenormalised_daterange') }}
    group by all

    union all

    select
        'AE' as dataset,
        dv_fin_year,
        dv_fin_month,
        count(*) as row_count
    from {{ ref('stg_sus_ae_monthly_encounterdenormalised_daterange') }}
    group by all

    union all

    select
        'APC episode' as dataset,
        dv_fin_year,
        dv_fin_month,
        count(*) as row_count
    from {{ ref('stg_sus_ip_monthly_encounterdenormalised_daterange') }}
    group by all

    union all

    select
        'APC spell' as dataset,
        dv_fin_year,
        dv_fin_month,
        count(*) as row_count
    from {{ ref('stg_sus_ip_monthly_encounterdenormalised_daterange') }}
    where dv_is_spell = 1
    group by all

), actual as (

    select 'OP' as dataset, dv_fin_year, dv_fin_month, count(*) as row_count
    from {{ ref('fct_sus_op_monthly_attendance') }}
    group by all

    union all

    select 'AE' as dataset, dv_fin_year, dv_fin_month, count(*) as row_count
    from {{ ref('fct_sus_ae_monthly_attendance') }}
    group by all

    union all

    select 'APC episode' as dataset, dv_fin_year, dv_fin_month, count(*) as row_count
    from {{ ref('fct_sus_apc_monthly_episode') }}
    group by all

    union all

    select 'APC spell' as dataset, dv_fin_year, dv_fin_month, count(*) as row_count
    from {{ ref('fct_sus_apc_monthly_spell') }}
    group by all

)

select
    coalesce(expected.dataset, actual.dataset) as dataset,
    coalesce(expected.dv_fin_year, actual.dv_fin_year) as dv_fin_year,
    coalesce(expected.dv_fin_month, actual.dv_fin_month) as dv_fin_month,
    coalesce(expected.row_count, 0) as expected_row_count,
    coalesce(actual.row_count, 0) as actual_row_count,
    coalesce(actual.row_count, 0) - coalesce(expected.row_count, 0) as row_count_difference
from expected
full outer join actual
    on expected.dataset = actual.dataset
    and equal_null(expected.dv_fin_year, actual.dv_fin_year)
    and equal_null(expected.dv_fin_month, actual.dv_fin_month)
where coalesce(expected.row_count, 0) != coalesce(actual.row_count, 0)
order by 1, 2, 3
