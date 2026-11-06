{{ config(materialized='view') }}

with dataset_tables as (

    -- =====================================================
    -- Dataset-to-provider-config mapping
    -- =====================================================

    select
        'SUS_IP' as table_id,
        'SUS' as dataset_group

    union all

    select
        'SLAM_LSACM' as table_id,
        'SLAM' as dataset_group

    union all

    select
        'SLAM_LSPLCM' as table_id,
        'SLAM' as dataset_group

),

assessment_dates as (

    -- One assessment date for every table/date collected
    select distinct
        h.measurement_date,
        h.table_id

    from {{ ref('dq_monitoring_provider_history') }} h

    inner join dataset_tables d
        on h.table_id = d.table_id

),

expected_providers as (

    /*
        Generate every expected CORE provider for each
        dataset and measurement date.

        Starting from config means a missing provider still
        produces a status row.
    */

    select
        a.measurement_date,
        a.table_id,
        d.dataset_group,
        c.provider_code,
        c.provider_name,
        c.provider_scope,
        c.monitor_individually,
        c.alert_enabled

    from assessment_dates a

    inner join dataset_tables d
        on a.table_id = d.table_id

    inner join {{ ref('dq_monitoring_provider_config') }} c
        on d.dataset_group = c.dataset_group

    where c.monitor_individually = true

),

provider_measurements as (

    select
        measurement_date,
        measured_at,
        table_id,
        provider_code,
        provider_name,
        row_count,
        previous_row_count,
        row_count_change,
        row_count_change_pct,
        latest_refresh_date,
        latest_actual_financial_year,
        latest_actual_financial_month,
        latest_update_date

    from {{ ref('dq_monitoring_provider_variance') }}

),

slam_period_candidates as (

    /*
        Find the most common actual financial period among
        configured CORE providers for each SLAM table/date.

        This avoids using arrival date as the expected position.
    */

    select
        p.measurement_date,
        p.table_id,
        p.latest_actual_financial_year,
        p.latest_actual_financial_month,

        count(*) as provider_count

    from provider_measurements p

    inner join dataset_tables d
        on p.table_id = d.table_id

    inner join {{ ref('dq_monitoring_provider_config') }} c
        on d.dataset_group = c.dataset_group
       and p.provider_code = c.provider_code

    where d.dataset_group = 'SLAM'
      and c.monitor_individually = true
      and p.latest_actual_financial_year is not null
      and p.latest_actual_financial_month is not null

    group by
        p.measurement_date,
        p.table_id,
        p.latest_actual_financial_year,
        p.latest_actual_financial_month

),

expected_slam_period as (

    /*
        Take the modal period.

        If two periods have the same provider count, prefer
        the later valid period.
    */

    select
        measurement_date,
        table_id,
        latest_actual_financial_year as expected_financial_year,
        latest_actual_financial_month as expected_financial_month,

        (
            to_number(
                left(
                    to_varchar(latest_actual_financial_year),
                    4
                )
            ) * 12
        ) + latest_actual_financial_month
            as expected_period_index

    from slam_period_candidates

    qualify row_number() over (
        partition by
            measurement_date,
            table_id

        order by
            provider_count desc,
            latest_actual_financial_year desc,
            latest_actual_financial_month desc
    ) = 1

),

provider_assessment as (

    select
        e.measurement_date,
        m.measured_at,
        e.table_id,
        e.dataset_group,
        e.provider_code,

        coalesce(
            e.provider_name,
            m.provider_name,
            e.provider_code
        ) as provider_name,

        e.provider_scope,
        e.monitor_individually,
        e.alert_enabled,

        m.row_count,
        m.previous_row_count,
        m.row_count_change,
        m.row_count_change_pct,

        m.latest_refresh_date,
        m.latest_actual_financial_year,
        m.latest_actual_financial_month,
        m.latest_update_date,

        s.expected_financial_year,
        s.expected_financial_month,
        s.expected_period_index,

        case
            when m.latest_actual_financial_year is not null
             and m.latest_actual_financial_month is not null
            then
                (
                    to_number(
                        left(
                            to_varchar(
                                m.latest_actual_financial_year
                            ),
                            4
                        )
                    ) * 12
                ) + m.latest_actual_financial_month
        end as provider_period_index

    from expected_providers e

    left join provider_measurements m
        on e.measurement_date = m.measurement_date
       and e.table_id = m.table_id
       and e.provider_code = m.provider_code

    left join expected_slam_period s
        on e.measurement_date = s.measurement_date
       and e.table_id = s.table_id

)

select
    measurement_date,
    measured_at,
    table_id,
    dataset_group,
    provider_code,
    provider_name,
    provider_scope,
    monitor_individually,
    alert_enabled,

    row_count,
    previous_row_count,
    row_count_change,
    row_count_change_pct,

    latest_refresh_date,
    latest_actual_financial_year,
    latest_actual_financial_month,
    latest_update_date,

    expected_financial_year,
    expected_financial_month,

    case
        -- Provider produced no row for this assessment
        when row_count is null
            then 'RED'

        -- SLAM has no valid financial-period information
        when dataset_group = 'SLAM'
         and provider_period_index is null
            then 'GREY'

        -- No SLAM estate baseline could be established
        when dataset_group = 'SLAM'
         and expected_period_index is null
            then 'GREY'

        -- Provider is two or more periods behind
        when dataset_group = 'SLAM'
         and provider_period_index
             < expected_period_index - 1
            then 'RED'

        -- Provider is exactly one period behind
        when dataset_group = 'SLAM'
         and provider_period_index
             = expected_period_index - 1
            then 'AMBER'

        -- First SUS measurement has no volume comparator
        when dataset_group = 'SUS'
         and previous_row_count is null
            then 'GREY'

        else 'GREEN'
    end as provider_status,

    case
        when row_count is null
            then 1

        when dataset_group = 'SLAM'
         and provider_period_index
             < expected_period_index - 1
            then 1

        when dataset_group = 'SLAM'
         and provider_period_index
             = expected_period_index - 1
            then 2

        when dataset_group = 'SLAM'
         and (
                provider_period_index is null
                or expected_period_index is null
             )
            then 3

        when dataset_group = 'SUS'
         and previous_row_count is null
            then 3

        else 4
    end as status_rank,

    case
        when row_count is null
            then 'Expected core provider absent from provider measurement'

        when dataset_group = 'SLAM'
         and provider_period_index is null
            then 'No valid actual financial period available for provider'

        when dataset_group = 'SLAM'
         and expected_period_index is null
            then 'Expected SLAM reporting period could not be established'

        when dataset_group = 'SLAM'
         and provider_period_index
             < expected_period_index - 1
            then 'Provider is two or more actual financial periods behind'

        when dataset_group = 'SLAM'
         and provider_period_index
             = expected_period_index - 1
            then 'Provider is one actual financial period behind'

        when dataset_group = 'SUS'
         and previous_row_count is null
            then 'Provider present but no previous SUS measurement is available'

        when dataset_group = 'SUS'
            then 'Provider present; daily row movement available for review'

        when dataset_group = 'SLAM'
            then 'Provider has reached the expected actual financial period'

        else 'Provider measurement available'
    end as status_reason

from provider_assessment
