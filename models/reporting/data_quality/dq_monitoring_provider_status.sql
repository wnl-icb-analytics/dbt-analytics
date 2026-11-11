{{ config(materialized='view') }}

with dataset_tables as (

    -- =====================================================
    -- Dataset-to-provider-config mapping
    -- =====================================================

    select
        'SUS_IP' as table_id,
        'SUS' as dataset_group,
        null::varchar as submission_dataset

    union all

    select
        'SLAM_LSACM' as table_id,
        'SLAM' as dataset_group,
        'LSACM' as submission_dataset

    union all

    select
        'SLAM_LSPLCM' as table_id,
        'SLAM' as dataset_group,
        'LSPLCM' as submission_dataset

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

        Starting from configuration means a missing provider
        still produces a DQ assessment row.
    */

    select
        a.measurement_date,
        a.table_id,
        d.dataset_group,
        d.submission_dataset,

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

slam_submission_periods as (

    /*
        Governed SLAM expected-period source.

        int_ls_contract_dataset_submissions already resolves
        the latest submitted month for each expected provider
        and dataset.

        Only configured CORE providers are used when identifying
        the current reporting position.
    */

    select
        d.table_id,
        s.dataset,
        s.provider_code,
        s.financial_year,
        s.fy_month_submitted

    from {{ ref('int_ls_contract_dataset_submissions') }} s

    inner join dataset_tables d
        on d.submission_dataset = s.dataset

    inner join {{ ref('dq_monitoring_provider_config') }} c
        on c.dataset_group = d.dataset_group
       and c.provider_code = s.provider_code

    where d.dataset_group = 'SLAM'
      and c.monitor_individually = true
      and s.financial_year is not null
      and s.fy_month_submitted is not null

),

expected_slam_period as (

    /*
        Establish the latest submitted period for
        each SLAM dataset.

        Example:
            submission registry = FY 2026 / M06
            DQ representation    = 202627 / M06
    */

    select
        table_id,

        financial_year * 100
            + mod(financial_year + 1, 100)
                as expected_financial_year,

        fy_month_submitted
            as expected_financial_month,

        (financial_year * 12)
            + fy_month_submitted
                as expected_period_index

    from slam_submission_periods

    qualify row_number() over (
        partition by table_id

        order by
            financial_year desc,
            fy_month_submitted desc
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
                )
                + m.latest_actual_financial_month

        end as provider_period_index

    from expected_providers e

    left join provider_measurements m
        on e.measurement_date = m.measurement_date
       and e.table_id = m.table_id
       and e.provider_code = m.provider_code

    left join expected_slam_period s
        on e.table_id = s.table_id

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

    -- =====================================================
    -- Provider Status
    -- =====================================================

    case

        -- Expected CORE provider produced no measurement
        when row_count is null
            then 'RED'

        -- SLAM provider period could not be determined
        when dataset_group = 'SLAM'
         and provider_period_index is null
            then 'GREY'

        -- Expected SLAM period could not be determined
        when dataset_group = 'SLAM'
         and expected_period_index is null
            then 'GREY'

        -- Provider is two or more reporting periods behind
        when dataset_group = 'SLAM'
         and provider_period_index
             < expected_period_index - 1
            then 'RED'

        -- Provider is exactly one reporting period behind
        when dataset_group = 'SLAM'
         and provider_period_index
             = expected_period_index - 1
            then 'AMBER'

        -- First SUS observation has no comparator
        when dataset_group = 'SUS'
         and previous_row_count is null
            then 'GREY'

        else 'GREEN'

    end as provider_status,

    -- =====================================================
    -- Status Rank
    --
    -- RED   = 1
    -- AMBER = 2
    -- GREY  = 3
    -- GREEN = 4
    -- =====================================================

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

    -- =====================================================
    -- Human-readable explanation
    -- =====================================================

    case

        when row_count is null
            then 'Expected core provider absent from latest submitted period'

        when dataset_group = 'SLAM'
         and provider_period_index is null
            then 'No valid submitted financial period available for provider'

        when dataset_group = 'SLAM'
         and expected_period_index is null
            then 'Expected SLAM reporting period could not be established'

        when dataset_group = 'SLAM'
         and provider_period_index
             < expected_period_index - 1
            then 'Provider is two or more submitted financial periods behind'

        when dataset_group = 'SLAM'
         and provider_period_index
             = expected_period_index - 1
            then 'Provider is one submitted financial period behind'

        when dataset_group = 'SUS'
         and previous_row_count is null
            then 'Provider present but no previous SUS measurement is available'

        when dataset_group = 'SUS'
            then 'Provider present; daily row movement available for review'

        when dataset_group = 'SLAM'
            then 'Provider has reached the latest genuinely submitted financial period'

        else 'Provider measurement available'

    end as status_reason

from provider_assessment
