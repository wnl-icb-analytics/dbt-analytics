-- Emit a rolling 2-year window of spells (for consumers like the C-LTCS event
-- timeline). Downstream 1-year consumers (fct_person_sus_apc_recent,
-- int_sus_apc_merged_spells) re-apply their own 12-month filter, so their row
-- coverage is unaffected. Median durations are computed over the full 2-year
-- emit set. in_year_duration remains a fixed 12-month "in year" measure.
{% set emit_lookback_months = -24 %}
{% set in_year_lookback_months = -12 %}
with
base_encounters_raw as (
    -- This model applies its own date-imputation rules for named consumers.
    -- Do not mix the reporting estimate with those independently derived dates.
    select
        * exclude (
            estimated_discharge_date,
            has_estimated_discharge_date,
            estimated_discharge_date_method
        )
        , start_date is null as dq_start_date
        , end_date is null as dq_end_date
        , duration is null as dq_duration
    from {{ ref('int_sus_apc_encounter') }}
    where (start_date between dateadd(month, {{ emit_lookback_months }}, current_date()) and current_date()
        or end_date between dateadd(month, {{ emit_lookback_months }}, current_date()) and current_date())
    and sk_patient_id is not null and sk_patient_id != '1'
    and spell_admission_method not in ('2C', '82', '31') -- birth of a baby
),

patient_median_duration as (
    select
        sk_patient_id,
        median(duration) as median_duration
    from base_encounters_raw
    where duration is not null
    group by sk_patient_id
),
hrg_median_duration as (
    select
        hrg_code,
        median(duration) as median_duration
    from base_encounters_raw
    where duration is not null
    and hrg_code is not null
    and hrg_code != 'UZ01Z'
    group by hrg_code
),
treatment_function_median_duration as (
    select
        treatment_function_code,
        median(duration) as median_duration
    from base_encounters_raw
    where duration is not null
    and treatment_function_code is not null
    group by treatment_function_code
),
global_median_duration as (
    select median(duration) as median_duration
    from base_encounters_raw
    where duration is not null
),

base_encounters_imputed_dates as (
    select
        * exclude (start_date, end_date, duration_to_date),
        coalesce(
            start_date,
            case
                when end_date is not null and duration is not null
                    then dateadd(day, -duration, end_date)
            end
        ) as start_date,
        coalesce(
            end_date,
            case
                when start_date is not null and duration is not null
                    then dateadd(day, duration, start_date)
            end
        ) as end_date,
        case
            when start_date is not null
                then greatest(0, datediff(day, start_date, current_date()))
        end as duration_to_date
    from base_encounters_raw
),

base_encounters_selected_median as (
    select
        enc.*,
        case
            when enc.hrg_code is not null and enc.hrg_code != 'UZ01Z'
                then coalesce(
                    hmd.median_duration,
                    pmd.median_duration,
                    gmd.median_duration
                )
            else coalesce(
                tfmd.median_duration,
                pmd.median_duration,
                gmd.median_duration
            )
        end as imputed_median_duration,
    from base_encounters_imputed_dates as enc
    left join hrg_median_duration as hmd
        on enc.hrg_code = hmd.hrg_code
    left join treatment_function_median_duration as tfmd
        on enc.treatment_function_code = tfmd.treatment_function_code
    left join patient_median_duration as pmd
        on enc.sk_patient_id = pmd.sk_patient_id
    cross join global_median_duration as gmd
),

base_encounters_duration_imputed as (
    select
        * exclude (duration),
        coalesce(
            duration,
            case
                when start_date is not null
                    then coalesce(
                        least(imputed_median_duration, duration_to_date),
                        imputed_median_duration
                    )
            end
        ) as duration
    from base_encounters_selected_median
),

base_encounters_end_date_imputed as (
    select
        * exclude (end_date),
        coalesce(
            end_date,
            case
                when start_date is not null and duration is not null
                    then dateadd(day, duration, start_date)
            end
        ) as end_date
    from base_encounters_duration_imputed
),

base_encounters_with_duration as (
    select
        * exclude (duration),
        coalesce(
            duration,
            datediff(day, start_date, end_date)
        ) as duration
    from base_encounters_end_date_imputed
),

base_encounters as (
    select
        *,
        case
            when start_date >= dateadd(month, {{ in_year_lookback_months }}, current_date())
                then duration
            else greatest(0, datediff(day, dateadd(month, {{ in_year_lookback_months }}, current_date()), end_date))
        end as in_year_duration
    from base_encounters_with_duration
)


select * 
from base_encounters
qualify row_number() over (
    partition by
        sk_patient_id,
        start_date,
        start_time,
        organisation_id,
        iff(
            start_date = end_date
            and (
                sk_patient_id is null
                or start_time is null
                or organisation_id is null
            ),
            visit_occurrence_id,
            null
        )
    order by end_date desc nulls last, end_time desc nulls last, visit_occurrence_id desc
) = 1
