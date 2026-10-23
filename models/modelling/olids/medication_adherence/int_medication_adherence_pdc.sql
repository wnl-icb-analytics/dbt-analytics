{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medication_adherence'])
}}

/*
Medicines adherence — rolling PDC (Proportion of Days Covered). One row per
(person_id, drug_name, window_start): person-anchored monthly rolling
12-month windows from the first order of each (person, drug) refill chain.

Port of MedicationTableSnowpark.compute_pdc_rolling(window_length=12,
pdc_type=2, exclusive=True):
- pdc_type=2 "dynamic window": the denominator is the exposure span (first
  order date to last supply end among the orders selected by the window),
  not the fixed 365-day frame. The window is only a SELECTION frame — it
  decides which orders participate and never enters the arithmetic.
- exclusive=True: strict upper bound on the window join (order_date <
  window_end), and supply overlapping an early refill is subtracted.
- Windows with no qualifying orders are retained with NULL exposure /
  covered_days / pdc, matching the original's left joins from the anchors.
- An overall PDC per (person, drug) over the full chain span rides along on
  every window row, as in the original output.

FAITHFUL VS CORRECTED

faithful (pdc, covered_days, window_days, overall_pdc, total_exposure_days)
replicates the AIC measure exactly, including three artefacts:
1. Last-order quirk — days_to_next_order is 0-filled on each chain's final
   order, so its whole duration is subtracted and it contributes ~0 covered
   days.
2. No right-censoring — supply projected beyond the observation date counts
   as covered, and exposure spans can end in the future.
3. No POI bounding — the window only selects orders, so exposure may begin
   before window_start and end after window_end; overhang days are covered
   by construction.

corrected (pdc_corrected, covered_days_corrected, window_days_corrected,
overall_pdc_corrected, total_exposure_days_corrected) fixes 1 and 2:
- the final order's supply counts (days_to_next_order_corrected is NULL on
  last orders); and
- every supply interval, and both corrected denominators, are right-censored
  at as_at_date, so unobserved days enter neither numerator nor denominator.
  The early-refill stockpile subtraction is censored identically, so days
  never counted as covered are never subtracted (without this, a recent
  long-duration order followed by a quick refill drives covered days
  sharply negative).

That aligns the corrected family with PDC2 as defined by Prieto-Merino et al.
(2021): the denominator runs from first supply to the date the last supply
runs out, bounded by the observation period. Censoring truncates rather than
drops — an order whose supply straddles as_at_date contributes its elapsed
portion to numerator and denominator alike; only an order dated after
as_at_date (a data error) contributes nothing.

Artefact 3 is deliberately NOT corrected: bounding exposure to the window
frame would make this a type-1/type-2 hybrid rather than PDC2 over the
exposure span, and would put a third cause between the two families.

as_at_date is derived from the DATA — least(max order date, current_date) —
not from the clock, so rebuilding on unchanged data reproduces the same
numbers; the current_date term only caps future-dated source records.
Override with var medication_adherence_as_at_date ('YYYY-MM-DD') to pin a
historical run. It is emitted as a column so every row carries the cutoff it
was computed under.

Because censoring removes covered days, pdc_corrected >= pdc holds only
where the exposure ends at or before as_at_date; the invariant tests exempt
the rest.

Other divergences from the original:
- All five drug classes in one table (the original wrote one table per class;
  its per-class loop only existed to name tables — every grouping key already
  includes drug_class).
- The original's collect() round-trips and global order prefilter are dropped:
  they existed only to materialise literals for Snowpark and are semantic
  no-ops in a set-based join.
- months_between(...)::int is kept verbatim (fractional, rounds half away
  from zero) — do not replace with datediff('month', ...), which counts
  boundary crossings and would change the anchor count.
- A window_days > 0 guard is added to the pdc division: unreachable on the
  faithful path (covered_days > 0 implies a positive span) but belt-and-braces
  against Snowflake division-by-zero, which Snowpark's lazy CASE never hit.
*/

{% set window_length_months = 12 %}
{% set as_at_override = var('medication_adherence_as_at_date', none) %}

with observation_cutoff as (
    {%- if as_at_override %}
    -- Pinned via var for reproducible historical runs
    select to_date('{{ as_at_override }}') as as_at_date
    {%- else %}
    -- Data-derived: how far the source actually observes, capped so that
    -- future-dated source records cannot push the cutoff forward
    select least(max(order_date), current_date) as as_at_date
    from {{ ref('int_medication_adherence_order_durations') }}
    {%- endif %}
),

orders as (
    select
        o.person_id,
        o.drug_name,
        o.drug_class,
        o.order_date,
        o.calculated_duration,
        coalesce(o.order_enddate, o.order_date) as order_enddate_filled,
        c.as_at_date,
        -- Corrected: supply truncated at the observation date
        least(coalesce(o.order_enddate, o.order_date), c.as_at_date) as order_enddate_censored,
        -- Faithful: reproduces the last-order quirk (days_to_next_order is
        -- 0-filled on the final order, so 0 < duration always holds and the
        -- whole duration is subtracted)
        case
            when o.days_to_next_order is not null
                and o.days_to_next_order < o.calculated_duration
                then o.calculated_duration - o.days_to_next_order
            else 0
        end as adjusted_overlap,
        -- Corrected: start of the early-refill stockpile region
        -- [order_date + days_to_next, supply end]. NULL when there is no
        -- early refill — including final orders, whose
        -- days_to_next_order_corrected is NULL. Clipped downstream so
        -- censored-away days are not subtracted.
        case
            when o.days_to_next_order_corrected is not null
                and o.days_to_next_order_corrected < o.calculated_duration
                then dateadd(day, o.days_to_next_order_corrected, o.order_date)
        end as overlap_region_start
    from {{ ref('int_medication_adherence_order_durations') }} o
    cross join observation_cutoff c
),

overall_bounds as (
    select
        person_id,
        drug_name,
        drug_class,
        min(order_date) as overall_start,
        max(order_enddate_filled) as overall_end
    from orders
    group by person_id, drug_name, drug_class
),

-- One row per monthly anchor from overall_start to overall_end (inclusive
-- offset range, hence + 1 on the exclusive array_generate_range stop)
anchors as (
    select
        b.person_id,
        b.drug_name,
        b.drug_class,
        b.overall_start,
        b.overall_end,
        dateadd(month, f.value::int, b.overall_start) as window_start,
        dateadd(month, f.value::int + {{ window_length_months }}, b.overall_start) as window_end
    from overall_bounds b,
        lateral flatten(
            input => array_generate_range(
                0, months_between(b.overall_end, b.overall_start)::int + 1
            )
        ) f
),

windowed_orders as (
    select
        a.person_id,
        a.drug_name,
        a.drug_class,
        a.window_start,
        a.window_end,
        o.order_date,
        o.order_enddate_filled,
        o.order_enddate_censored,
        o.adjusted_overlap,
        o.overlap_region_start
    from anchors a
    inner join orders o
        on a.person_id = o.person_id
        and a.drug_name = o.drug_name
        and a.drug_class = o.drug_class
        and o.order_date < a.window_end
        and o.order_enddate_filled >= a.window_start
),

-- pdc_type=2 dynamic exposure span per window (uncensored — the corrected
-- denominator applies the cutoff in windows_assembled)
window_exposure as (
    select
        person_id,
        drug_name,
        drug_class,
        window_start,
        window_end,
        min(order_date) as exposure_start,
        max(order_enddate_filled) as exposure_end
    from windowed_orders
    group by person_id, drug_name, drug_class, window_start, window_end
),

-- Each order clipped to the exposure span; the corrected columns additionally
-- truncate supply (and the stockpile subtraction) at the observation date
windowed_clipped as (
    select
        wo.person_id,
        wo.drug_name,
        wo.drug_class,
        wo.window_start,
        wo.window_end,
        case
            when least(wo.order_enddate_filled, e.exposure_end)
                > greatest(wo.order_date, e.exposure_start)
                then datediff(
                    day,
                    greatest(wo.order_date, e.exposure_start),
                    least(wo.order_enddate_filled, e.exposure_end)
                )
            else 0
        end as overlap_days,
        case
            when least(wo.order_enddate_censored, e.exposure_end)
                > greatest(wo.order_date, e.exposure_start)
                then datediff(
                    day,
                    greatest(wo.order_date, e.exposure_start),
                    least(wo.order_enddate_censored, e.exposure_end)
                )
            else 0
        end as overlap_days_corrected,
        wo.adjusted_overlap,
        case
            when wo.overlap_region_start is not null
                then greatest(
                    0,
                    datediff(
                        day,
                        wo.overlap_region_start,
                        least(wo.order_enddate_censored, e.exposure_end)
                    )
                )
            else 0
        end as adjusted_overlap_corrected
    from windowed_orders wo
    inner join window_exposure e
        on wo.person_id = e.person_id
        and wo.drug_name = e.drug_name
        and wo.drug_class = e.drug_class
        and wo.window_start = e.window_start
        and wo.window_end = e.window_end
),

window_covered as (
    select
        person_id,
        drug_name,
        drug_class,
        window_start,
        window_end,
        sum(overlap_days - adjusted_overlap) as covered_days,
        sum(overlap_days_corrected - adjusted_overlap_corrected) as covered_days_corrected
    from windowed_clipped
    group by person_id, drug_name, drug_class, window_start, window_end
),

-- Overall PDC over the full chain span [overall_start, overall_end]. The
-- clipping is kept for line-by-line parity with the original even though
-- every order lies within the bounds by construction.
overall_clipped as (
    select
        o.person_id,
        o.drug_name,
        o.drug_class,
        b.overall_start,
        b.overall_end,
        o.as_at_date,
        case
            when least(o.order_enddate_filled, b.overall_end)
                > greatest(o.order_date, b.overall_start)
                then datediff(
                    day,
                    greatest(o.order_date, b.overall_start),
                    least(o.order_enddate_filled, b.overall_end)
                )
            else 0
        end as overlap_days,
        case
            when least(o.order_enddate_censored, b.overall_end)
                > greatest(o.order_date, b.overall_start)
                then datediff(
                    day,
                    greatest(o.order_date, b.overall_start),
                    least(o.order_enddate_censored, b.overall_end)
                )
            else 0
        end as overlap_days_corrected,
        o.adjusted_overlap,
        case
            when o.overlap_region_start is not null
                then greatest(
                    0,
                    datediff(
                        day,
                        o.overlap_region_start,
                        least(o.order_enddate_censored, b.overall_end)
                    )
                )
            else 0
        end as adjusted_overlap_corrected
    from orders o
    inner join overall_bounds b
        on o.person_id = b.person_id
        and o.drug_name = b.drug_name
        and o.drug_class = b.drug_class
),

overall_covered as (
    select
        person_id,
        drug_name,
        drug_class,
        overall_start,
        overall_end,
        as_at_date,
        sum(overlap_days - adjusted_overlap) as covered_days,
        sum(overlap_days_corrected - adjusted_overlap_corrected) as covered_days_corrected
    from overall_clipped
    group by person_id, drug_name, drug_class, overall_start, overall_end, as_at_date
),

overall_pdc as (
    select
        person_id,
        drug_name,
        drug_class,
        datediff(day, overall_start, overall_end) as total_exposure_days,
        datediff(day, overall_start, least(overall_end, as_at_date)) as total_exposure_days_corrected,
        -- No covered_days > 0 guard here — faithful to the original, which
        -- only guarded on total_exposure_days (overall_pdc can be <= 0)
        case
            when datediff(day, overall_start, overall_end) > 0
                then covered_days / datediff(day, overall_start, overall_end)
        end as overall_pdc,
        case
            when datediff(day, overall_start, least(overall_end, as_at_date)) > 0
                then covered_days_corrected
                     / datediff(day, overall_start, least(overall_end, as_at_date))
        end as overall_pdc_corrected
    from overall_covered
),

windows_assembled as (
    select
        a.person_id,
        a.drug_name,
        a.drug_class,
        a.window_start,
        a.window_end,
        a.overall_start,
        a.overall_end,
        c.as_at_date,
        e.exposure_start,
        e.exposure_end,
        cov.covered_days,
        cov.covered_days_corrected,
        datediff(day, e.exposure_start, e.exposure_end) as window_days,
        -- Corrected denominator: exposure truncated at the observation date
        datediff(day, e.exposure_start, least(e.exposure_end, c.as_at_date)) as window_days_corrected
    from anchors a
    cross join observation_cutoff c
    left join window_exposure e
        on a.person_id = e.person_id
        and a.drug_name = e.drug_name
        and a.drug_class = e.drug_class
        and a.window_start = e.window_start
        and a.window_end = e.window_end
    left join window_covered cov
        on a.person_id = cov.person_id
        and a.drug_name = cov.drug_name
        and a.drug_class = cov.drug_class
        and a.window_start = cov.window_start
        and a.window_end = cov.window_end
)

select
    w.person_id,
    w.drug_name,
    w.drug_class,
    w.window_start,
    w.window_end,
    w.exposure_start,
    w.exposure_end,
    w.covered_days,
    w.covered_days_corrected,
    w.window_days,
    w.window_days_corrected,
    case
        when w.covered_days > 0 and w.window_days > 0
            then w.covered_days / w.window_days
    end as pdc,
    case
        when w.covered_days_corrected > 0 and w.window_days_corrected > 0
            then w.covered_days_corrected / w.window_days_corrected
    end as pdc_corrected,
    w.overall_start,
    w.overall_end,
    op.total_exposure_days,
    op.total_exposure_days_corrected,
    op.overall_pdc,
    op.overall_pdc_corrected,
    w.as_at_date,
    2 as pdc_type,
    true as exclusive,
    {{ window_length_months }} as window_length_months
from windows_assembled w
left join overall_pdc op
    on w.person_id = op.person_id
    and w.drug_name = op.drug_name
    and w.drug_class = op.drug_class
