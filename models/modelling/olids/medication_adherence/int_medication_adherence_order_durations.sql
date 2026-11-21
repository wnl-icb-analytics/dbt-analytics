{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medication_adherence'])
}}

/*
Medicines adherence (PDC) — dose parsing, calculated durations and refill
chains. One row per (person_id, drug_name, order_date) after same-day dedup.

Port of MedicationTableSnowpark.clean_dose, .calculate_duration_and_flags,
.calculate_order_end_date and .calculate_days_to_next_order.

The frequency CASE replicates the original's pattern precedence:
WHEN ORDER IS LOAD-BEARING — DO NOT REORDER OR "TIDY" THE PATTERNS
('twice' must come after 'twice daily'/'twice a day'; 'daily' after all the
'X times daily' forms; 'od' after the word forms; etc.). The null-unsafe !=
in duration_flag is likewise deliberate (NULL comparison falls to ELSE 0,
matching Snowpark when/otherwise) — do not change to IS DISTINCT FROM.

Divergences from the original:
- Step 7 (deduplicate_orders) omitted: a no-op, the ranked same-day dedup in
  step 6 already guarantees the (person, drug, date) grain.
- medication_order_id added as a deterministic dedup tie-break; the Snowpark
  rank left equal-quantity ties nondeterministic.
- days_to_next_order_corrected / is_last_order added: the original 0-fills
  days_to_next_order on the final order of each refill chain, which later
  subtracts that order's entire duration from covered days (see
  int_medication_adherence_pdc). The corrected column keeps NULL instead so
  the PDC model can compute both faithful and corrected variants.
- dose_clean / count_token retained in the output for QA (the original
  dropped dose_clean); purely additive.
- dateadd(day, <fractional calculated_duration>, order_date) is passed
  through uncast: the original executed the identical DATEADD in Snowflake
  via Snowpark, so whatever fractional-day handling applied then applies now.
*/

with base as (
    select * from {{ ref('int_medication_adherence_orders') }}
),

-- clean_dose: lowercase, \b0d\b -> 'od', collapse whitespace
dose_cleaned as (
    select
        base.*,
        regexp_replace(
            regexp_replace(lower(dose), '\\b0d\\b', 'od'),
            '\\s+', ' '
        ) as dose_clean
    from base
),

dose_parsed as (
    select
        dose_cleaned.*,

        -- Frequency: first matching pattern wins. NULL dose_clean fails every
        -- RLIKE and falls to ELSE 1, matching Snowpark otherwise(1).
        case
            when dose_clean rlike '.*\\bfour times daily\\b.*' then 4
            when dose_clean rlike '.*\\bthree times daily\\b.*' then 3
            when dose_clean rlike '.*\\btwice daily\\b.*' then 2
            when dose_clean rlike '.*\\bonce daily\\b.*' then 1
            when dose_clean rlike '.*\\bfour times a day\\b.*' then 4
            when dose_clean rlike '.*\\bthree times a day\\b.*' then 3
            when dose_clean rlike '.*\\btwice a day\\b.*' then 2
            when dose_clean rlike '.*\\bonce a day\\b.*' then 1
            when dose_clean rlike '.*\\bfour times\\b.*' then 4
            when dose_clean rlike '.*\\bthree times\\b.*' then 3
            when dose_clean rlike '.*\\bevery morning\\b.*' then 1
            when dose_clean rlike '.*\\bevery evening\\b.*' then 1
            when dose_clean rlike '.*\\bat night\\b.*' then 1
            when dose_clean rlike '.*\\btwice\\b.*' then 2
            when dose_clean rlike '.*\\bonce\\b.*' then 1
            when dose_clean rlike '.*\\bdaily\\b.*' then 1
            when dose_clean rlike '.*\\bper day\\b.*' then 1
            when dose_clean rlike '.*\\ba day\\b.*' then 1
            when dose_clean rlike '.*\\beach day\\b.*' then 1
            when dose_clean rlike '.*\\bevery day\\b.*' then 1
            when dose_clean rlike '.*\\bmorning\\b.*' then 1
            when dose_clean rlike '.*\\bbreakfast\\b.*' then 1
            when dose_clean rlike '.*\\bnight\\b.*' then 1
            when dose_clean rlike '.*\\bqds\\b.*' then 4
            when dose_clean rlike '.*\\btds\\b.*' then 3
            when dose_clean rlike '.*\\bbd\\b.*' then 2
            when dose_clean rlike '.*\\bod\\b.*' then 1
            when dose_clean rlike '.*\\bo\\.?d\\b.*' then 1
            else 1
        end as frequency,

        -- First match of a number-word or digits (whole-match extract; the
        -- numeric alternative deliberately has no word boundary — faithful)
        regexp_substr(
            dose_clean,
            '\\b(half|one|two|three|four|five|six|seven|eight|nine|ten)\\b|(\\d+\\.?\\d*)'
        ) as count_token
    from dose_cleaned
),

dose_counted as (
    select
        dose_parsed.*,
        -- Numeric match kept only when <= 4 (a larger number, e.g. a pack
        -- size, deliberately falls through to 1 — faithful)
        case
            when count_token rlike '^\\d+\\.?\\d*$' and try_to_double(count_token) <= 4
                then try_to_double(count_token)
            when count_token = 'half' then 0.5
            when count_token = 'one' then 1
            when count_token = 'two' then 2
            when count_token = 'three' then 3
            when count_token = 'four' then 4
            when count_token = 'five' then 5
            when count_token = 'six' then 6
            when count_token = 'seven' then 7
            when count_token = 'eight' then 8
            when count_token = 'nine' then 9
            when count_token = 'ten' then 10
            else 1
        end as tablet_count
    from dose_parsed
),

durations as (
    select
        dose_counted.*,
        tablet_count * frequency as tablets_per_day,
        case
            when quantity_value is not null and tablet_count * frequency > 0
                then quantity_value / (tablet_count * frequency)
        end as calculated_duration_raw,
        -- Deliberately null-unsafe: NULL != x -> NULL -> ELSE 0 (see header)
        case
            when calculated_duration_raw != duration_days then 1
            else 0
        end as duration_flag,
        coalesce(calculated_duration_raw, duration_days) as calculated_duration,
        dateadd(day, coalesce(calculated_duration_raw, duration_days), order_date) as order_enddate
    from dose_counted
),

-- Same-day dedup: keep the highest-quantity order per (person, drug, date);
-- assumption inherited from the original that re-issued statements corrected
-- the quantity. medication_order_id DESC added as deterministic tie-break.
deduplicated as (
    {{ deduplicate_table('durations', ['person_id', 'drug_name', 'order_date'], ['quantity_value', 'medication_order_id']) }}
),

with_next_order as (
    select
        deduplicated.*,
        lead(order_date) over (
            partition by person_id, drug_name
            order by order_date
        ) as next_order_date
    from deduplicated
)

select
    medication_order_id,
    person_id,
    order_date,
    drug_name,
    drug_name_source,
    vtm_name,
    drug_class,
    bnf_code,
    dose,
    dose_clean,
    count_token,
    frequency,
    tablet_count,
    tablets_per_day,
    quantity_unit,
    quantity_value,
    duration_days,
    calculated_duration_raw,
    duration_flag,
    calculated_duration,
    order_enddate,
    next_order_date,
    -- Faithful: 0-filled on the final order of a chain (drives the last-order
    -- quirk in the PDC model)
    coalesce(datediff(day, order_date, next_order_date), 0) as days_to_next_order,
    -- Corrected: NULL on the final order
    datediff(day, order_date, next_order_date) as days_to_next_order_corrected,
    next_order_date is null as is_last_order,
    mapped_concept_code,
    mapped_concept_display
from with_next_order
