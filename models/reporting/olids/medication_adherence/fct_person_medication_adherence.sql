{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medication_adherence'])
}}

/*
Reporting-layer medicines adherence snapshot. One row per person; per drug
class, corrected-PDC measures aggregated across the person's qualifying
refill chains (drugs, VTM grain) in that class. Drill-down to drug/window
level lives in int_medication_adherence_pdc_windows (modelling layer).

Snapshot cohort — a chain qualifies only if BOTH hold:
- Repeat therapy: at least one of the chain's orders carries a
  REPEAT_PRESCRIPTION authorisation type (is_repeat_order upstream).
  Acute-only chains are excluded — PDC presumes ongoing intended therapy.
  Deliberate divergence from the original pipeline, which had no cohort
  filter.
- Recent activity: the chain's supply extended into the last
  {{ var('medication_adherence_recency_years', 1) }} year(s)
  (overall_end >= current_date - that many years). Recency is judged on
  supply, NOT on "repeat still live today": a lapsed-but-recent chain stays
  in with its (low) PDC visible — filtering on live supply would select the
  cohort on the very outcome being measured. End-date recency also means
  returning patients re-qualify automatically as soon as they collect again,
  so the filter only drops chains with a 12+ month terminal gap — beyond any
  standard permissible-gap definition of ongoing therapy.
Chains failing either test are absent; a class with no qualifying chains
shows NULL measures and drug_count 0 — meaning "no current repeat therapy in
class", not "no data" (history remains in the windows table).

Per qualifying (person, class, drug) two readings are taken from each
family:
- latest: PDC of the most recent fully elapsed rolling window (window_end
  <= current_date) that has a value — adherence over the chain's most
  recent completed ~12 months of therapy. NULL for chains younger than one
  window length.
- overall: the chain-level PDC across its full span (within the 3-year
  processing horizon).

Because a person can hold several chains within a class, each reading is
aggregated both ways — min (worst drug) and mean — pending a decision on
which to keep; <class>_drug_count says how many chains contributed.

Both measure families are available for comparison (see
int_medication_adherence_pdc):
- *_pdc_corrected_* columns: PDC2 — the last-order quirk fixed AND all
  supply right-censored at the observation date, so unobserved days enter
  neither numerator nor denominator.
- *_pdc_* columns (no _corrected): the faithful AIC measure — quirk
  included, and unobserved future supply counted as covered.
Neither family bounds exposure to the window frame, so a window's value can
describe a period overhanging that frame — an upward bias that matters when
comparing against a threshold such as 0.80. Each family's "latest" reading
independently picks
that family's most recent fully elapsed window with a non-NULL value, so
the two latest readings can come from DIFFERENT windows of the same chain
(a chain whose faithful value is NULL in a window — covered_days <= 0 via
the quirk — falls back to an earlier window for the faithful reading only).

No opt-out filter is applied at this layer. For secondary use, consumers
should INNER JOIN to REPORTING.OLIDS_PERSON_STATUS.DIM_PERSON_SECONDARY_USE_ALLOWED
ON person_id per the project convention.
*/

{% set class_prefixes = {
    'RAAS': 'raas',
    'Beta-blocker': 'beta_blocker',
    'Calcium-channel blocker': 'ccb',
    'Non-insulin anti-diabetic': 'non_insulin_antidiabetic',
    'Lipid lowering drugs': 'lipid_lowering'
} %}
{% set recency_years = var('medication_adherence_recency_years', 1) %}

with windows as (
    select *
    from {{ ref('int_medication_adherence_pdc_windows') }}
),

-- Chains with at least one repeat-authorised order
repeat_chains as (
    select distinct
        person_id,
        drug_class,
        drug_name as vtm_code
    from {{ ref('int_medication_adherence_orders') }}
    where is_repeat_order
),

-- Snapshot cohort first: repeat chains active in the last
-- {{ recency_years }} year(s). One row per chain (overall PDC is constant
-- across a chain's windows). Filtering before the latest-window QUALIFY
-- keeps excluded chains out of the window-function sort entirely.
chains as (
    select distinct
        w.person_id,
        w.drug_class,
        w.vtm_code,
        w.overall_pdc_corrected,
        w.overall_pdc
    from windows w
    inner join repeat_chains r
        on w.person_id = r.person_id
        and w.drug_class = r.drug_class
        and w.vtm_code = r.vtm_code
    where w.overall_end >= dateadd(year, -{{ recency_years }}, current_date)
),

-- Most recent fully elapsed window with a corrected PDC value, per
-- qualifying chain (per-chain computation, so filtering first cannot
-- change any surviving chain's result)
latest_window_corrected as (
    select
        w.person_id,
        w.drug_class,
        w.vtm_code,
        w.pdc_corrected as latest_pdc_corrected
    from windows w
    inner join chains c
        on w.person_id = c.person_id
        and w.drug_class = c.drug_class
        and w.vtm_code = c.vtm_code
    where w.pdc_corrected is not null
        and w.window_end <= current_date
    qualify row_number() over (
        partition by w.person_id, w.drug_class, w.vtm_code
        order by w.window_start desc
    ) = 1
),

-- Faithful equivalent — independently picks the faithful family's most
-- recent non-NULL elapsed window, which can differ from the corrected one
latest_window_faithful as (
    select
        w.person_id,
        w.drug_class,
        w.vtm_code,
        w.pdc as latest_pdc
    from windows w
    inner join chains c
        on w.person_id = c.person_id
        and w.drug_class = c.drug_class
        and w.vtm_code = c.vtm_code
    where w.pdc is not null
        and w.window_end <= current_date
    qualify row_number() over (
        partition by w.person_id, w.drug_class, w.vtm_code
        order by w.window_start desc
    ) = 1
),

per_chain as (
    select
        c.person_id,
        c.drug_class,
        c.vtm_code,
        lc.latest_pdc_corrected,
        lf.latest_pdc,
        c.overall_pdc_corrected,
        c.overall_pdc
    from chains c
    left join latest_window_corrected lc
        on c.person_id = lc.person_id
        and c.drug_class = lc.drug_class
        and c.vtm_code = lc.vtm_code
    left join latest_window_faithful lf
        on c.person_id = lf.person_id
        and c.drug_class = lf.drug_class
        and c.vtm_code = lf.vtm_code
)

select
    person_id
    {%- for label, prefix in class_prefixes.items() %},
  --  min(iff(drug_class = '{{ label }}', latest_pdc_corrected, null)) as {{ prefix }}_latest_pdc_corrected_min,
    avg(iff(drug_class = '{{ label }}', latest_pdc_corrected, null)) as {{ prefix }}_latest_pdc_corrected_mean,
  --  min(iff(drug_class = '{{ label }}', overall_pdc_corrected, null)) as {{ prefix }}_overall_pdc_corrected_min,
    avg(iff(drug_class = '{{ label }}', overall_pdc_corrected, null)) as {{ prefix }}_overall_pdc_corrected_mean,
  --  min(iff(drug_class = '{{ label }}', latest_pdc, null)) as {{ prefix }}_latest_pdc_min,
    avg(iff(drug_class = '{{ label }}', latest_pdc, null)) as {{ prefix }}_latest_pdc_mean,
  -- min(iff(drug_class = '{{ label }}', overall_pdc, null)) as {{ prefix }}_overall_pdc_min,
    avg(iff(drug_class = '{{ label }}', overall_pdc, null)) as {{ prefix }}_overall_pdc_mean,
    count(distinct iff(drug_class = '{{ label }}', vtm_code, null)) as {{ prefix }}_drug_count
    {%- endfor %}
from per_chain
group by person_id
