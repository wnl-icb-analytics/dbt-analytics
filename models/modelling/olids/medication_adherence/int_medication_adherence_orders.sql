{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medication_adherence'])
}}

/*
Medicines adherence (PDC) — base medication orders. One row per medication
order in the five adherence drug classes, restricted to oral solid forms.

Port of the NCL meds_adherence Snowpark pipeline: the notebook extraction
query plus MedicationTableSnowpark.normalise_missing_values and
.filter_oral_medications.

Selection is by BNF paragraph via get_medication_orders(bnf_code=...), one
call per paragraph UNION ALL'd, mirroring the original's
bnf_reference IN ('020505','020400','020602','060102','021200'). Note the
macro filters on the repo's canonical dm+d-derived bnf_code (chapter pruning
+ LIKE), not the source bnf_reference string the original environment used —
a documented, deliberate alignment to this repo's BNF columns.

drug_name replicates the original's vtm_concept_name grain (therapeutic
moiety — the refill-chain partition key) via stg_reference_bnf_latest, but
keys on the VTM *code* (the reference's vtm column, a stable dm+d/SNOMED
identifier) rather than the vtm_name string: snomed_code -> vtm first, then
a second-chance join at the BNF chemical-substance level (LEFT(bnf_code, 9)
— the moiety-equivalent tier of the BNF hierarchy), then
mapped_concept_display as last-resort fallback. vtm_name is carried as a
readable companion label only — it plays no part in the grouping.
drug_name_source records which level resolved, so fallback coverage is
measurable (see analyses/medication_adherence_pdc_validation.sql). The
reference rows are AMP/AMPP level, so VMP-coded orders typically resolve via
the chemical-substance join; keys mapping to more than one vtm code (rare
salt edge cases at the 9-char level) are resolved deterministically (lowest
code first) and surfaced by the validation query.

Processing dropped as already handled upstream:
- Person resolution (int_patient_person_unique), deleted-record filtering
  (lds_is_deleted) and clinical_effective_date IS NOT NULL are handled by the
  macro / staging layer — not re-implemented here.
- The original's validate_columns / lowercase_columns steps are replaced by
  the dbt staging contract and Snowflake identifier handling.
- normalise_missing_values is applied only to the consumed varchar columns
  (dose, quantity_unit, drug_name); quantity_value / duration_days arrive
  typed numeric from OLIDS, so the original's string-sentinel handling is a
  structural no-op for them.
- age_at_event is not carried: the original selected it but never used it in
  any calculation or output. Demographics come from dim_person at reporting.
- is_repeat_order is additive (not in the original): flags orders whose
  medication statement has a REPEAT_PRESCRIPTION authorisation type (same
  cluster/join pattern as int_medication_orders_repeat_current). It plays no
  part in the PDC computation — the reporting snapshot uses it to restrict
  its cohort to repeat chains.
*/

{# Processing horizon: only orders from the last N years enter the pipeline
   (var-overridable). The cutoff is truncated to a month boundary so the
   horizon only moves once a month — a raw rolling cutoff would re-anchor
   refill chains (and therefore every window grid) on every build day.
   THIS IS A DIVERGENCE FROM THE ORIGINAL, WHICH PROCESSED FULL HISTORY:
   chains older than the horizon are left-truncated, so overall_start,
   window grids and overall_pdc describe the horizon, not the full chain.
   Any parity comparison against an AIC deployment must apply the same
   horizon (or override it to a large value). #}
{% set order_lookback_years = var('medication_adherence_order_lookback_years', 3) %}

{% set bnf_paragraph_classes = {
    '020505': 'RAAS',
    '020400': 'Beta-blocker',
    '020602': 'Calcium-channel blocker',
    '060102': 'Non-insulin anti-diabetic',
    '021200': 'Lipid lowering drugs'
} %}

with repeat_prescription_codes as (
    select distinct code
    from {{ ref('stg_reference_combined_codesets') }}
    where cluster_id = 'REPEAT_PRESCRIPTION'
),

medication_orders as (
    {%- for bnf_paragraph, label in bnf_paragraph_classes.items() %}
    select
        mo.*,
        '{{ label }}' as drug_class
    from (
        {{ get_medication_orders(bnf_code=bnf_paragraph) }}
    ) mo
    where mo.order_date >= date_trunc('month', dateadd(year, -{{ order_lookback_years }}, current_date))
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
),

-- VTM lookup, primary: SNOMED code of the order's mapped concept. The
-- reference table holds AMP (presentation) and AMPP (pack) rows, so one
-- snomed_code repeats — dedupe deterministically (alphabetical vtm_name).
vtm_by_snomed as (
    select
        to_varchar(snomed_code) as snomed_code,
        to_varchar(vtm) as vtm,
        vtm_name
    from {{ ref('stg_reference_bnf_latest') }}
    where snomed_code is not null
        and vtm is not null
    qualify row_number() over (
        partition by to_varchar(snomed_code)
        order by to_varchar(vtm)
    ) = 1
),

-- VTM lookup, second chance: chemical-substance level of the BNF code
-- (chars 1-9: chapter/section/paragraph/sub-paragraph/chemical substance).
-- Catches VMP-coded orders absent from the AMP/AMPP snomed_code set: any
-- presentation of the same chemical substance in the reference resolves the
-- VTM, since brands/strengths of one chemical substance share the moiety.
vtm_by_bnf as (
    select
        left(bnf_code, 9) as bnf_chemical_substance,
        to_varchar(vtm) as vtm,
        vtm_name
    from {{ ref('stg_reference_bnf_latest') }}
    where bnf_code is not null
        and vtm is not null
    qualify row_number() over (
        partition by left(bnf_code, 9)
        order by to_varchar(vtm)
    ) = 1
),

with_vtm as (
    select
        mo.*,
        coalesce(vs.vtm, vb.vtm, mo.mapped_concept_display) as drug_name_resolved,
        coalesce(vs.vtm_name, vb.vtm_name) as vtm_name,
        case
            when vs.vtm is not null then 'VTM_SNOMED'
            when vb.vtm is not null then 'VTM_BNF'
            else 'CONCEPT_DISPLAY'
        end as drug_name_source,
        (rpc.code is not null) as is_repeat_order
    from medication_orders mo
    left join vtm_by_snomed vs
        on mo.mapped_concept_code = vs.snomed_code
    left join vtm_by_bnf vb
        on left(mo.bnf_code, 9) = vb.bnf_chemical_substance
    left join {{ ref('stg_olids_medication_statement') }} ms
        on mo.medication_statement_id = ms.id
    left join repeat_prescription_codes rpc
        on ms.authorisation_type_code = rpc.code
),

-- normalise_missing_values: sentinel strings -> NULL (no trim, matching the
-- original's lower(col) isin comparison exactly)
normalised as (
    select
        medication_order_id,
        person_id,
        order_date,
        case
            when lower(drug_name_resolved) in ('none', 'na', 'null', '', 'nan') then null
            else drug_name_resolved
        end as drug_name,
        drug_name_source,
        vtm_name,
        drug_class,
        bnf_code,
        bnf_name,
        case
            when lower(order_dose) in ('none', 'na', 'null', '', 'nan') then null
            else order_dose
        end as dose,
        order_duration_days as duration_days,
        case
            when lower(order_quantity_unit) in ('none', 'na', 'null', '', 'nan') then null
            else order_quantity_unit
        end as quantity_unit,
        order_quantity_value as quantity_value,
        mapped_concept_code,
        mapped_concept_display,
        is_repeat_order
    from with_vtm
)

-- filter_oral_medications: exact-match keyword list; NULL quantity_unit rows
-- drop out, matching Snowpark isin semantics
select *
from normalised
where lower(quantity_unit) in ('tablet', 'tablets', 'tab', 'capsule', 'capsules', 'cap')
