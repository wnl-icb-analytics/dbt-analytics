{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['medication_adherence'])
}}

/*
Medicines adherence — long windowed PDC table. One row per (person_id,
vtm_code, window_start): the full rolling-window output of
int_medication_adherence_pdc restricted to persons with a linked patient
record (inner join dim_person) and presented with the readable VTM name.
Kept as its own model (rather than folded into the person-grain mart)
because of that inner join; this is the drill-down table behind
fct_person_medication_adherence.

The chain key is the VTM code; drug_name re-presents it as the readable VTM
name — matching the original AIC pipeline's output — via a deduplicated
code -> name map (one name per code, lowest alphabetically, so the join can
never fan out). vtm_code remains the unique grain key: distinct codes could
in principle share a name string, so uniqueness is tested on the code, not
the name. Rows whose chain key never resolved to a VTM (drug_name_source =
CONCEPT_DISPLAY upstream) pass their concept display string through both
columns unchanged.
*/

with vtm_names as (
    select
        to_varchar(vtm) as vtm_code,
        vtm_name
    from {{ ref('stg_reference_bnf_latest') }}
    where vtm is not null
        and vtm_name is not null
    qualify row_number() over (
        partition by to_varchar(vtm)
        order by vtm_name
    ) = 1
)

select
    f.person_id,
    f.drug_name as vtm_code,
    coalesce(v.vtm_name, f.drug_name) as drug_name,
    f.drug_class,
    f.window_start,
    f.window_end,
    f.exposure_start,
    f.exposure_end,
    f.covered_days,
    f.covered_days_corrected,
    f.window_days,
    f.window_days_corrected,
    f.pdc,
    f.pdc_corrected,
    f.overall_start,
    f.overall_end,
    f.total_exposure_days,
    f.total_exposure_days_corrected,
    f.overall_pdc,
    f.overall_pdc_corrected,
    f.as_at_date,
    f.pdc_type,
    f.exclusive,
    f.window_length_months
from {{ ref('int_medication_adherence_pdc') }} f
inner join {{ ref('dim_person') }} p
    on f.person_id = p.person_id
left join vtm_names v
    on f.drug_name = v.vtm_code
