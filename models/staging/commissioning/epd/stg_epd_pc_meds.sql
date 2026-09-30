/*
Primary care dispensed prescription items, including historical submissions.
MedsV2 replaces MedsV1 for every processing period present in V2. Earlier
periods remain on V1; overlapping months are never added together.

V1 uses dmic_pseudo_nhs_number; V2 uses dmic_pseudo_person_id. Both carry
sk_patient_id values. Missing keys and out-of-area records are retained.
Costs remain in source pence; the cost model converts them to pounds.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='processed_period',
    tmp_relation_type='table',
    on_schema_change='fail'
) }}

{% set complete_query %}
with v2_periods as (
    select distinct processed_period from {{ ref('raw_epd_pc_medsv2') }}
), meds_input as (
    select
        dmic_pseudo_nhs_number as source_patient_key,
        processed_period,
        processing_period_date::date as processing_period_date,
        paid_bnf_code,
        paid_bnf_name,
        paiddmd_code,
        prescribed_bnf_code,
        prescribed_bnf_name,
        prescribeddmd_code,
        paid_formulation,
        paid_drug_strength,
        item_actual_cost::number(18,4) as item_actual_cost,
        item_nic,
        item_count::number(18,4) as item_count,
        paid_quantity,
        prescribed_quantity,
        paid_indicator,
        not_dispensed_indicator,
        private_prescription_indicator,
        charge_status,
        exemption_code,
        out_of_hours_indicator,
        paid_acbs_indicator,
        paid_cd_indicator,
        prescriber_id,
        prescriber_type,
        patient_gpods,
        patient_gpccg,
        patient_ccg,
        patient_la,
        patient_lsoa,
        patient_gplsoa,
        patient_age,
        patient_gender,
        age_bands,
        item_id,
        bsa_prescription_id,
        eps_prescription_id,
        uniq_submission_id
    from {{ ref('raw_epd_pc_medsv1') }} as source_rows
    where not exists (
        select 1 from v2_periods
        where equal_null(v2_periods.processed_period, source_rows.processed_period)
    )
    union all
    select
        dmic_pseudo_person_id as source_patient_key,
        processed_period,
        processing_period_date::date as processing_period_date,
        paid_bnf_code,
        paid_bnf_name,
        paiddmd_code,
        prescribed_bnf_code,
        prescribed_bnf_name,
        prescribeddmd_code,
        paid_formulation,
        paid_drug_strength,
        item_actual_cost::number(18,4) as item_actual_cost,
        item_nic,
        item_count::number(18,4) as item_count,
        paid_quantity,
        prescribed_quantity,
        paid_indicator,
        not_dispensed_indicator,
        private_prescription_indicator,
        charge_status,
        exemption_code,
        out_of_hours_indicator,
        paid_acbs_indicator,
        paid_cd_indicator,
        prescriber_id,
        prescriber_type,
        patient_gpods,
        patient_gpccg,
        patient_ccg,
        patient_la,
        patient_lsoa,
        patient_gplsoa,
        patient_age,
        patient_gender,
        age_bands,
        item_id,
        bsa_prescription_id,
        eps_prescription_id,
        uniq_submission_id
    from {{ ref('raw_epd_pc_medsv2') }} as source_rows
), latest_submissions as (
    select
        processed_period as submission_period,
        max(uniq_submission_id) as latest_submission_id
    from meds_input
    group by processed_period
)

select
    -- Standard patient key across the two delivery versions.
    {{consistent_sk_patient_id_format('source_patient_key')}}      as sk_patient_id

    -- period
    , processed_period                          as processed_period
    , processing_period_date                    as processing_period_date

    -- drug identification
    , paid_bnf_code
    , paid_bnf_name
    , paiddmd_code
    , prescribed_bnf_code
    , prescribed_bnf_name
    , prescribeddmd_code
    , paid_formulation
    , paid_drug_strength

    -- cost (source units, see header)
    , item_actual_cost                          as item_actual_cost
    , try_to_double(item_nic)                    as item_nic

    -- volume
    , item_count                                as item_count
    , try_to_double(paid_quantity)              as paid_quantity
    , try_to_double(prescribed_quantity)        as prescribed_quantity

    -- dispensing / charge flags
    , paid_indicator
    , not_dispensed_indicator
    , private_prescription_indicator
    , charge_status
    , exemption_code
    , out_of_hours_indicator
    , paid_acbs_indicator
    , paid_cd_indicator

    -- prescriber
    , prescriber_id
    , prescriber_type

    -- patient organisation / geography
    , patient_gpods                             as registered_practice_code
    , patient_gpccg
    , patient_ccg
    , patient_la
    , patient_lsoa
    , patient_gplsoa

    -- patient demographics
    , patient_age
    , patient_gender
    , age_bands

    -- identifiers
    , item_id
    , bsa_prescription_id
    , eps_prescription_id
    , uniq_submission_id

    -- Latest-submission flag. The EPD feed restates each processing period as
    -- a full reload: most periods carry 2 near-identical submissions
    -- (uniq_submission_id 'YYYYMM_01', 'YYYYMM_02', ...). Summing all of them
    -- double-counts cost ~2x. Cost models must filter is_latest_submission.
    , uniq_submission_id
        = latest_submissions.latest_submission_id
                                                as is_latest_submission

from meds_input as meds
left join latest_submissions
    on equal_null(meds.processed_period, latest_submissions.submission_period)
{% endset %}

{% set changed_periods = none %}
{% if is_incremental() and flags.WHICH in ['run', 'build'] %}
    {# Header counts do not describe this shared extract. Compare the complete
       projected rows so corrections within an existing submission are detected.
       Do not run this data scan during dbt compile or documentation generation. #}
    {% set period_comparison %}
        /* {{ tojson({
            "node_id": model.unique_id,
            "target_name": target.name,
            "invocation_id": invocation_id,
            "operation": "compare_processing_periods"
        }) }} */
        with incoming as (
            {{ complete_query }}
        ), source_periods as (
            select processed_period, count(*) as row_count, hash_agg(*) as content_hash
            from incoming
            group by processed_period
        ), target_periods as (
            select processed_period, count(*) as row_count, hash_agg(*) as content_hash
            from {{ this }}
            group by processed_period
        )
        select
            coalesce(s.processed_period, t.processed_period) as processed_period,
            s.row_count is not null as source_has_period
        from source_periods as s
        full outer join target_periods as t
            on equal_null(s.processed_period, t.processed_period)
        where s.row_count is distinct from t.row_count
           or s.content_hash is distinct from t.content_hash
    {% endset %}
    {% set differences = run_query(period_comparison) %}
    {% set changed_periods = [] %}
    {% for row in differences.rows %}
        {% if not row[1] or row[0] is none %}
            {{ exceptions.raise_compiler_error(
                'EPD has a removed or null processing period. Run stg_epd_pc_meds '
                ~ 'with --full-refresh to reconcile the complete source.'
            ) }}
        {% endif %}
        {% do changed_periods.append(row[0]) %}
    {% endfor %}
{% endif %}

{{ complete_query }}
{% if changed_periods is not none %}
    {% if changed_periods | length == 0 %}
where false
    {% else %}
-- Replace every row in a changed month, including older submission flags.
where meds.processed_period in (
        {% for period in changed_periods %}
    '{{ period | replace("'", "''") }}'{% if not loop.last %},{% endif %}
        {% endfor %}
)
    {% endif %}
{% endif %}
