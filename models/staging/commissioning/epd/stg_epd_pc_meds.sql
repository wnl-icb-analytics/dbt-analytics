/*
Staging for the English Prescribing Dataset (EPD) primary care meds feed.

One row per dispensed prescription item per patient per processing period.
Faithful passthrough of raw_epd_pc_medsv1: renames the patient key and types
the cost/quantity/date columns. No rows are dropped (out-of-area patients
are retained and simply won't match WNL person dimensions downstream).

Source is WNL-wide: DATA_LAKE.EPD_PRIMARY_CARE was repointed to
Data_Store_Prescribing (NCL + NWL) on 2026-06-26, so this staging covers both
ICBs (NWL ~1.4M + NCL ~0.9M patients).

Patient key: dmic_pseudo_nhs_number is the DMIC pseudonymised NHS number and
is the same domain as sk_patient_id elsewhere (validated 2026-06-26). Renamed
to sk_patient_id so it joins dim_person_pseudo / dim_person_demographics_basic.

Cost units: item_actual_cost and item_nic are carried in source units. NHSBSA
EPD reports cost in pence; conversion to £ is deferred to the modelling layer
where prescribing cost is combined with the other PODs.
*/

-- Materialised as a table: the is_latest_submission window over the full WNL EPD
-- feed is expensive to recompute on every downstream read.
{{ config(materialized = 'table') }}

select
    -- patient key (renamed from dmic_pseudo_nhs_number)
    {{consistent_sk_patient_id_format('dmic_pseudo_nhs_number')}}      as sk_patient_id

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
        = max(uniq_submission_id) over (partition by processed_period)
                                                as is_latest_submission

from {{ ref('raw_epd_pc_medsv1') }}
