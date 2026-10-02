{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
CCMS (Cambridge Comorbidity Score) raw flags per person.

One row per person in int_patient_person_unique. 27 SNOMED-driven condition
flags, 8 medication-driven flags, and 1 eGFR flag — 36 columns total.

Sources:
- Observations: stg_olids_observation.mapped_concept_code joined to
  stg_common_ccmc_snomed.conceptid
- Medications: int_ccms_medication_orders (pre-filtered to CCMS codelist +
  the lithium any-time exception)
- Spine: int_patient_person_unique (every person with a patient record)

Date rules (Payne et al. 2020 / 2022 modified CCMS):
- Most condition observations: 5-year lookback
- depressivedisorder, anxietydisorder, neurosis: 12-month lookback
- malignantneoplasticdiseaseexceptskin, malignantmelanoma: 5-year lookback
- Condition IDs in the bypass list (e.g. dementia, COPD, T1DM): no time limit
- Medication flags: ≥4 issues in 12m (≥1 for antiepileptic and lithium);
  lithium counts any issue ever
- eGFR: most recent two values both <60
*/

WITH date_constants AS (
    SELECT
        CURRENT_DATE() AS current_dt,
        DATEADD('month', -12, CURRENT_DATE()) AS twelve_months_ago,
        DATEADD('month', -60, CURRENT_DATE()) AS five_years_ago
),

snomed_codes_filtered AS (
    SELECT
        conceptid::VARCHAR AS conceptid,
        conditionid,
        conditionname
    FROM {{ ref('stg_common_ccmc_snomed') }}
),

over_16s as (
    select person_id
    from {{ref('dim_person_age')}}
    where age_at_least >= 16
),

pmi AS (
    SELECT DISTINCT p.person_id
    FROM {{ ref('int_patient_person_unique') }} p
    INNER JOIN over_16s o ON p.person_id = o.person_id
    WHERE p.person_id IS NOT NULL
),

-- Observations joined to CCMS SNOMED codelist, pre-filtered to the date windows
-- (or kept all-time for conditions in the bypass list).
obs_with_conditions AS (
    SELECT
        o.person_id,
        s.conditionid,
        s.conditionname,
        o.clinical_effective_date::DATE AS clinical_effective_date,
        o.result_value
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN snomed_codes_filtered s
        ON o.mapped_concept_code::VARCHAR = s.conceptid
    CROSS JOIN date_constants dc
    WHERE o.person_id IS NOT NULL
        AND o.mapped_concept_code IS NOT NULL
        AND (
            o.clinical_effective_date >= dc.five_years_ago
            OR s.conditionid IN (
                1500, 4385, 4392, 4934, 2025, 1056, 2028, 2133, 2002,
                2047, 2048, 5052, 2150, 2031, 5055, 5054, 5149, 5150,
                2032, 5053, 1525, 5051, 5047, 5048
            )
        )
),

obs_flags AS (
    SELECT
        person_id,
        MAX(IFF(conditionid = 4385, 1, 0)) AS alcoholintoxication,
        MAX(IFF(conditionid = 4392, 1, 0)) AS harmfuluseofalcohol,
        MAX(IFF(conditionid = 4934, 1, 0)) AS disordercausedbyalcohol,
        MAX(IFF(
            conditionid = 4790
                AND clinical_effective_date
                    >= (SELECT twelve_months_ago FROM date_constants),
            1, 0
        )) AS depressivedisorder,
        MAX(IFF(
            conditionid = 4791
                AND clinical_effective_date
                    >= (SELECT twelve_months_ago FROM date_constants),
            1, 0
        )) AS anxietydisorder,
        MAX(IFF(
            conditionid = 5151
                AND clinical_effective_date
                    >= (SELECT twelve_months_ago FROM date_constants),
            1, 0
        )) AS neurosis,
        MAX(IFF(conditionid = 2025, 1, 0)) AS atrialfibrillation,
        MAX(IFF(
            conditionid = 4778
                AND clinical_effective_date
                    >= (SELECT five_years_ago FROM date_constants),
            1, 0
        )) AS malignantneoplasticdiseaseexceptskin,
        MAX(IFF(
            conditionid = 5059
                AND clinical_effective_date
                    >= (SELECT five_years_ago FROM date_constants),
            1, 0
        )) AS malignantmelanoma,
        MAX(IFF(conditionid = 1056, 1, 0)) AS viralhepatitisrpt,
        MAX(IFF(conditionid = 2028, 1, 0)) AS chronicliverdisease,
        MAX(IFF(conditionid = 2133, 1, 0)) AS copd,
        MAX(IFF(conditionid = 2002, 1, 0)) AS dementia,
        MAX(IFF(conditionid = 2047, 1, 0)) AS diabetest1,
        MAX(IFF(conditionid = 2048, 1, 0)) AS diabetest2,
        MAX(IFF(conditionid = 5052, 1, 0)) AS disorderofprostate,
        MAX(IFF(conditionid = 2150, 1, 0)) AS epilepsy,
        MAX(IFF(conditionid = 2031, 1, 0)) AS congestivecardiacfailure,
        MAX(IFF(conditionid = 5055, 1, 0)) AS irritablebowelsyndrome,
        MAX(IFF(conditionid = 5054, 1, 0)) AS developmentalacademicdisorder,
        MAX(IFF(conditionid = 5149, 1, 0)) AS intellectualdisability,
        MAX(IFF(conditionid = 5150, 1, 0)) AS completetrisomy21syndrome,
        MAX(IFF(conditionid = 2032, 1, 0)) AS multiplesclerosis,
        MAX(IFF(conditionid = 5053, 1, 0)) AS parkinsonism,
        MAX(IFF(conditionid = 1525, 1, 0)) AS periphvascdiseaseleg,
        MAX(IFF(conditionid = 5051, 1, 0)) AS substancemisuse,
        MAX(IFF(conditionid = 5047, 1, 0)) AS schizophreniadisorder,
        MAX(IFF(conditionid = 5048, 1, 0)) AS bipolardisorder
    FROM obs_with_conditions
    WHERE conditionname != 'eGFR'
    GROUP BY person_id
),

med_counts AS (
    SELECT
        person_id,
        conditionid,
        COUNT(*) AS med_count
    FROM {{ ref('int_ccms_medication_orders') }}
    GROUP BY person_id, conditionid
),

meds_flags AS (
    SELECT
        person_id,
        MAX(IFF(conditionid = 5070 AND med_count >= 4, 1, 0)) AS antidepressantmedication,
        MAX(IFF(conditionid = 5160 AND med_count >= 4, 1, 0)) AS anxiolytics,
        MAX(IFF(conditionid = 5071 AND med_count >= 4, 1, 0)) AS laxatives,
        MAX(IFF(conditionid = 5069 AND med_count >= 1, 1, 0)) AS antiepilepticmedication,
        MAX(IFF(conditionid = 5068 AND med_count >= 4, 1, 0)) AS antispasmedicmedication,
        MAX(IFF(conditionid = 5065 AND med_count >= 4, 1, 0)) AS analgesicsoropiodsnotmigrainemedication,
        MAX(IFF(conditionid = 5066 AND med_count >= 4, 1, 0)) AS antiepilepticmedicationexceptbenzodiazepines,
        MAX(IFF(conditionid = 5029 AND med_count >= 1, 1, 0)) AS lithium
    FROM med_counts
    GROUP BY person_id
),

-- eGFR flags for CKD

-- Take the daily minimum 
/*
- eGFR can be calculated via multiple methods, and many systems report more than one of these variants for each test
- Some methods underestimate risk in particular groups, so taking the minimum value is the conservative approach
    */
daily_egfr as (
    select
        person_id,
        clinical_effective_date,
        min(result_value::float) as daily_min_egfr
    from obs_with_conditions
    where conditionname = 'eGFR'
    and result_value is not null
    group by person_id, clinical_effective_date
),

-- Rank distinct eGFR measurement days, most recent first
ranked_egfr_days as (
    select
        person_id,
        clinical_effective_date,
        daily_min_egfr,
        row_number() over (
            partition by person_id
            order by clinical_effective_date desc
        ) as date_rank
    from daily_egfr
),

-- Require at least two eGFR measurement days, with both below 60
egfr_flag as (
    select person_id, 1 as egfr
    from ranked_egfr_days
    where date_rank <= 2
    group by person_id
    having count(*) = 2
    and max(daily_min_egfr) < 60
)

SELECT
    p.person_id,
    COALESCE(o.alcoholintoxication, 0) AS alcoholintoxication,
    COALESCE(o.harmfuluseofalcohol, 0) AS harmfuluseofalcohol,
    COALESCE(o.disordercausedbyalcohol, 0) AS disordercausedbyalcohol,
    COALESCE(o.depressivedisorder, 0) AS depressivedisorder,
    COALESCE(o.anxietydisorder, 0) AS anxietydisorder,
    COALESCE(o.neurosis, 0) AS neurosis,
    COALESCE(o.atrialfibrillation, 0) AS atrialfibrillation,
    COALESCE(o.malignantneoplasticdiseaseexceptskin, 0) AS malignantneoplasticdiseaseexceptskin,
    COALESCE(o.malignantmelanoma, 0) AS malignantmelanoma,
    COALESCE(o.viralhepatitisrpt, 0) AS viralhepatitisrpt,
    COALESCE(o.chronicliverdisease, 0) AS chronicliverdisease,
    COALESCE(o.copd, 0) AS copd,
    COALESCE(o.dementia, 0) AS dementia,
    COALESCE(o.diabetest1, 0) AS diabetest1,
    COALESCE(o.diabetest2, 0) AS diabetest2,
    COALESCE(o.disorderofprostate, 0) AS disorderofprostate,
    COALESCE(o.epilepsy, 0) AS epilepsy,
    COALESCE(o.congestivecardiacfailure, 0) AS congestivecardiacfailure,
    COALESCE(o.irritablebowelsyndrome, 0) AS irritablebowelsyndrome,
    COALESCE(o.developmentalacademicdisorder, 0) AS developmentalacademicdisorder,
    COALESCE(o.intellectualdisability, 0) AS intellectualdisability,
    COALESCE(o.completetrisomy21syndrome, 0) AS completetrisomy21syndrome,
    COALESCE(o.multiplesclerosis, 0) AS multiplesclerosis,
    COALESCE(o.parkinsonism, 0) AS parkinsonism,
    COALESCE(o.periphvascdiseaseleg, 0) AS periphvascdiseaseleg,
    COALESCE(o.substancemisuse, 0) AS substancemisuse,
    COALESCE(o.schizophreniadisorder, 0) AS schizophreniadisorder,
    COALESCE(o.bipolardisorder, 0) AS bipolardisorder,
    COALESCE(m.antidepressantmedication, 0) AS antidepressantmedication,
    COALESCE(m.anxiolytics, 0) AS anxiolytics,
    COALESCE(m.laxatives, 0) AS laxatives,
    COALESCE(m.antiepilepticmedication, 0) AS antiepilepticmedication,
    COALESCE(m.antispasmedicmedication, 0) AS antispasmedicmedication,
    COALESCE(m.analgesicsoropiodsnotmigrainemedication, 0) AS analgesicsoropiodsnotmigrainemedication,
    COALESCE(m.antiepilepticmedicationexceptbenzodiazepines, 0) AS antiepilepticmedicationexceptbenzodiazepines,
    COALESCE(m.lithium, 0) AS lithium,
    COALESCE(e.egfr, 0) AS egfr
FROM pmi p
LEFT JOIN obs_flags o ON p.person_id = o.person_id
LEFT JOIN meds_flags m ON p.person_id = m.person_id
LEFT JOIN egfr_flag e ON p.person_id = e.person_id
