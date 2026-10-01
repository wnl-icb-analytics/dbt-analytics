{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
CCMS final condition flags — collapses the 35 raw flags in
int_ccms_intermediate_flags into the 20 Cambridge categories used by the
weighted score.

Combination rules per Payne et al. 2020 / 2022 modified CCMS:
- alcohol problems = any of three alcohol observation flags
- anxietyordepression = any depressive/anxiety/neurosis observation OR
  antidepressant / anxiolytic medication
- cancer = malignant neoplasm OR melanoma
- chronickidneydisease = eGFR rule
- chronic liver disease = chronic liver disease OR viral hepatitis
- constipation = laxatives proxy
- diabetes = T1 OR T2
- epilepsy = epilepsy observation AND antiepileptic medication
- heartfailure = congestive cardiac failure observation
- IBS = IBS observation OR antispasmodic medication
- learningdisability = developmental academic disorder OR intellectual
  disability OR complete trisomy 21
- painfulcondition = non-migraine analgesics/opioids OR (antiepileptic-except-
  benzodiazepines AND epilepsy=0)
- psychoactivesubstancemisuse = substance misuse observation
- schizophreniaorbipolardisorder = schizophrenia OR bipolar OR lithium
*/

WITH ccmsintermediate AS (
    SELECT * FROM {{ ref('int_ccms_intermediate_flags') }}
)

SELECT
    person_id,

    CASE
        WHEN alcoholintoxication = 1
            OR disordercausedbyalcohol = 1
            OR harmfuluseofalcohol = 1
        THEN 1 ELSE 0
    END AS alcoholproblems,

    CASE
        WHEN depressivedisorder = 1
            OR anxietydisorder = 1
            OR neurosis = 1
            OR antidepressantmedication = 1
            OR anxiolytics = 1
        THEN 1 ELSE 0
    END AS anxietyordepression,

    atrialfibrillation,

    CASE
        WHEN malignantneoplasticdiseaseexceptskin = 1
            OR malignantmelanoma = 1
        THEN 1 ELSE 0
    END AS cancerinthelast5years,

    egfr AS chronickidneydisease,

    CASE
        WHEN viralhepatitisrpt = 1 OR chronicliverdisease = 1
        THEN 1 ELSE 0
    END AS chronicliverdiseaseandviralhepatitis,

    laxatives AS constipation,
    copd,
    dementia,

    CASE
        WHEN diabetest1 = 1 OR diabetest2 = 1 THEN 1 ELSE 0
    END AS diabetes,

    disorderofprostate,

    CASE
        WHEN epilepsy = 1 AND antiepilepticmedication = 1 THEN 1 ELSE 0
    END AS epilepsy,

    congestivecardiacfailure AS heartfailure,

    CASE
        WHEN irritablebowelsyndrome = 1 OR antispasmedicmedication = 1
        THEN 1 ELSE 0
    END AS irritablebowelsyndrome,

    CASE
        WHEN developmentalacademicdisorder = 1
            OR intellectualdisability = 1
            OR completetrisomy21syndrome = 1
        THEN 1 ELSE 0
    END AS learningdisability,

    multiplesclerosis,

    CASE
        WHEN analgesicsoropiodsnotmigrainemedication = 1
            OR (antiepilepticmedicationexceptbenzodiazepines = 1
                AND epilepsy = 0)
        THEN 1 ELSE 0
    END AS painfulcondition,

    parkinsonism,
    periphvascdiseaseleg,
    substancemisuse AS psychoactivesubstancemisuse,

    CASE
        WHEN schizophreniadisorder = 1
            OR bipolardisorder = 1
            OR lithium = 1
        THEN 1 ELSE 0
    END AS schizophreniaorbipolardisorder

FROM ccmsintermediate
