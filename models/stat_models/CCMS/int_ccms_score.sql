{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Cambridge Comorbidity Score per person.

Weighted sum of the 20 Cambridge categories from int_ccms_final_flags.
Weights are the published Cambridge 2022 modified CCMS coefficients
(Payne et al. 2020 / Brilleman et al. 2022) — see
https://bjgp.org/content/73/731/e435.

ccms_score_id changes whenever the score for a person changes (or on a
fresh run); useful as a snapshot key when the table is rebuilt.
*/

WITH flags AS (
    SELECT * FROM {{ ref('int_ccms_final_flags') }}
),

scored AS (
    SELECT
        person_id,
        (
            alcoholproblems * 0.792243
            + anxietyordepression * 0.324207
            + atrialfibrillation * 0.334891
            + cancerinthelast5years * 1.202615
            + chronickidneydisease * 0.213652
            + chronicliverdiseaseandviralhepatitis * 0.68621
            + constipation * 0.383006
            + copd * 0.702181
            + dementia * 0.938001
            + diabetes * 0.29467
            + disorderofprostate * (-0.18781)
            + epilepsy * 0.477465
            + heartfailure * 0.505245
            + irritablebowelsyndrome * (-0.20368)
            + learningdisability * 0.637273
            + multiplesclerosis * 0.761606
            + painfulcondition * 0.445521
            + parkinsonism * 0.546194
            + periphvascdiseaseleg * 0.334558
            + psychoactivesubstancemisuse * 0.449321
            + schizophreniaorbipolardisorder * 0.482469
        ) AS cambridge_comorbidity_score,
        CURRENT_TIMESTAMP() AS last_updated
    FROM flags
)

SELECT
    {{
        dbt_utils.generate_surrogate_key(
            ['person_id', 'cambridge_comorbidity_score', 'last_updated']
        )
    }} AS ccms_score_id,
    person_id,
    cambridge_comorbidity_score,
    last_updated
FROM scored
