{{
    config(
        materialized='view',
        tags=['childhood_imms']
    )
}}
--table detailing the latest vaccination date for each practice and GP, based on the maximum vaccination date across all doses for each person.
WITH PERSON_MAX_DATE AS (
    SELECT
        PRACTICE_BOROUGH,
        PRACTICE_CODE,
        GP_NAME,
        GREATEST_IGNORE_NULLS(
            SIXIN1_DATE_DOSE_1,
            SIXIN1_DATE_DOSE_2,
            SIXIN1_DATE_DOSE_3,
            SIXIN1_DATE_DOSE_4,
            MENB_DATE_DOSE_1,
            MENB_DATE_DOSE_2,
            MENB_DATE_DOSE_3,
            ROTA_DATE_DOSE_1,
            ROTA_DATE_DOSE_2,
            PCV_DATE_DOSE_1,
            PCV_DATE_DOSE_2,
            HIBMC_DATE_DOSE_1,
            MMR_DATE_DOSE_1,
            MMRV_DATE_DOSE_1,
            MMR_DATE_DOSE_2,
            MMRV_DATE_DOSE_2,
            FOURIN1_DATE_DOSE_1
        ) AS PERSON_MAX_VACC_DATE
    FROM {{ ref('int_childhood_imms_person_level_child') }}
)
SELECT
    PRACTICE_BOROUGH,
    PRACTICE_CODE,
    GP_NAME,
    MAX(PERSON_MAX_VACC_DATE) AS LATEST_VACCINATION_DATE
FROM PERSON_MAX_DATE
GROUP BY
    PRACTICE_BOROUGH,
    PRACTICE_CODE,
    GP_NAME