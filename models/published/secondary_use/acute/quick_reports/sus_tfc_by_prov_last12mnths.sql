{{
    config(
        materialized='view',
        tags=['quick_reports'])
}}
SELECT   op.SK_PATIENT_ID
        ,TO_CHAR(op.START_DATE, 'YYYY-MM') AS APPOINTMENT_DATE_MNTH
        ,op.TREATMENT_FUNCTION_CODE
        ,op.TREATMENT_FUNCTION_CODE || ' - ' ||op.TREATMENT_FUNCTION_CODE_DESC AS Treatment_Function
        ,op.ORGANISATION_ID AS PROVIDER_CODE
        ,count(*) as TOTAL_TFC
FROM {{ ref("obt_encounter_op") }} op
WHERE op.START_DATE > DATEADD(YEAR,-1,(SELECT MAX(START_DATE) FROM {{ ref("obt_encounter_op") }} ))
GROUP BY ALL;
