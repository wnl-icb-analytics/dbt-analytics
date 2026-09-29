{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

select distinct* from (
select 
sched.VACCINE_ORDER,
sched.VACCINE_ID, 
clut.VACCINE,
CAST(clut.SNOMEDCONCEPTID AS VARCHAR) AS CODE,
clut.PROPOSEDCLUSTER as CODECLUSTERID, 
sched.ADMINISTERED_CLUSTER_ID, 
sched.DRUG_CLUSTER_ID,
sched.DECLINED_CLUSTER_ID,
sched.CONTRAINDICATED_CLUSTER_ID , 
sched.DOSE_NUMBER AS SCHEDULE_DOSE,
clut.DOSE AS CODE_DOSE,
CASE 
WHEN clut.VACCINE = 'Shingles' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '1' THEN '1'
WHEN clut.VACCINE = 'Shingles' and clut.DOSE = '1,2' and sched.DOSE_NUMBER = '2' THEN '2'
WHEN clut.VACCINE = sched.VACCINE_NAME AND clut.DOSE = sched.DOSE_NUMBER THEN clut.DOSE
END as DOSE_MATCH
FROM {{ ref('stg_reference_adult_imms_codes') }}  clut
INNER JOIN {{ ref('stg_reference_imms_schedule_adult_latest') }} sched ON
            (sched.administered_cluster_id = clut.proposedcluster OR
            sched.drug_cluster_id = clut.proposedcluster OR
            sched.declined_cluster_id = clut.proposedcluster OR
            sched.contraindicated_cluster_id = clut.proposedcluster) 
            
order by vaccine, code, dose_match
) a
where a.dose_match is not null
order by 1