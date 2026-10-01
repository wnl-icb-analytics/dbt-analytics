{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}

/*
Find latest code for Illicit Drug Use observation per person
 */
select 
person_id
,clinical_effective_date
,concept_display
,illicit_drug_assessed_last_12m
,ILLICIT_DRUG_PATTERN
,ILLICIT_DRUG_CLASS
FROM {{ ref('int_illicit_drug_use_all') }} a
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1