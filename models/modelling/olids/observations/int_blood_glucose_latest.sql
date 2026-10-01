{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}

--This model captures Blood Glucose measurement including fasting glucose using PCD Refset codes. Latest Event using a macro.

SELECT
person_id
,clinical_effective_date
--,concept_code
,concept_display
--,source_cluster_id
,result_value
,result_unit_display
,case when concept_code in ('1003141000000105','1003131000000101','58111000237107','997681000000108') then TRUE else FALSE end as is_fasting
,CONCAT(CAST(RESULT_VALUE AS VARCHAR) || ' ' || RESULT_UNIT_DISPLAY) as BLOOD_GLUCOSE_DISPLAY

FROM (
    {{ get_latest_events(
        ref('int_blood_glucose_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_blood_glucose