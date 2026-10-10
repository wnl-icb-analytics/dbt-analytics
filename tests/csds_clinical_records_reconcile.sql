with expected as (
    select 'CYP501' as source_table, 'immunisation' as clinical_record_type, cyp501_unique_id::varchar as source_row_id
    from {{ ref('int_csds_coded_immunisation') }}
    union all
    select 'CYP609', 'referral_assessment', cyp609_unique_id::varchar
    from {{ ref('stg_csds_referral_assessment') }}
    union all
    select 'CYP612', 'activity_assessment', cyp612_unique_id::varchar
    from {{ ref('stg_csds_activity_assessment') }}
    union all
    select 'CYP202', 'procedure', source_row_id::varchar
    from {{ ref('fct_csds_care_activity') }}
    where procedure_code is not null
    union all
    select 'CYP202', 'finding', source_row_id::varchar
    from {{ ref('fct_csds_care_activity') }}
    where finding_code is not null
    union all
    select 'CYP202', 'observation', source_row_id::varchar
    from {{ ref('fct_csds_care_activity') }}
    where observation_code is not null or observation_value is not null
    union all
    select 'CYP502', 'childhood_immunisation', cyp502_unique_id::varchar
    from {{ ref('stg_csds_childhood_immunisation') }}
    union all
    select 'CYP601', 'previous_diagnosis', cyp601_unique_id::varchar
    from {{ ref('stg_csds_previous_diagnosis') }}
    {% for model_name, source_table, record_type in [
        ('stg_csds_provisional_diagnosis', 'CYP606', 'provisional_diagnosis'),
        ('stg_csds_primary_diagnosis', 'CYP607', 'primary_diagnosis'),
        ('stg_csds_secondary_diagnosis', 'CYP608', 'secondary_diagnosis')
    ] %}
    union all
    select '{{ source_table }}', '{{ record_type }}', {{ source_table | lower }}_unique_id::varchar
    from {{ ref(model_name) }}
    {% endfor %}
    union all
    select 'CYP603', 'newborn_hearing_screening', cyp603_unique_id::varchar
    from {{ ref('stg_csds_newborn_hearing_screening') }}
    where newborn_hearing_screening_outcome is not null
    union all
    select 'CYP603', 'newborn_hearing_audiology', cyp603_unique_id::varchar
    from {{ ref('stg_csds_newborn_hearing_screening') }}
    where newborn_hearing_audiology_outcome is not null
    {% for condition in [
        'phenylketonuria', 'sickle_cell_disease', 'cystic_fibrosis', 'congenital_hypothyroidism',
        'medium_chain_acyl_coa_dehydrogenase_deficiency', 'homocystinuria', 'maple_syrup_urine_disease',
        'glutaric_aciduria_type_1', 'isovaleric_aciduria'
    ] %}
    union all
    select 'CYP604', 'newborn_blood_spot_{{ condition }}', cyp604_unique_id::varchar
    from {{ ref('stg_csds_blood_spot_result') }}
    where outcome_{{ condition }} is not null
    {% endfor %}
    {% for area in ['hips', 'heart', 'eyes', 'testes'] %}
    union all
    select 'CYP605', 'infant_physical_examination_{{ area }}', cyp605_unique_id::varchar
    from {{ ref('stg_csds_infant_physical_examination') }}
    where result_{{ area }} is not null
    {% endfor %}
    union all
    select 'CYP610', 'breastfeeding_status', cyp610_unique_id::varchar
    from {{ ref('stg_csds_breastfeeding_status') }}
    {% for record_type, column_name in [
        ('person_weight', 'person_weight'),
        ('person_height', 'person_height_in_metres'),
        ('person_length', 'person_length_in_centimetres')
    ] %}
    union all
    select 'CYP611', '{{ record_type }}', cyp611_unique_id::varchar
    from {{ ref('stg_csds_observation') }}
    where {{ column_name }} is not null
    {% endfor %}
)
select coalesce(e.source_table, a.source_table) as source_table,
    coalesce(e.clinical_record_type, a.clinical_record_type) as clinical_record_type,
    e.source_row_id as expected_source_row_id, a.source_row_id as actual_source_row_id,
    a.source_record_id
from expected as e
full outer join {{ ref('fct_csds_clinical_record') }} as a
    on e.source_table = a.source_table
    and e.clinical_record_type = a.clinical_record_type
    and e.source_row_id = a.source_row_id
where e.source_row_id is null or a.source_row_id is null
