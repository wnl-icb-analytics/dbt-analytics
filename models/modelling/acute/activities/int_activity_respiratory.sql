-- SET VARIABLES
-- All respiratory related codes << 50043002
{% 
    set snomed_diagnosis_list = dbt_utils.get_column_values(
        ref('stg_reference_combined_codesets'),
        'code',
        where="cluster_id = 'TOT_RESP_COND'"
    ) 
%}

-- entire respiratory chapter
{% set icd10_prefix_list = ['J'] %}

-- all adult and respiratory hrg codes
{% set hrg_prefix_list = ['DZ', 'PD'] %}

-- specific procedures
{% set OPCS4_prefix_list = ['E49', 'E63.4', 'E65', 'E25', 'E36', 'E85'] %}
-- remove invasive ventilation?

-- all respiratory
{% set specialty_list = ['340'] %}


with ae_attendance_summary as (
    select visit_occurrence_id
    from {{ ref('int_sus_uec_encounter') }}
    where 
        -- respiratory chief complaint
        chief_complaint_ecds_group1 = 'Airway / breathing' 
        or
        -- respiratory primary diagnosis
        (
            {% for prefix in icd10_prefix_list %}
                startswith(primary_diagnosis_code_icd10, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) 
        -- respiratory primary diagnosis snomed
        or 
         primary_diagnosis_code_snomed in {{ to_sql_list(snomed_diagnosis_list) }}
        -- respiratory procedure / investigation snomed?
),


ae_diagnosis as ( -- likely to be too permissive as all diagnosis codes are recorded regardless of relevance
    select visit_occurrence_id
    from {{ ref('int_sus_uec_diagnosis') }} -- consider changing to all J?
    where (
            {% for prefix in icd10_prefix_list %}
                startswith(mapped_icd10_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) 
        or 
        (source_concept_code in {{ to_sql_list(snomed_diagnosis_list) }})
),

-- consider adding procedures


-- Admitted
admitted_spells_summary as (
    select visit_occurrence_id
    from {{ ref('int_sus_apc_encounter') }}
    where (
            {% for prefix in icd10_prefix_list %}
                startswith(primary_diagnosis_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) 
      or LEFT(hrg_code, 2) IN {{ to_sql_list(hrg_prefix_list) }} -- consider switching to core_hrg_chapter = D if want more broad
),

admitted_procedure as (
    select visit_occurrence_id
    from {{ ref('int_sus_apc_procedure') }}
    where (
            {% for prefix in OPCS4_prefix_list %}
                startswith(source_concept_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) ),

admitted_procedure_hrg as (
    select visit_occurrence_id
    from {{ ref('int_sus_apc_procedure_hrg') }}
    where LEFT(source_concept_code, 2) IN {{ to_sql_list(hrg_prefix_list) }}
),


-- Outpatient
-- Notable exclusion: respiratory infections (J1*, J2*, B97.4)
outpatient_appts_summary as (
    select visit_occurrence_id
    from {{ ref('int_sus_op_appointment') }}
    where main_specialty_code IN {{ to_sql_list(specialty_list) }}
      or treatment_function_code IN {{ to_sql_list(specialty_list) }}
      or (
            {% for prefix in icd10_prefix_list %}
                startswith(primary_diagnosis_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) 
    or LEFT(core_hrg_code, 2) IN {{ to_sql_list(hrg_prefix_list) }}
),

outpatient_diagnosis as ( -- keeping as seems less catch all than APC
    select visit_occurrence_id
    from {{ ref('int_sus_op_diagnosis') }}
    where (
            {% for prefix in icd10_prefix_list %}
                startswith(source_concept_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) 
),

-- DZ = Adult Respiratory System Procedures and Disorder
-- PD = paediatric respiratory system procedures and disorder

outpatient_procedure as (
    select visit_occurrence_id
    from {{ ref('int_sus_op_procedure') }}
    where {% for prefix in OPCS4_prefix_list %}
                startswith(source_concept_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
),

specialty_filters as (
    select visit_occurrence_id
    from ae_attendance_summary
    
    union all

    select visit_occurrence_id
    from ae_diagnosis
    
    union all
    
    select visit_occurrence_id
    from admitted_spells_summary
    
    union all
    
    select visit_occurrence_id
    from admitted_procedure
    
    union all
    
    select visit_occurrence_id
    from admitted_procedure_hrg

    union all
    
    select visit_occurrence_id
    from outpatient_appts_summary
    
    union all
    
    select visit_occurrence_id
    from outpatient_diagnosis
    
    
    union all
    
    select visit_occurrence_id
    from outpatient_procedure
),

inclusion_list as (
    select distinct visit_occurrence_id
    from specialty_filters
)

select summ.*, 
    true as respiratory_encounter
from {{ref('int_encounter_primary_coding')}} as summ
inner join inclusion_list as filt on filt.visit_occurrence_id = summ.visit_occurrence_id