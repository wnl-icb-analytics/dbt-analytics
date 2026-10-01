-- SET VARIABLES
-- Only asthma and copd related codes << 195967001 |asthma| or << 13645005 |COPD|
{% 
    set snomed_diagnosis_list_resp = dbt_utils.get_column_values(
        ref('stg_reference_combined_codesets'),
        'code',
        where="cluster_id = 'LOWER_RESP_COND'"
    ) 
%}

{% 
    set snomed_diagnosis_list_all = dbt_utils.get_column_values(
        ref('stg_reference_combined_codesets'),
        'code',
        where="cluster_id = 'TOT_RESP_COND'"
    )
%}

{% set snomed_diagnosis_exclude = snomed_diagnosis_list_all | reject('in', snomed_diagnosis_list_resp) | list %}

-- specific icd10 codes (lower acute, lower chronic)
{% set icd10_prefix_list_inclusion = ['J4', 'J2'] %}
{% set icd10_prefix_list_exclusion = ['J0', 'J1', 'J3', 'J5', 'J6', 'J7', 'J8', 'J9'] %}

-- ICD-10 codes for respiratory diseases:
-- Acute upper respiratory infections (J00-J06)
-- Influenza and pneumonia (J09-J18)
-- Other acute lower respiratory infections (J20-J22) -- INCLUDED
-- Other diseases of upper respiratory tract (J30-J39)
-- Chronic lower respiratory diseases (J40-J4A) -- INCLUDED
-- Lung diseases due to external agents (J60-J70)
-- Other respiratory diseases principally affecting the interstitium (J80-J84)
-- Suppurative and necrotic conditions of the lower respiratory tract (J85-J86)
-- Other diseases of the pleura (J90-J94)
-- Intraoperative and postprocedural complications and disorders of respiratory system, not elsewhere classified (J95)
-- Other diseases of the respiratory system (J96-J99)

-- all adult and respiratory hrg codes
{% set hrg_prefix_list_inclusion = ['DZ15', 'DZ22', 'DZ23', 'DZ65'] %}
{% set hrg_prefix_list_exclusion = ['DZ01','DZ02','DZ09','DZ10','DZ11','DZ12','DZ14','DZ16','DZ17','DZ18','DZ19','DZ20','DZ24','DZ25','DZ26','DZ27','DZ28','DZ29','DZ30','DZ31','DZ32','DZ33','DZ36','DZ37','DZ38','DZ42','DZ45','DZ46','DZ49','DZ50','DZ51','DZ52','DZ55','DZ56','DZ57','DZ58','DZ59','DZ60','DZ62','DZ63','DZ64','DZ66','DZ67','DZ68','DZ69','DZ70','DZ71'] %}

-- ASTHMA = DZ15, ACUTE LOWER INFECTION = DZ22, BROCHOPNEUMONIA = DZ23
-- specific procedures
{% set OPCS4_prefix_list = ['E49', 'E63.4', 'E65', 'E25', 'E36'] %}

with respiratory_activities as (
    select * 
    from {{ ref('int_activity_respiratory') }}
),

acute_exclusions as (
    select visit_occurrence_id
    from respiratory_activities
    where    -- Meets exclusion criteria
            (
                primary_complaint in ('13094009', '70407001', '262599003')
                or 
                {% for prefix in icd10_prefix_list_exclusion %}
                    startswith(primary_diagnosis_icd10, '{{ prefix }}')
                    {% if not loop.last %} or {% endif %}
                {% endfor %}
                or 
                primary_diagnosis_snomed in {{ to_sql_list(snomed_diagnosis_exclude) }}
                or 
                LEFT(core_hrg, 4) IN {{ to_sql_list(hrg_prefix_list_exclusion) }}
            )
            -- BUT does NOT meet inclusion criteria (exclusion override)
            and not (
                -- Inclusion criteria checks
                {% for prefix in icd10_prefix_list_inclusion %}
                    startswith(primary_diagnosis_icd10, '{{ prefix }}')
                    {% if not loop.last %} or {% endif %}
                {% endfor %}
                or
                primary_diagnosis_snomed in {{ to_sql_list(snomed_diagnosis_list_resp) }}
                or
                {% for prefix in hrg_prefix_list_inclusion %}
                    startswith(core_hrg, '{{ prefix }}')
                    {% if not loop.last %} or {% endif %}
                {% endfor %} 
                or 
                LEFT(core_hrg, 4) IN {{ to_sql_list(hrg_prefix_list_inclusion) }}
            )
    ),

ae_diagnosis_excluded as ( 
    select resp.visit_occurrence_id
    from {{ ref('int_sus_uec_diagnosis') }} obs
    inner join respiratory_activities as resp on obs.visit_occurrence_id = resp.visit_occurrence_id  -- is in respiratory list
    where -- in exclusion list 
        (
            {% for prefix in icd10_prefix_list_exclusion %}
                startswith(obs.mapped_icd10_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
            or 
            obs.source_concept_code in {{ to_sql_list(snomed_diagnosis_exclude) }}
        )
        and not (
            -- not in inclusion list 
            {% for prefix in icd10_prefix_list_inclusion %}
                startswith(obs.mapped_icd10_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %} 
            or 
            obs.source_concept_code in {{ to_sql_list(snomed_diagnosis_list_resp) }}
        )
),

-- consider adding procedures
admitted_procedure_hrg_excluded as (
    select resp.visit_occurrence_id
    from {{ ref('int_sus_apc_procedure_hrg') }} obs
    inner join respiratory_activities as resp on obs.visit_occurrence_id = resp.visit_occurrence_id  -- is in respiratory list
    where -- in exclusion list 
         (LEFT(source_concept_code, 4) IN {{ to_sql_list(hrg_prefix_list_exclusion ) }})
        -- is not in inclusion list 
        and not (LEFT(source_concept_code, 4) IN {{ to_sql_list(hrg_prefix_list_inclusion) }})
),


-- Outpatient
outpatient_diagnosis_excluded as ( -- keeping as seems less catch all than APC
    select resp.visit_occurrence_id
    from {{ ref('int_sus_op_diagnosis') }} obs
    inner join respiratory_activities as resp on obs.visit_occurrence_id = resp.visit_occurrence_id  -- is in respiratory list
    where  -- in exclusion list 
        (
            {% for prefix in icd10_prefix_list_exclusion %}
                startswith(obs.source_concept_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        ) 
        -- and is not in inclusion list 
        and not (
            {% for prefix in icd10_prefix_list_inclusion %}
                startswith(obs.source_concept_code, '{{ prefix }}')
                {% if not loop.last %} or {% endif %}
            {% endfor %}
        )
),


exclusion_list as (
    select visit_occurrence_id
    from acute_exclusions
    
    union all
    
    select visit_occurrence_id
    from ae_diagnosis_excluded
    
    union all
    
    select visit_occurrence_id
    from admitted_procedure_hrg_excluded

    union all
    
    select visit_occurrence_id
    from outpatient_diagnosis_excluded

)

select resp.*,
    true as lower_respiratory_encounter
from respiratory_activities as resp
left join exclusion_list ex on ex.visit_occurrence_id = resp.visit_occurrence_id
where ex.visit_occurrence_id is null