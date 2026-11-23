{# The non-leap anchor also distinguishes the former inclusive 365-day window. #}
{% set calculations = [asthma_management_history(as_of="'2024-03-31'::date"),
    asthma_management_history(as_of="'2023-03-31'::date")] %}
{% set replacements = {'stg_olids_person':'synthetic_people',
    'stg_olids_observation':'synthetic_observations',
    'int_asthma_medications_all':'synthetic_asthma',
    'int_antibacterial_medications_all':'synthetic_antibacterial',
    'stg_olids_medication_order':'synthetic_orders','stg_olids_patient':'synthetic_patients'} %}
{% set rendered = namespace(calls=[]) %}
{% for calculation in calculations %}
    {% set query = namespace(sql=calculation) %}
    {% for model, fixture in replacements.items() %}
        {% set query.sql = query.sql | replace(ref(model) | string, fixture) %}
    {% endfor %}
    {% do rendered.calls.append(query.sql) %}
{% endfor %}
with synthetic_people as (
    select column1::number id from values (-18304),(-18305),(-18306),(-18307)
), synthetic_observations as (
    select null::number person_id,null::varchar mapped_concept_code,null::date clinical_effective_date where false
), synthetic_asthma as (
    select null::number person_id,null::varchar mapped_concept_code,null::date order_date where false
), synthetic_antibacterial as (
    select column1::number person_id,column2::date order_date
    from values (-18304,'2023-03-31'),(-18305,'2023-04-01'),
        (-18306,'2022-03-31'),(-18307,'2022-04-01')
), synthetic_orders as (
    select null::varchar id,null::varchar medication_statement_id,null::varchar patient_id,null::number person_id,
        null::date clinical_effective_date,null::varchar medication_name,null::timestamp_ntz date_recorded,
        null::varchar dose,null::number quantity_value,null::varchar quantity_unit,null::number duration_days,
        null::number estimated_cost,null::varchar statement_medication_name,null::varchar mapped_concept_code,
        null::varchar mapped_concept_display,null::varchar bnf_chapter,null::varchar bnf_section,
        null::varchar bnf_code,null::varchar bnf_name where false
), synthetic_patients as (
    select null::varchar id,null::number person_id,null::varchar sk_patient_id where false
), actual as (
    {% for call in rendered.calls %}
    {% if not loop.first %}union all{% endif %}
    select {{ loop.index }}::number anchor,person_id,recent_antibacterial_medications from ({{ call }})
    {% endfor %}
), expected as (
    select column1::number anchor,column2::number person_id,column3::boolean recent_antibacterial_medications
    from values (1,-18304,false),(1,-18305,true),(1,-18306,false),(1,-18307,false),
        (2,-18304,true),(2,-18305,false),(2,-18306,false),(2,-18307,true)
)
(select * from actual minus select * from expected)
union all
(select * from expected minus select * from actual)
