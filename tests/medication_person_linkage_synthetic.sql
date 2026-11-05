{# Replace only relations, preserving both real medication macros and all four paths. #}
{% set calls = [get_medication_orders(bnf_code='0103050'),
    get_medication_orders(cluster_id='SYN_CLUSTER', source='SYN_SOURCE'),
    get_medication_statements(bnf_code='0103050'),
    get_medication_statements(cluster_id='SYN_CLUSTER', source='SYN_SOURCE')] %}
{% set replacements = {'stg_olids_medication_order':'synthetic_orders',
    'stg_olids_medication_statement':'synthetic_statements',
    'stg_olids_patient':'synthetic_patients',
    'stg_reference_combined_codesets':'synthetic_codes'} %}
{% set rendered = namespace(calls=[]) %}
{% for call in calls %}
    {% set query = namespace(sql=call) %}
    {% for model, fixture in replacements.items() %}
        {% set query.sql = query.sql | replace(ref(model) | string, fixture) %}
    {% endfor %}
    {% do rendered.calls.append(query.sql) %}
{% endfor %}
with synthetic_people as (
    select column1::varchar id from values ('SYN_MISSING'),('SYN_CONFLICT')
), synthetic_patients as (
    select 'SYN_CONFLICT'::varchar id, -18399::number person_id, 'SYN_SK'::varchar sk_patient_id
), synthetic_codes as (
    select 'SYN_PRODUCT'::varchar code,'SYN_CLUSTER'::varchar cluster_id,
        'SYN_SOURCE'::varchar source,'Synthetic cluster'::varchar cluster_description
), synthetic_orders as (
    select id, id as patient_id, -18301::number person_id,
        dateadd(day,-1,current_date())::date clinical_effective_date,
        '01'::varchar bnf_chapter,'0103'::varchar bnf_section,'0103050SYN'::varchar bnf_code,
        null::varchar bnf_name,null::varchar medication_statement_id,null::varchar medication_name,
        null::timestamp_ntz date_recorded,null::varchar dose,null::number quantity_value,
        null::varchar quantity_unit,null::number duration_days,null::number estimated_cost,
        null::varchar statement_medication_name,'SYN_PRODUCT'::varchar mapped_concept_code,
        null::varchar mapped_concept_display
    from synthetic_people
), synthetic_statements as (
    select id,id as patient_id,-18301::number person_id,
        dateadd(day,-1,current_date())::date clinical_effective_date,
        null::timestamp_ntz date_recorded,null::varchar medication_name,null::varchar dose,
        null::number quantity_value,null::varchar quantity_unit,null::varchar authorisation_type_code,
        null::varchar authorisation_type_display,null::varchar issue_method,null::boolean is_active,
        null::date cancellation_date,null::date expiry_date,null::number age_at_event,
        'SYN_PRODUCT'::varchar mapped_concept_code,null::varchar mapped_concept_display,
        '01'::varchar bnf_chapter,'0103'::varchar bnf_section,'0103050SYN'::varchar bnf_code,
        null::varchar bnf_name
    from synthetic_people
), actual as (
{% for call in rendered.calls %}
    {% if not loop.first %}union all{% endif %}
    select {{ loop.index }} as path,
        {% if loop.index <= 2 %}medication_order_id{% else %}medication_statement_id{% endif %} as id,
        person_id, sk_patient_id
    from ({{ call }})
{% endfor %}
), expected as (
    select paths.column1::number as path, people.id, -18301::number person_id,
        iff(people.id='SYN_CONFLICT','SYN_SK',null)::varchar sk_patient_id
    from (values (1),(2),(3),(4)) paths cross join synthetic_people people
)
(select * from actual minus select * from expected)
union all
(select * from expected minus select * from actual)
