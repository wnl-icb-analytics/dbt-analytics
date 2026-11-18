-- Headers must not win result selection; genuine later empty entries must still win.
{% set permitted = ltc_lcs_permitted_mapped_codes("'synthetic'", "'SYNTHETIC'") %}
{% set calculations = namespace(queries=[]) %}
{% set queries = [
    get_ltc_lcs_observations('synthetic'),
    get_ltc_lcs_observations('synthetic', exclude_result_headers=true),
    get_ltc_lcs_observations_latest('synthetic'),
    get_ltc_lcs_latest_record_in_set('synthetic', 'synthetic')
] %}
{% for query in queries %}
    {% set sql = namespace(text=query | replace(permitted, "select 'mapped' as snomed_code, 'synthetic' as valueset_id")) %}
    {% for model, fixture in {
        'stg_olids_observation': 'synthetic_observations',
        'stg_olids_enriched_concept_map': 'synthetic_concepts',
        'stg_reference_ltc_lcs_original_codes': 'synthetic_original_codes',
        'stg_reference_ltc_lcs_valuesets': 'synthetic_valuesets'
    }.items() %}
        {% set sql.text = sql.text | replace(ref(model) | string, fixture) %}
    {% endfor %}
    {% do calculations.queries.append(sql.text) %}
{% endfor %}

with synthetic_observations as (
    select column1::varchar as id, column2::number as person_id,
        column3::date as clinical_effective_date, column3::timestamp_ntz as date_recorded,
        column4::float as result_value, column5::varchar as result_text,
        column6::boolean as is_result_header, column7::varchar as mapped_concept_code,
        'synthetic-patient' as patient_id,
        iff(column7 is null, 'source-concept', null)::varchar as observation_source_concept_id,
        null::varchar as result_units_source_concept_id, null::varchar as result_unit_display,
        null::varchar as allergy_medication_name, false as is_problem, false as is_review,
        null::date as problem_end_date, null::varchar as mapped_concept_id,
        null::varchar as mapped_concept_display, null::varchar as episodicity_source_concept_id
    from values
        ('header', -1, '2026-01-02', null, null, true, 'mapped'),
        ('child', -1, '2026-01-02', 91, null, false, 'mapped'),
        ('older', -2, '2026-01-01', 92, null, false, 'mapped'),
        ('empty', -2, '2026-01-03', null, null, false, 'mapped'),
        ('text', -3, '2026-01-03', null, 'result text', false, 'mapped'),
        ('source-header', -4, '2026-01-02', null, null, true, null),
        ('source-child', -4, '2026-01-02', 93, null, false, null)
),
synthetic_concepts as (
    select 'source-concept' as source_concept_id, 'source-code' as source_code
),
synthetic_original_codes as (
    select 'synthetic' as valueset_id, 'source-code' as original_code
),
synthetic_valuesets as (
    select 'synthetic' as valueset_id, 'synthetic' as valueset_friendly_name
),
all_observations as ({{ calculations.queries[0] }}),
result_observations as ({{ calculations.queries[1] }}),
latest as ({{ calculations.queries[2] }}),
latest_in_set as ({{ calculations.queries[3] }}),
expected as (
    select column1::number as person_id, column2::varchar as observation_id
    from values (-1, 'child'), (-2, 'empty'), (-3, 'text'), (-4, 'source-child')
),
differences as (
    (select person_id, observation_id from latest minus select * from expected)
    union all
    (select * from expected minus select person_id, observation_id from latest)
    union all
    (select person_id, observation_id from latest_in_set minus select * from expected)
    union all
    (select * from expected minus select person_id, observation_id from latest_in_set)
)
select 'latest result differs' as failure
where exists (select 1 from differences)
union all
select 'default matcher removed headers'
where (select count(*) from all_observations where observation_id in ('header', 'source-header')) <> 2
union all
select 'result matcher retained headers'
where exists (select 1 from result_observations where observation_id in ('header', 'source-header'))
