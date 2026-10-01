-- Each latest clinical staging model represents every accepted source row once.
{% set sections = [
    'childhood_immunisation', 'previous_diagnosis', 'newborn_hearing_screening', 'blood_spot_result',
    'infant_physical_examination', 'provisional_diagnosis', 'primary_diagnosis', 'secondary_diagnosis'
] %}
{% for section in sections %}
select
    '{{ section }}' as section
    , (select count(*) from {{ ref('stg_csds_' ~ section ~ '_history') }}) as accepted_row_count
    , (select sum(accepted_source_record_count) from {{ ref('stg_csds_' ~ section) }}) as represented_row_count
where accepted_row_count is distinct from represented_row_count
{% if not loop.last %}union all{% endif %}
{% endfor %}
