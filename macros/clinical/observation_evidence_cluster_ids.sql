{% macro observation_evidence_cluster_ids(consumer='all', source=none) %}
    {%- set lists = {
        'dietary_review': {
            'ECL_CACHE': ['DM_DIET_REVIEW_COD'],
            'PCD': ['NUTRIASS_COD'],
            'OPENCODELISTS': ['NHSD_PRIMARY_CARE_DOMAIN_REFSETS/DIETITIAN_COD']
        },
        'medication_review': {'ECL_CACHE': ['SMI_LONGER_LIVES_MEDICINES_REVIEW']},
        'weight_management': {
            'all': ['SMI_LONGER_LIVES_WEIGHT_MANAGEMENT', 'WTMGINT_COD', 'WTMGINTDEC_COD'],
            'PCD': ['EXERCISEINT_COD']
        },
        'dental_inspection': {'all': ['SMI_LONGER_LIVES_DENTAL', 'ORALHEALTHREV_COD', 'ORALHEALTHINT_COD']}
    } -%}
    {%- set cluster_ids = [] -%}
    {%- for name, sources in lists.items() if consumer == 'all' or consumer == name -%}
        {%- for list_source, ids in sources.items() if source is none or source == list_source -%}
            {%- do cluster_ids.extend(ids) -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- if not cluster_ids -%}
        {{ exceptions.raise_compiler_error('No evidence lists for ' ~ consumer ~ ' / ' ~ source) }}
    {%- endif -%}
    {{ return("'" ~ cluster_ids | join("','") ~ "'") }}
{% endmacro %}
