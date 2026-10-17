{% macro get_medication_statements(bnf_code=none, cluster_id=none, source=none, include_history=false) %}
    -- Optional source parameter to filter to specific refset (e.g., 'LTC_LCS')
    -- include_history=true expands cluster codes with retired SNOMED predecessors via SCT_History
    --
    -- BNF filtering uses pre-computed columns from stg_olids_medication_statement
    -- (clustered on bnf_chapter, mapped_concept_code in dbt-olids), so 2/4-char
    -- prefixes prune to a tiny number of partitions automatically.
    {% if bnf_code is none and cluster_id is none %}
    {{ exceptions.raise_compiler_error("Must provide either bnf_code or cluster_id parameter to get_medication_statements macro") }}
{% endif %}

{# Accept cluster_id as string or list, normalise to a clean upper-cased token list #}
{% set cluster_ids_str = '' %}
{% if cluster_id is not none %}
    {% if cluster_id is string and ',' in cluster_id %}
        {% set cluster_ids = cluster_id.replace("'", "").split(",") %}
    {% elif cluster_id is string %}
        {% set cluster_ids = [cluster_id] %}
    {% else %}
        {% set cluster_ids = cluster_id %}
    {% endif %}
    {% set cleaned_cluster_ids = [] %}
    {% for c in cluster_ids %}
        {% set token = c | string | replace("'", "") | replace('"', '') | trim | upper %}
        {% if token %}{% do cleaned_cluster_ids.append(token) %}{% endif %}
    {% endfor %}
    {% if cleaned_cluster_ids | length == 0 %}
        {{ exceptions.raise_compiler_error("get_medication_statements requires non-empty cluster_id values") }}
    {% endif %}
    {% set cluster_ids_str = cleaned_cluster_ids | join("','") %}
{% endif %}

{# Build the BNF prefix filter: pick the cluster-key column that matches the prefix length #}
{% set bnf_filter = '' %}
{% if bnf_code is not none %}
    {% set bnf_prefix = bnf_code | string | trim %}
    {% if bnf_prefix | length == 2 %}
        {% set bnf_filter = "AND ms.bnf_chapter = '" ~ bnf_prefix ~ "'" %}
    {% elif bnf_prefix | length == 4 %}
        {% set bnf_filter = "AND ms.bnf_section = '" ~ bnf_prefix ~ "'" %}
    {% elif bnf_prefix | length >= 15 %}
        {% set bnf_filter = "AND ms.bnf_code = '" ~ bnf_prefix ~ "'" %}
    {% else %}
        {% set bnf_filter = "AND ms.bnf_chapter = '" ~ bnf_prefix[:2] ~ "' AND ms.bnf_code LIKE '" ~ bnf_prefix ~ "%'" %}
    {% endif %}
{% endif %}

    {%- if cluster_id is not none -%}
    -- Join pre-mapped medication statements with cluster definitions
    WITH cluster_codes AS (
        SELECT DISTINCT
            code as mapped_concept_code,
            cluster_id,
            cluster_description
        FROM {{ ref('stg_reference_combined_codesets') }}
        WHERE UPPER(cluster_id) IN ('{{ cluster_ids_str }}')
        {% if source is not none %}
        AND source = '{{ source }}'
        {% endif %}
    ){% if include_history %},

    historical_codes AS (
        {{ snomed_history_predecessors('cluster_codes') }}
    ),

    expanded_codes AS (
        SELECT * FROM cluster_codes
        UNION ALL
        SELECT * FROM historical_codes
    ){% endif %}

    SELECT
        ms.id AS medication_statement_id,
        ms.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        ms.clinical_effective_date AS statement_date,
        ms.date_recorded,
        ms.medication_name AS statement_medication_name,
        ms.dose AS statement_dose,
        ms.quantity_value AS statement_quantity_value,
        ms.quantity_unit AS statement_quantity_unit,
        ms.authorisation_type_code,
        ms.authorisation_type_display,
        ms.issue_method,
        ms.is_active,
        ms.cancellation_date,
        ms.expiry_date,
        ms.age_at_event,
        ms.mapped_concept_code,
        ms.mapped_concept_display,
        cc.cluster_id,
        ms.bnf_chapter,
        ms.bnf_section,
        ms.bnf_code,
        ms.bnf_name
    FROM {{ ref('stg_olids_medication_statement') }} ms
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON ms.patient_id = pp.patient_id
    INNER JOIN {% if include_history %}expanded_codes{% else %}cluster_codes{% endif %} cc
        ON ms.mapped_concept_code = cc.mapped_concept_code
    WHERE ms.clinical_effective_date IS NOT NULL
    {{ bnf_filter }}
    {%- else -%}
    -- BNF-only path: prune via ms.bnf_chapter / bnf_section / bnf_code (clustered upstream)
    SELECT
        ms.id AS medication_statement_id,
        ms.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        ms.clinical_effective_date AS statement_date,
        ms.date_recorded,
        ms.medication_name AS statement_medication_name,
        ms.dose AS statement_dose,
        ms.quantity_value AS statement_quantity_value,
        ms.quantity_unit AS statement_quantity_unit,
        ms.authorisation_type_code,
        ms.authorisation_type_display,
        ms.issue_method,
        ms.is_active,
        ms.cancellation_date,
        ms.expiry_date,
        ms.age_at_event,
        ms.mapped_concept_code,
        ms.mapped_concept_display,
        NULL AS cluster_id,
        ms.bnf_chapter,
        ms.bnf_section,
        ms.bnf_code,
        ms.bnf_name
    FROM {{ ref('stg_olids_medication_statement') }} ms
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON ms.patient_id = pp.patient_id
    WHERE ms.clinical_effective_date IS NOT NULL
    {{ bnf_filter }}
    {%- endif -%}
{% endmacro %}
