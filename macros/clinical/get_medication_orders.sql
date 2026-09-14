{% macro get_medication_orders(bnf_code=none, cluster_id=none, source=none, include_history=false, versioned=false) %}
    -- Optional source parameter to filter to specific refset (e.g., 'LTC_LCS')
    -- include_history=true expands cluster codes with retired SNOMED predecessors via SCT_History
    -- versioned=true reads every published version of a UKHSA cluster from
    -- stg_reference_ukhsa_codecluster_versions and adds spec_version to the output, so the
    -- caller can pin each campaign to the version it was reported under (UKHSA sources only)
{% if versioned and (source is none or not source.startswith('UKHSA_')) %}
    {{ exceptions.raise_compiler_error("get_medication_orders(versioned=true) needs a UKHSA source") }}
{% endif %}
{% if versioned and include_history %}
    {{ exceptions.raise_compiler_error("get_medication_orders does not support include_history with versioned=true") }}
{% endif %}
    --
    -- BNF filtering uses pre-computed columns from stg_olids_medication_order
    -- (clustered on bnf_chapter, mapped_concept_code in dbt-olids), so 2/4-char
    -- prefixes prune to a tiny number of partitions automatically.
    {% if bnf_code is none and cluster_id is none %}
    {{ exceptions.raise_compiler_error("Must provide either bnf_code or cluster_id parameter to get_medication_orders macro") }}
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
        {{ exceptions.raise_compiler_error("get_medication_orders requires non-empty cluster_id values") }}
    {% endif %}
    {% set cluster_ids_str = cleaned_cluster_ids | join("','") %}
{% endif %}

{# Build the BNF prefix filter: pick the cluster-key column that matches the prefix length #}
{% set bnf_filter = '' %}
{% if bnf_code is not none %}
    {% set bnf_prefix = bnf_code | string | trim %}
    {% if bnf_prefix | length == 2 %}
        {% set bnf_filter = "AND mo.bnf_chapter = '" ~ bnf_prefix ~ "'" %}
    {% elif bnf_prefix | length == 4 %}
        {% set bnf_filter = "AND mo.bnf_section = '" ~ bnf_prefix ~ "'" %}
    {% elif bnf_prefix | length >= 15 %}
        {% set bnf_filter = "AND mo.bnf_code = '" ~ bnf_prefix ~ "'" %}
    {% else %}
        {# Mid-length prefix (e.g. 6 chars for paragraph): use chapter for pruning + LIKE for precision #}
        {% set bnf_filter = "AND mo.bnf_chapter = '" ~ bnf_prefix[:2] ~ "' AND mo.bnf_code LIKE '" ~ bnf_prefix ~ "%'" %}
    {% endif %}
{% endif %}

    {%- if cluster_id is not none -%}
    -- Join pre-mapped medication orders with cluster definitions
    WITH cluster_codes AS (
        SELECT DISTINCT
            code as mapped_concept_code,
            cluster_id,
            cluster_description{% if versioned %},
            spec_version{% endif %}
        FROM {% if versioned %}{{ ref('stg_reference_ukhsa_codecluster_versions') }}{% else %}{{ ref('stg_reference_combined_codesets') }}{% endif %}
        WHERE UPPER(cluster_id) IN ('{{ cluster_ids_str }}')
        {% if source is not none %}
        AND source = '{{ source }}'
        {% endif %}
    ){% if include_history %},

    historical_codes AS (
        SELECT DISTINCT
            h.old_concept_id::VARCHAR AS mapped_concept_code,
            cc.cluster_id,
            cc.cluster_description
        FROM {{ ref('stg_nhsd_snomed_sct_history') }} h
        INNER JOIN cluster_codes cc
            ON h.new_concept_id::VARCHAR = cc.mapped_concept_code
        WHERE h.old_concept_id::VARCHAR NOT IN (
            SELECT mapped_concept_code FROM cluster_codes
        )
    ),

    expanded_codes AS (
        SELECT * FROM cluster_codes
        UNION ALL
        SELECT * FROM historical_codes
    ){% endif %}

    SELECT
        mo.id AS medication_order_id,
        mo.medication_statement_id,
        mo.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        mo.clinical_effective_date AS order_date,
        mo.medication_name AS order_medication_name,
        mo.date_recorded,
        mo.dose AS order_dose,
        mo.quantity_value AS order_quantity_value,
        mo.quantity_unit AS order_quantity_unit,
        mo.duration_days AS order_duration_days,
        mo.estimated_cost,
        mo.statement_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        cc.cluster_id,{% if versioned %}
        cc.spec_version,{% endif %}
        mo.bnf_chapter,
        mo.bnf_section,
        mo.bnf_code,
        mo.bnf_name
    FROM {{ ref('stg_olids_medication_order') }} mo
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    INNER JOIN {% if include_history %}expanded_codes{% else %}cluster_codes{% endif %} cc
        ON mo.mapped_concept_code = cc.mapped_concept_code
    WHERE mo.clinical_effective_date IS NOT NULL
    {{ bnf_filter }}
    {%- else -%}
    -- BNF-only path: prune via mo.bnf_chapter / bnf_section / bnf_code (clustered upstream)
    SELECT
        mo.id AS medication_order_id,
        mo.medication_statement_id,
        mo.patient_id,
        pp.person_id,
        pp.sk_patient_id,
        mo.clinical_effective_date AS order_date,
        mo.medication_name AS order_medication_name,
        mo.date_recorded,
        mo.dose AS order_dose,
        mo.quantity_value AS order_quantity_value,
        mo.quantity_unit AS order_quantity_unit,
        mo.duration_days AS order_duration_days,
        mo.estimated_cost,
        mo.statement_medication_name,
        mo.mapped_concept_code,
        mo.mapped_concept_display,
        NULL AS cluster_id,
        mo.bnf_chapter,
        mo.bnf_section,
        mo.bnf_code,
        mo.bnf_name
    FROM {{ ref('stg_olids_medication_order') }} mo
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON mo.patient_id = pp.patient_id
    WHERE mo.clinical_effective_date IS NOT NULL
    {{ bnf_filter }}
    {%- endif -%}
{% endmacro %}
