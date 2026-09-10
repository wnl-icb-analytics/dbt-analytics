{% macro get_observations(cluster_ids, source=none, include_history=false, versioned=false) %}
    {#- versioned=true reads every published version of the UKHSA cluster from
        stg_reference_ukhsa_codecluster_versions instead of the current one in
        COMBINED_CODESETS, and adds spec_version to the output so the caller can pin
        each campaign to the version it was reported under. Only valid with a UKHSA
        source. include_history is not supported with versioned. -#}
    {%- if cluster_ids is none or cluster_ids|trim == '' -%}
        {{ exceptions.raise_compiler_error("Must provide a non-empty cluster_ids parameter to get_observations macro") }}
    {%- endif -%}
    {%- if versioned and (source is none or not source.startswith('UKHSA_')) -%}
        {{ exceptions.raise_compiler_error("get_observations(versioned=true) needs a UKHSA source") }}
    {%- endif -%}
    {%- if versioned and include_history -%}
        {{ exceptions.raise_compiler_error("get_observations does not support include_history with versioned=true") }}
    {%- endif -%}
    -- Join pre-mapped observations with cluster definitions for flexibility
    WITH cluster_codes AS (
        SELECT DISTINCT
            code as mapped_concept_code,
            cluster_id,
            cluster_description,
            code_description{% if versioned %},
            spec_version{% endif %}
        FROM {% if versioned %}{{ ref('stg_reference_ukhsa_codecluster_versions') }}{% else %}{{ ref('stg_reference_combined_codesets') }}{% endif %}
        WHERE UPPER(cluster_id) IN ({{ cluster_ids|upper }})
        {% if source %}
          AND source = '{{ source }}'
        {% endif %}
    ){% if include_history %},

    -- Expand cluster codes with historical predecessors from SCT_History
    -- Picks up retired SNOMED codes that map to current cluster codes
    historical_codes AS (
        SELECT DISTINCT
            h.old_concept_id::VARCHAR AS mapped_concept_code,
            cc.cluster_id,
            cc.cluster_description,
            cc.code_description
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
        o.id,
        o.patient_id,
        o.person_id,
        -- Use date_recorded as fallback if clinical_effective_date is after it (data quality fix)
        CASE
            WHEN o.clinical_effective_date > o.date_recorded THEN o.date_recorded
            ELSE COALESCE(o.clinical_effective_date, '1900-01-01')
        END AS clinical_effective_date,
        o.clinical_effective_date AS clinical_effective_date_raw,  -- Original value for audit
        o.date_recorded,
        o.result_value,
        o.result_units_source_concept_id,
        o.result_unit_code,
        o.result_unit_display,
        o.result_text,
        o.allergy_medication_name,
        o.is_problem,
        o.is_review,
        o.problem_end_date,
        o.mapped_concept_id,
        o.mapped_concept_code,
        o.mapped_concept_display,
        o.episodicity_source_concept_id,
        o.age_at_event,
        o.lds_transform_datetime,
        cc.cluster_id,
        cc.cluster_description,
        cc.code_description{% if versioned %},
        cc.spec_version{% endif %}
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {% if include_history %}expanded_codes{% else %}cluster_codes{% endif %} cc
        ON o.mapped_concept_code = cc.mapped_concept_code
    -- Deduplicate: preserve legitimate cross-cluster duplicates but remove within-cluster duplicates
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.id, cc.cluster_id{% if versioned %}, cc.spec_version{% endif %}
        ORDER BY o.mapped_concept_code
    ) = 1

{% endmacro %}
