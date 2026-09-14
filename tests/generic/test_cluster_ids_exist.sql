{% test cluster_ids_exist(model, cluster_ids=none, source=none, versioned=false, arguments=none) %}
    -- Generic test to verify that specified cluster IDs exist in codesets.
    -- Pass `source` when the model reads clusters from one terminology source. The same
    -- concept is named differently between sources - UKHSA_COVID has CKD15_COD where
    -- UKHSA_FLU has CKD_15_COD - so a cluster that exists somewhere is not evidence that
    -- it exists where the model looks for it, and get_observations filters on source.
    -- Pass `versioned: true` for models that read get_observations(..., versioned=true):
    -- the cluster must then exist in stg_reference_ukhsa_codecluster_versions for the
    -- source in at least one published version, which covers clusters UKHSA has since
    -- retired but closed campaigns still pin to, such as the flu LONGRES_COD.
    {%- if arguments and arguments.cluster_ids -%}
        {%- set cluster_ids = arguments.cluster_ids -%}
    {%- endif %}
    {%- if arguments and arguments.source -%}
        {%- set source = arguments.source -%}
    {%- endif %}
    {%- if arguments and arguments.versioned -%}
        {%- set versioned = arguments.versioned -%}
    {%- endif %}
    {%- set codeset_ref = ref('stg_reference_ukhsa_codecluster_versions') if versioned else ref('stg_reference_combined_codesets') %}

    WITH required_clusters AS (
        SELECT UPPER(TRIM(value)) AS cluster_id
        FROM TABLE(SPLIT_TO_TABLE(
            '{{ cluster_ids }}',
            ','
        ))
    )
    SELECT
        rc.cluster_id,
        {% if source %}'{{ source }}'{% else %}'any'{% endif %} AS expected_source,
        'Cluster ID not found in {{ codeset_ref.identifier }} for the expected source' AS failure_reason
    FROM required_clusters rc
    WHERE rc.cluster_id NOT IN (
        SELECT DISTINCT UPPER(cluster_id)
        FROM {{ codeset_ref }}
        {% if source %}WHERE source = '{{ source }}'{% endif %}
    )
{% endtest %}
