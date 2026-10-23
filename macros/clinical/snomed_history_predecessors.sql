{% macro snomed_history_predecessors(cluster_codes, carry_columns=['cluster_description']) %}
    {#- Retired SNOMED predecessors from the NHS England history substitution table.
        Ambiguity is assessed across every history row for the old concept.
        Ambiguous predecessors require every listed successor in the same cluster.
        Output order matches cluster_codes for UNION ALL. -#}
    SELECT DISTINCT
        h.old_concept_id::VARCHAR AS mapped_concept_code,
        cc.cluster_id
        {%- for col in carry_columns %},
        cc.{{ col }}
        {%- endfor %}
    FROM {{ ref('stg_nhsd_snomed_sct_history') }} AS h
    INNER JOIN {{ cluster_codes }} AS cc
        ON h.new_concept_id::VARCHAR = cc.mapped_concept_code
    INNER JOIN (
        SELECT
            old_concept_id,
            BOOLOR_AGG(is_ambiguous) AS is_ambiguous,
            COUNT(DISTINCT new_concept_id) AS successor_count
        FROM {{ ref('stg_nhsd_snomed_sct_history') }}
        GROUP BY old_concept_id
    ) AS predecessor
        ON h.old_concept_id = predecessor.old_concept_id
    INNER JOIN (
        SELECT
            h2.old_concept_id,
            cc2.cluster_id,
            COUNT(DISTINCT h2.new_concept_id) AS successors_in_cluster
        FROM {{ ref('stg_nhsd_snomed_sct_history') }} AS h2
        INNER JOIN {{ cluster_codes }} AS cc2
            ON h2.new_concept_id::VARCHAR = cc2.mapped_concept_code
        GROUP BY h2.old_concept_id, cc2.cluster_id
    ) AS coverage
        ON h.old_concept_id = coverage.old_concept_id
        AND cc.cluster_id = coverage.cluster_id
    -- A direct code in any requested cluster cannot enter another through history.
    -- This stops resolution codes entering diagnosis clusters.
    WHERE h.old_concept_id::VARCHAR NOT IN (SELECT mapped_concept_code FROM {{ cluster_codes }})
        AND (
            NOT predecessor.is_ambiguous
            OR coverage.successors_in_cluster = predecessor.successor_count
        )
{%- endmacro %}
