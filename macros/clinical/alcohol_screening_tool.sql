{% macro alcohol_screening_tool(cluster_id, concept_code) -%}
    CASE
        WHEN {{ cluster_id }} = 'FAST_COD' THEN 'FAST'
        WHEN {{ cluster_id }} = 'AUDITC_COD' THEN 'AUDIT-C'
        WHEN {{ cluster_id }} = 'AUDIT_COD' THEN
            CASE WHEN {{ concept_code }} IN ('774089008', '774090004')
                THEN 'AUDIT-PC' ELSE 'AUDIT' END
    END
{%- endmacro %}
