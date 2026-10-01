{#
    Schema name generation macro

    Environments (dev, test, prod) are handled at the database level instead of the schema level.

    Automatic schema derivation:
    - Staging schemas use the source-system folder name.
    - OLIDS subdomains use {DOMAIN}_{SUBDOMAIN}.
    - Other modelling and reporting schemas use the domain folder name.
    - Reference schemas use the reference subfolder name.
    - Explicit custom schemas take precedence.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {%- set path_parts = node.fqn -%}
        {%- set auto_schema_domains = var('auto_schema_domains', []) -%}

        {# Staging models: schema named after the source-system folder containing the model #}
        {%- if path_parts | length >= 4 and path_parts[1] == 'staging' -%}
            {{ path_parts[-2] | upper }}
        {# Auto-schema domains (olids): {DOMAIN}_{SUBDOMAIN} #}
        {%- elif path_parts | length >= 4 and path_parts[1] in ['modelling', 'reporting'] and path_parts[2] in auto_schema_domains -%}
            {%- set domain = path_parts[2] | upper -%}
            {%- set subdomain = path_parts[3] | upper -%}
            {{ domain ~ '_' ~ subdomain }}
        {# Other modelling/reporting domains: folder name is the schema (models/modelling/acute -> ACUTE) #}
        {%- elif path_parts | length >= 4 and path_parts[1] in ['modelling', 'reporting'] -%}
            {{ path_parts[2] | upper }}
        {# Reference layer: models/reference/<schema>/model.sql -> <SCHEMA> #}
        {%- elif path_parts | length >= 4 and path_parts[1] == 'reference' -%}
            {{ path_parts[2] | upper }}
        {# Published layer with auto-schema domains #}
        {%- elif path_parts | length >= 5 and path_parts[1] == 'published' and path_parts[3] in auto_schema_domains -%}
            {%- set domain = path_parts[3] | upper -%}
            {%- set subdomain = path_parts[4] | upper -%}
            {{ domain ~ '_' ~ subdomain }}
        {%- else -%}
            {{ default_schema }}
        {%- endif -%}

    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
