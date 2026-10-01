{# Standardise Adult Social Care CLD primary support reason values to a canonical label.
   Handles casing, colon vs slash separators, ampersands, and truncated submissions. #}
{% macro clean_asc_cld_primary_support_reason(primary_support_reason_column) %}
    {%- set normalized -%}
        regexp_replace(
            regexp_replace(
                lower(trim({{ primary_support_reason_column }})),
                '\\s+/\\s+',
                ': '
            ),
            '\\s*&\\s*',
            ' and '
        )
    {%- endset -%}

    case
        when {{ primary_support_reason_column }} is null then null

        when {{ normalized }} in (
            'physical support: personal care support',
            'physical support: personal care and support'
        )
            then 'Physical support: Personal care support'

        when {{ normalized }} = 'physical support: access and mobility only'
            then 'Physical support: Access and mobility only'

        when {{ normalized }} like 'learning disability support%'
            then 'Learning disability support'

        when {{ normalized }} like 'mental health support%'
            then 'Mental health support'

        when {{ normalized }} in ('unknown', 'psr not known')
            then 'Unknown'

        when {{ normalized }} = 'support with memory and cognition'
            then 'Support with memory and cognition'

        when {{ normalized }} = 'social support: support to unpaid carer'
            then 'Social support: Support to unpaid carer'

        when {{ normalized }} in (
            'social support: support for social isolation or other r',
            'social support: support for social isolation/other',
            'social support: support for social isolation or other',
            'social support: support for social isolation: other'
        )
            or {{ normalized }} like 'social support: support for social isolation%'
            then 'Social support: Support for social isolation or other'

        when {{ normalized }} = 'sensory support: support for visual impairment'
            then 'Sensory support: Support for visual impairment'

        when {{ normalized }} = 'sensory support: support for hearing impairment'
            then 'Sensory support: Support for hearing impairment'

        when {{ normalized }} = 'social support: substance misuse support'
            then 'Social support: Substance misuse support'

        when {{ normalized }} = 'sensory support: support for dual impairment'
            then 'Sensory support: Support for dual impairment'

        when {{ normalized }} = 'social support: asylum seeker support'
            then 'Social support: Asylum seeker support'

        else trim({{ primary_support_reason_column }})
    end
{% endmacro %}
