{# Map a cleaned ASC CLD primary support reason to a high-level category. #}
{% macro asc_cld_primary_support_reason_category(primary_support_reason_cleaned_column) %}
    case
        when {{ primary_support_reason_cleaned_column }} is null
            then 'Unknown'

        when {{ primary_support_reason_cleaned_column }} in (
            'Physical support: Personal care support',
            'Physical support: Access and mobility only'
        )
            then 'Physical'

        when {{ primary_support_reason_cleaned_column }} = 'Learning disability support'
            then 'Learning disability'

        when {{ primary_support_reason_cleaned_column }} = 'Mental health support'
            then 'Mental Health'

        when {{ primary_support_reason_cleaned_column }} in (
            'Sensory support: Support for visual impairment',
            'Sensory support: Support for hearing impairment',
            'Sensory support: Support for dual impairment'
        )
            then 'Sensory'

        when {{ primary_support_reason_cleaned_column }} in (
            'Social support: Support to unpaid carer',
            'Social support: Support for social isolation or other',
            'Social support: Substance misuse support',
            'Social support: Asylum seeker support'
        )
            then 'Social'

        when {{ primary_support_reason_cleaned_column }} = 'Support with memory and cognition'
            then 'Memory/cognition'

        when {{ primary_support_reason_cleaned_column }} = 'Unknown'
            then 'Unknown'

        else 'Other'
    end
{% endmacro %}
