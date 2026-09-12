{#
    ICD-10 helpers for NHS Talking Therapies (IAPT data set) coded clinical entries.
    Codes arrive with or without the decimal point, and NHSD validated codes may keep a
    trailing dagger (D) or asterisk (A) marker (ETOS v2.1.22 IDS202 row 25).
#}

{% macro iapt_icd10_lookup_code(code) -%}
    {#- Uppercase letters and digits only, without the dagger or asterisk marker. -#}
    nullif(regexp_replace(
        regexp_replace(upper({{ code }}), '[^A-Z0-9]', ''),
        '^([A-Z][0-9]{2}([0-9X][0-9]?)?)[AD]$', '\\1'
    ), '')
{%- endmacro %}

{% macro iapt_icd10_precision(lookup_code) -%}
    {#- Precision of a code already passed through iapt_icd10_lookup_code. A fourth
        character X is the filler for an undivided three-character category
        (user guidance v1.6.1 p11). -#}
    case
        when {{ lookup_code }} is null then null
        when regexp_like({{ lookup_code }}, '[A-Z][0-9]{2}') then 'three_character'
        when regexp_like({{ lookup_code }}, '[A-Z][0-9]{2}X') then 'three_character_filler'
        when regexp_like({{ lookup_code }}, '[A-Z][0-9]{3}') then 'four_character'
        when regexp_like({{ lookup_code }}, '[A-Z][0-9]{2}[0-9X][0-9]') then 'five_character'
        else 'invalid_format'
    end
{%- endmacro %}
