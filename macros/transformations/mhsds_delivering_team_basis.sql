{% macro mhsds_delivering_team_basis(additional_local_id, legacy_local_id, primary_local_id, specification_version) -%}
case
    -- Source-derived identifiers alone do not establish a submitted team pointer.
    when {{ additional_local_id }} is not null then 'contact_additional_team'
    when {{ legacy_local_id }} is not null then 'contact_legacy_team'
    when try_to_decimal({{ specification_version }}::varchar, 10, 2) >= 6
        and try_to_decimal({{ specification_version }}::varchar, 10, 2) < 7
        and {{ primary_local_id }} is not null then 'referral_primary_team'
    else 'unresolved'
end
{%- endmacro %}
