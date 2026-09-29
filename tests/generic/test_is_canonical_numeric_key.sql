{#
  Asserts that a patient surrogate key, though stored and joined as VARCHAR,
  holds a value whose string form is identical to its numeric form.

  HxFlake_pseudo_generation reinterprets the key as a number and are unsafe otherwise.

  Test designed to catch 2 scenarios:
  1. A non-numeric value silently nulls ('+70', '70.0') 
  2. Distinct keys (e.g., '070', '70', '70.0') collapse onto one signature and one hx_flake
  
  Verified clean at the time of writing (2026-09-04); the test exists so a feed
  change cannot reintroduce it unnoticed.
#}
{% test is_canonical_numeric_key(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and (
          try_to_number({{ column_name }}::varchar) is null
          or {{ column_name }}::varchar <> try_to_number({{ column_name }}::varchar)::varchar
          or try_to_number({{ column_name }}::varchar) not between 0 and 4294967295
      )
{% endtest %}