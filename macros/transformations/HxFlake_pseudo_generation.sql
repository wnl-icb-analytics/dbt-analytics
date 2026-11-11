{% macro hxflake_pseudo_generation(sk_patient_id) %}
LEFT(
    SUBSTR(
        TRIM(
            REVERSE(
                RIGHT(
                    LPAD(
                        TO_CHAR(TO_NUMBER({{ sk_patient_id }}), 'XXXXXXXXX'),
                        9,
                        0
                    ),
                    10
                )
            )
        ) || '0000000000',
        1,
        2
    ) || '-' || RPAD(
        SUBSTR(
            TRIM(
                REVERSE(
                    RIGHT(
                        LPAD(
                            TO_CHAR(TO_NUMBER({{ sk_patient_id }}), 'XXXXXXXXX'),
                            9,
                            0
                        ),
                        10
                    )
                )
            ) || '0000000000',
            3,
            4
        ),
        3,
        '0'
    ) || '-' || SUBSTR(
        TRIM(
            REVERSE(
                RIGHT(
                    LPAD(
                        TO_CHAR(TO_NUMBER({{ sk_patient_id }}), 'XXXXXXXXX'),
                        9,
                        0
                    ),
                    10
                )
            )
        ) || '0000000000',
        6,
        3
    ) || '0000000000',
    10
)
{% endmacro %}

{# Inverse of hxflake_pseudo_generation. Exact while sk_patient_id is below
   16^8 (eight hex digits); the forward macro drops any higher digit. Returns
   null for a malformed pseudonym. #}
{% macro hxflake_to_sk_patient_id(hx_flake) %}
    try_to_number(reverse(replace({{ hx_flake }}, '-', '')), 'XXXXXXXX')::varchar
{% endmacro %}
