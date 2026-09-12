{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='end_date',
        cluster_by=['end_date', 'person_id'],
        tags=['efi2', 'monthly-full']
    )
}}

WITH scored AS (
    SELECT
        pl.person_id,
        pl.end_date,
        COALESCE(score.efi, 0) AS efi_score,
        FLOOR(DATEDIFF('month', pl.date_of_birth, pl.end_date) / 12)
            AS age_at_end,
        pl.gender,
        pl.date_of_death
    FROM {{ ref('int_efi2_patient_list_history') }} AS pl
    LEFT JOIN {{ ref('int_efi2_chronology_history') }} AS score
        ON pl.person_id = score.person_id
        AND pl.end_date = score.end_date
    {% if is_incremental() %}
    WHERE {{ rebuild_month_window('pl.end_date', 'end_date') }}
    {% endif %}
)

SELECT
    person_id,
    end_date,
    efi_score,
    {{ efi2_category('efi_score') }} AS category,
    age_at_end,
    gender,
    date_of_death
FROM scored
