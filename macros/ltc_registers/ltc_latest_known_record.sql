{% macro ltc_latest_known_record(events_sql, reference_dates_cte='reference_dates') %}
{#-
    For each person and reference date, the record_key of the latest record known
    by that date: the highest record_key among records whose known_date is on or
    before the reference date.

    Each person's records are ordered by the date they became known, and the
    latest record so far is carried forward as an interval that lasts until the
    next record becomes known. Each interval is joined to the reference dates it
    covers, so a person and date are evaluated once instead of every record being
    joined to every later date.

    Args:
        events_sql           query returning person_id, record_key (sortable text;
                             the latest record has the highest key) and known_date
        reference_dates_cte  name of a CTE in scope with a reference_date column

    Returns: reference_date, person_id, record_key
-#}
    WITH known_records AS (
        {{ events_sql }}
    ),

    running_latest AS (
        SELECT
            person_id,
            known_date,
            record_key,
            MAX(record_key) OVER (
                PARTITION BY person_id
                ORDER BY known_date, record_key
                ROWS UNBOUNDED PRECEDING
            ) AS latest_record_key
        FROM known_records
    ),

    latest_by_known_date AS (
        -- The last record known on each date carries the latest key up to that date.
        SELECT
            person_id,
            known_date,
            latest_record_key
        FROM running_latest
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY person_id, known_date ORDER BY record_key DESC
        ) = 1
    ),

    intervals AS (
        SELECT
            person_id,
            known_date,
            latest_record_key,
            LEAD(known_date) OVER (PARTITION BY person_id ORDER BY known_date) AS next_known_date
        FROM latest_by_known_date
    )

    SELECT
        ref_date.reference_date,
        iv.person_id,
        iv.latest_record_key AS record_key
    FROM intervals AS iv
    INNER JOIN {{ reference_dates_cte }} AS ref_date
        ON ref_date.reference_date >= iv.known_date
        AND (iv.next_known_date IS NULL OR ref_date.reference_date < iv.next_known_date)
{% endmacro %}


{% macro ltc_known_date(clinical_date_column, recorded_date_column) %}
{#- Date a record became known: the later of its clinical and recorded dates. -#}
    GREATEST(
        CAST({{ clinical_date_column }} AS DATE),
        COALESCE(CAST({{ recorded_date_column }} AS DATE), CAST({{ clinical_date_column }} AS DATE))
    )
{% endmacro %}
