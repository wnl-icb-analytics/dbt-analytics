{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['monthly-full']
    )
}}

-- Five annual positions at the month-end immediately before 1 August.
--
-- The anchors are literals because the Jinja loop names an output column
-- after each one. They are only valid while they sit inside the rolling
-- 60-month window that fct_person_segment_by_month holds, and that window
-- moves forward at every monthly full refresh. Adding a July position
-- requires the window to be widened first, and the earliest position expires
-- once it ages out: its columns then read null for every person.
-- tests/segmentation_annual_anchors_exist_in_monthly_history.sql fails when
-- a configured anchor is no longer in the parent. Keep its list in step with
-- this one.
{% set annual_snapshots = [
    ('2022-07-31', '2022_07_31'),
    ('2023-07-31', '2023_07_31'),
    ('2024-07-31', '2024_07_31'),
    ('2025-07-31', '2025_07_31'),
    ('2026-07-31', '2026_07_31')
] %}

WITH annual_rows AS (
    SELECT
        *,
        CASE
            WHEN segment_number IS NOT NULL
                THEN 'SEG' || segment_number::VARCHAR
            WHEN is_deceased THEN 'DEAD'
            WHEN NOT is_born THEN 'NOT_BORN'
            ELSE 'NOT_REGISTERED'
        END AS annual_status
    FROM {{ ref('fct_person_segment_by_month') }}
    WHERE end_date IN (
        {% for snapshot_date, suffix in annual_snapshots %}
            '{{ snapshot_date }}'{% if not loop.last %},{% endif %}
        {% endfor %}
    )
)

SELECT
    person_id,
    {% for snapshot_date, suffix in annual_snapshots %}
        MAX(IFF(end_date = '{{ snapshot_date }}', annual_status, NULL))
            AS status_{{ suffix }},
        MAX(IFF(end_date = '{{ snapshot_date }}', segment_number, NULL))
            AS segment_number_{{ suffix }},
        MAX(IFF(end_date = '{{ snapshot_date }}', is_born::INT, NULL))::BOOLEAN
            AS is_born_{{ suffix }},
        MAX(IFF(end_date = '{{ snapshot_date }}', is_alive::INT, NULL))::BOOLEAN
            AS is_alive_{{ suffix }},
        MAX(IFF(end_date = '{{ snapshot_date }}', is_registered::INT, NULL))::BOOLEAN
            AS is_registered_{{ suffix }},
        MAX(IFF(end_date = '{{ snapshot_date }}', is_active::INT, NULL))::BOOLEAN
            AS is_active_{{ suffix }},
        MAX(IFF(end_date = '{{ snapshot_date }}', is_deceased::INT, NULL))::BOOLEAN
            AS is_deceased_{{ suffix }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM annual_rows
GROUP BY person_id
