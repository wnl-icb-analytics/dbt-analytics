{% macro calculate_nice_lithium_therapy(reference_dates_relation) %}
{# Active lithium at each reference date, shared by the LTC population and IND256. #}
WITH lithium_reference_dates AS (
    SELECT reference_date FROM {{ reference_dates_relation }}
),
lithium_events AS (
    -- Known date caps both order and recorded dates without losing clinical ordering.
    SELECT
        person_id,
        TO_CHAR(order_date::DATE, 'YYYYMMDD') AS record_key,
        MIN({{ ltc_known_date('order_date', 'date_recorded') }}) AS known_date
    FROM {{ ref('int_lithium_medications_all') }}
    GROUP BY person_id, record_key
),

lithium_orders AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, known_date FROM lithium_events', 'lithium_reference_dates') }}
),

stop_events AS (
    SELECT
        person_id,
        TO_CHAR(clinical_effective_date::DATE, 'YYYYMMDD') AS record_key,
        MIN({{ ltc_known_date('clinical_effective_date', 'date_recorded') }}) AS known_date
    FROM {{ ref('int_lithium_stop_all') }}
    GROUP BY person_id, record_key
),

lithium_stops AS (
    {{ ltc_latest_known_record('SELECT person_id, record_key, known_date FROM stop_events', 'lithium_reference_dates') }}
),

lithium_therapy AS (
    SELECT
        orders.person_id,
        orders.reference_date AS reporting_date,
        TO_DATE(orders.record_key, 'YYYYMMDD') AS latest_lithium_order_date
    FROM lithium_orders AS orders
    LEFT JOIN lithium_stops AS stops
        ON orders.person_id = stops.person_id
        AND orders.reference_date = stops.reference_date
    -- Six calendar months, strict lower bound. A stop on the order day does not stop therapy.
    WHERE TO_DATE(orders.record_key, 'YYYYMMDD') > DATEADD(month, -6, orders.reference_date)
        AND (stops.record_key IS NULL OR stops.record_key <= orders.record_key)
)
SELECT person_id, reporting_date, latest_lithium_order_date FROM lithium_therapy
{% endmacro %}
