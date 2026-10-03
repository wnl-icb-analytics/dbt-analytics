{{ config(tags=['monthly-full', 'nice-history']) }}

{% set query = calculate_nice_copd_observations() %}
{% set query = query | replace(ref('stg_reference_combined_codesets') | string, 'synthetic_input_0') %}
{% set query = query | replace(ref('stg_nhsd_snomed_sct_history') | string, 'synthetic_input_1') %}
{% set query = query | replace(ref('stg_olids_observation') | string, 'synthetic_input_2') %}

WITH synthetic_input_0 AS (
  SELECT
    CAST(column1 AS VARCHAR) AS code,
    CAST(column2 AS VARCHAR) AS cluster_id,
    CAST(column3 AS VARCHAR) AS source,
    CAST('Synthetic referral' AS VARCHAR) AS cluster_description,
    CAST('Synthetic referral' AS VARCHAR) AS code_description
  FROM (VALUES
    ('SYNTHETIC_CURRENT', 'PULRHBOFF_COD', 'PCD'),
    ('SYNTHETIC_OTHER_SOURCE', 'PULRHBOFF_COD', 'ECL_CACHE'),
    ('SYNTHETIC_DECLINE', 'PULRHBDEC_COD', 'PCD'))
), synthetic_input_1 AS (
  SELECT
    CAST(column1 AS VARCHAR) AS old_concept_id,
    CAST(column2 AS VARCHAR) AS new_concept_id
  FROM (VALUES
    ('SYNTHETIC_PREDECESSOR', 'SYNTHETIC_CURRENT'),
    ('SYNTHETIC_PREDECESSOR', 'SYNTHETIC_CURRENT'),
    ('SYNTHETIC_OTHER_PREDECESSOR', 'SYNTHETIC_OTHER_SOURCE'))
), synthetic_input_2 AS (
  SELECT
    CAST(column1 AS DECIMAL(38, 0)) AS id,
    CAST(column1 AS DECIMAL(38, 0)) AS person_id,
    CAST(column2 AS VARCHAR) AS mapped_concept_code,
    CAST('2026-09-01' AS DATE) AS clinical_effective_date,
    CAST('2026-09-01' AS DATE) AS date_recorded,
    CAST(NULL AS VARCHAR) AS patient_id,
    CAST(NULL AS VARCHAR) AS result_value,
    CAST(NULL AS VARCHAR) AS result_units_source_concept_id,
    CAST(NULL AS VARCHAR) AS result_unit_code,
    CAST(NULL AS VARCHAR) AS result_unit_display,
    CAST(NULL AS VARCHAR) AS result_text,
    CAST(NULL AS VARCHAR) AS allergy_medication_name,
    CAST(NULL AS VARCHAR) AS is_problem,
    CAST(NULL AS VARCHAR) AS is_review,
    CAST(NULL AS VARCHAR) AS problem_end_date,
    CAST(NULL AS VARCHAR) AS mapped_concept_id,
    CAST(NULL AS VARCHAR) AS mapped_concept_display,
    CAST(NULL AS VARCHAR) AS episodicity_source_concept_id,
    CAST(NULL AS VARCHAR) AS age_at_event,
    CAST(NULL AS VARCHAR) AS lds_transform_datetime
  FROM (VALUES
    (-9751, 'SYNTHETIC_PREDECESSOR'),
    (-9752, 'SYNTHETIC_OTHER_PREDECESSOR'),
    (-9753, 'SYNTHETIC_DECLINE'))
), actual AS (
  SELECT
    person_id,
    observation_id,
    event_date,
    evidence_type
  FROM (
    {{ query }}
  )
), expected AS (
  SELECT
    -CAST(9751 AS DECIMAL(38, 0)) AS person_id,
    -CAST(9751 AS DECIMAL(38, 0)) AS observation_id,
    CAST('2026-09-01' AS DATE) AS event_date,
    CAST('PULRHBOFF_COD' AS VARCHAR) AS evidence_type
), actual_counts AS (
  SELECT
    *,
    COUNT(*) AS occurrences
  FROM actual
  GROUP BY ALL
), expected_counts AS (
  SELECT
    *,
    COUNT(*) AS occurrences
  FROM expected
  GROUP BY ALL
), failures AS (
  (
    SELECT
      *
    FROM actual_counts
    EXCEPT
    SELECT
      *
    FROM expected_counts
  )
  UNION ALL
  (
    SELECT
      *
    FROM expected_counts
    EXCEPT
    SELECT
      *
    FROM actual_counts
  )
)
SELECT
  COUNT(*) AS failure_count
FROM failures
HAVING
  COUNT(*) > 0
