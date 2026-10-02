-- Lab extreme outliers: how many results and people the "keep extreme outliers"
-- change affects.
--
-- The haemoglobin, platelets, ALT, GGT, bilirubin and eosinophil count _latest
-- models no longer exclude results flagged is_extreme_outlier (above
-- BIOLOGICAL_UPPER in the observation_value_bounds seed). This counts, for each
-- measurement, the outlier results in the _all model and the people whose
-- latest valid result is an outlier: the people whose _latest value changes.
-- Every row is an aggregate count.
--
-- Usage: compile with dbt, then run the compiled query in Snowflake. It reads
-- the _all models and int_efi2_rules, so it gives the same answer before and
-- after the _latest models are rebuilt.
--
-- COLUMNS
--   results                      valid results (non-NULL, non-negative)
--   outlier_results              of those, flagged is_extreme_outlier
--   people_with_result           people with any valid result
--   people_with_any_outlier      people with at least one outlier result
--   people_latest_is_outlier     people whose latest valid result is an
--                                outlier. Their _latest value is now that
--                                outlier instead of an earlier result or none.
--   anaemia_coded_latest_is_outlier
--                                haemoglobin only: of those, people with an
--                                eFI2 anaemia code. Their eFI2 anaemia deficit
--                                can now switch off, because a value above
--                                300 g/L is never below the anaemia threshold.
--   min_outlier_value,           lowest and median outlier value, to judge
--   median_outlier_value         whether outliers look like real extreme
--                                results or unit errors
--
-- Covers every person in the _all models, not only currently registered
-- people, matching the _latest models.

WITH lab_results AS (
    SELECT 'haemoglobin' AS measurement, person_id, id, clinical_effective_date, inferred_value, is_extreme_outlier
    FROM {{ ref('int_haemoglobin_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'platelets', person_id, id, clinical_effective_date, inferred_value, is_extreme_outlier
    FROM {{ ref('int_platelets_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'alt', person_id, id, clinical_effective_date, inferred_value, is_extreme_outlier
    FROM {{ ref('int_alt_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'ggt', person_id, id, clinical_effective_date, inferred_value, is_extreme_outlier
    FROM {{ ref('int_ggt_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'bilirubin', person_id, id, clinical_effective_date, inferred_value, is_extreme_outlier
    FROM {{ ref('int_bilirubin_all') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
    UNION ALL
    SELECT 'eosinophil_count', person_id, id, clinical_effective_date, inferred_value, is_extreme_outlier
    FROM {{ ref('int_eosinophil_count') }}
    WHERE inferred_value IS NOT NULL AND NOT is_negative
),

-- Latest valid result per person and measurement, chosen as the _latest models
-- now choose it.
latest AS (
    SELECT measurement, person_id, is_extreme_outlier
    FROM lab_results
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY measurement, person_id
        ORDER BY clinical_effective_date DESC, id DESC
    ) = 1
),

-- People with an eFI2 anaemia code. Their anaemia rows exist whatever their
-- haemoglobin, so this list is the same before and after the change.
anaemia_coded AS (
    SELECT DISTINCT person_id
    FROM {{ ref('int_efi2_rules') }}
    WHERE deficit = 'Anaemia & haematinic deficiency'
),

result_counts AS (
    SELECT
        measurement,
        COUNT(*)                                            AS results,
        COUNT_IF(is_extreme_outlier)                        AS outlier_results,
        COUNT(DISTINCT person_id)                           AS people_with_result,
        COUNT(DISTINCT IFF(is_extreme_outlier, person_id, NULL)) AS people_with_any_outlier,
        MIN(IFF(is_extreme_outlier, inferred_value, NULL))  AS min_outlier_value,
        MEDIAN(IFF(is_extreme_outlier, inferred_value, NULL)) AS median_outlier_value
    FROM lab_results
    GROUP BY measurement
),

latest_counts AS (
    SELECT
        l.measurement,
        COUNT_IF(l.is_extreme_outlier)                      AS people_latest_is_outlier,
        COUNT_IF(l.is_extreme_outlier AND a.person_id IS NOT NULL) AS anaemia_coded_latest_is_outlier
    FROM latest AS l
    LEFT JOIN anaemia_coded AS a
        ON l.person_id = a.person_id
        AND l.measurement = 'haemoglobin'
    GROUP BY l.measurement
)

SELECT
    r.measurement,
    r.results,
    r.outlier_results,
    r.people_with_result,
    r.people_with_any_outlier,
    lc.people_latest_is_outlier,
    IFF(r.measurement = 'haemoglobin', lc.anaemia_coded_latest_is_outlier, NULL)
                                                            AS anaemia_coded_latest_is_outlier,
    ROUND(r.min_outlier_value, 1)                           AS min_outlier_value,
    ROUND(r.median_outlier_value, 1)                        AS median_outlier_value
FROM result_counts AS r
INNER JOIN latest_counts AS lc
    ON r.measurement = lc.measurement
ORDER BY r.measurement
