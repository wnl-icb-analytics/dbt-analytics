{% macro standardise_count_observation(base_cte, measurement, value_column='result_value') %}

{#
    Standardises count-type observation values and units.

    Produces a standardised output with inferred values and units, an audit reason, the
    is_negative / is_extreme_outlier flags, and a HIGH/MEDIUM/LOW confidence rating. A given
    row always carries a value and corresponding unit (nothing is dropped except excluded
    units, whose value/unit are NULL).

    Uses a 3-pass approach, all gated on the ABSOLUTE [lower, upper] band from the
    observation_value_bounds seed:
      Pass 1 - Accept values already inside [ABSOLUTE_LOWER, ABSOLUTE_UPPER], regardless
               of recorded unit. Kept exactly as recorded.

      Pass 2 - For values ABOVE ABSOLUTE_UPPER, apply a known conversion factor from the
               seed (e.g. {cells}/uL -> 10*9/L via x0.001) when the converted value lands
               back inside [ABSOLUTE_LOWER, ABSOLUTE_UPPER].

      Pass 3 - For values ABOVE ABSOLUTE_UPPER, check if the value is unambiguously a raw
               base-unit recording (e.g. cells/L instead of 10*9/L) by dividing by
               10^CANONICAL_EXPONENT and checking the result lands inside
               [BIOLOGICAL_LOWER, ABSOLUTE_UPPER]. CANONICAL_EXPONENT IS NULL disables Pass 3.

      Conversions (Pass 2/3) are attempted ONLY when the value is implausibly HIGH
      (numeric_value > ABSOLUTE_UPPER), never when implausibly low. Values matching no
      pass (out-of-range and unconvertible, including negatives) pass through unchanged.

    Unit classification after LEFT JOIN to seed:
      known    - CONVERT_FROM_UNIT IS NOT NULL AND MULTIPLY_BY IS NOT NULL
      excluded - CONVERT_FROM_UNIT IS NOT NULL AND MULTIPLY_BY IS NULL
      unknown  - CONVERT_FROM_UNIT IS NULL (no match in seed)

    Flags and confidence:
      is_negative        - inferred_value < 0
      is_extreme_outlier - inferred_value > BIOLOGICAL_UPPER
      confidence:
        HIGH   - Value kept exactly as recorded in the canonical unit (nothing changed).
        MEDIUM - Only the unit was relabelled (value kept) and not an extreme outlier.
        LOW    - Value was converted (Pass 2/3), is negative, is an extreme outlier
                 (> BIOLOGICAL_UPPER), or the unit is excluded.

    Parameters:
      base_cte (str):
          Name of the CTE containing raw observations. Must include result_unit_code,
          result_unit_display, and the value_column.

      measurement (str):
          Identifier matching DEFINITION_NAME in the observation_standard_units,
          observation_value_bounds, and observation_unit_rules seeds
          (e.g. 'eosinophil_count').

      value_column (str):
          Column name containing the numeric result. Default: 'result_value'.

    Output columns (appended to all columns from base_cte):
      original_result_value       - Raw value preserved from source
      original_result_unit_code   - Raw unit code preserved from source
      original_result_unit_display - Raw unit display preserved from source
      inferred_value              - Standardised numeric value (NULL only for excluded units)
      inferred_unit               - Standardised unit (NULL only for excluded units)
      value_was_converted         - TRUE if value was mathematically changed
      conversion_reason           - Human-readable explanation of action taken
      is_negative                 - TRUE if inferred_value < 0
      is_extreme_outlier          - TRUE if inferred_value > BIOLOGICAL_UPPER
      confidence                  - HIGH / MEDIUM / LOW
#}

-- Filter seed to rules for this measurement only
unit_rules AS (
    SELECT *
    FROM {{ ref('observation_unit_rules') }}
    WHERE DEFINITION_NAME = '{{ measurement }}'
),

-- Look up the canonical target unit for this measurement (e.g. '10*9/L')
canonical_unit AS (
    SELECT UNIT AS CONVERT_TO_UNIT
    FROM {{ ref('observation_standard_units') }}
    WHERE DEFINITION_NAME = '{{ measurement }}'
      AND PRIMARY_UNIT = TRUE
),

-- Look up the plausible value range and Pass 3 configuration for this measurement
value_bounds AS (
    SELECT ABSOLUTE_LOWER, ABSOLUTE_UPPER, CANONICAL_EXPONENT, BIOLOGICAL_LOWER, BIOLOGICAL_UPPER
    FROM {{ ref('observation_value_bounds') }}
    WHERE DEFINITION_NAME = '{{ measurement }}'
),

-- Join observations to seed and classify each record's unit status
classified AS (
    SELECT
        base.*,
        CAST(base.{{ value_column }} AS FLOAT) AS numeric_value,
        base.result_unit_code AS original_result_unit_code,
        base.result_unit_display AS original_result_unit_display,
        base.{{ value_column }} AS original_result_value,
        cu.CONVERT_TO_UNIT AS canonical_unit_code,
        vb.ABSOLUTE_LOWER AS lower_limit,
        vb.ABSOLUTE_UPPER AS upper_limit,
        vb.CANONICAL_EXPONENT AS canonical_exponent,
        vb.BIOLOGICAL_LOWER AS biological_lower, -- Pass 3 floor
        vb.BIOLOGICAL_UPPER AS biological_upper, -- extreme-outlier threshold
        ur.MULTIPLY_BY AS seed_multiply_by,
        ur.PRE_OFFSET AS seed_pre_offset,
        ur.POST_OFFSET AS seed_post_offset,
        -- Unit classification: known (matched with conversion), excluded (matched without), unknown (no match)
        CASE
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NULL THEN 'excluded'
            WHEN ur.CONVERT_FROM_UNIT IS NOT NULL AND ur.MULTIPLY_BY IS NOT NULL THEN 'known'
            ELSE 'unknown'
        END AS unit_status,
        -- Canonical = unit converts to itself with factor 1 (e.g. 10*9/L -> 10*9/L)
        (ur.CONVERT_FROM_UNIT = ur.CONVERT_TO_UNIT AND ur.MULTIPLY_BY = 1) AS is_canonical
    FROM {{ base_cte }} base
    LEFT JOIN unit_rules ur
        ON base.result_unit_code = ur.CONVERT_FROM_UNIT
    CROSS JOIN canonical_unit cu
    CROSS JOIN value_bounds vb
),

-- Determine which pass (if any) applies - evaluated ONCE per row
pass_assigned AS (
    SELECT
        *,
        (numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by
            + COALESCE(seed_post_offset, 0) AS converted_value,
        CASE
            WHEN unit_status = 'excluded' THEN 'excluded'
            -- Pass 1: value already plausible, keep as recorded
            WHEN numeric_value BETWEEN lower_limit AND upper_limit
                THEN 'pass_1'
            -- Pass 2: implausibly high, rescued by a known unit factor
            WHEN numeric_value > upper_limit
             AND unit_status = 'known'
             AND seed_multiply_by != 1
             AND ((numeric_value + COALESCE(seed_pre_offset, 0)) * seed_multiply_by
                  + COALESCE(seed_post_offset, 0))
                  BETWEEN lower_limit AND upper_limit
                THEN 'pass_2'
            -- Pass 3: implausibly high, unambiguously a raw base-unit recording
            WHEN numeric_value > upper_limit
             AND canonical_exponent IS NOT NULL
             AND (numeric_value / POWER(10, canonical_exponent)) BETWEEN biological_lower AND upper_limit
                THEN 'pass_3'
            ELSE 'no_match'
        END AS matched_pass
    FROM classified
),

-- Derive the standardised value/unit/audit columns from matched_pass
standardised AS (
    SELECT
        *,

        -- inferred_value: the standardised numeric result
        CASE matched_pass
            WHEN 'pass_1' THEN numeric_value
            WHEN 'pass_2' THEN converted_value
            WHEN 'pass_3' THEN numeric_value / POWER(10, canonical_exponent)
            -- pass through: keep raw value when no pass matched
            WHEN 'no_match' THEN numeric_value
            ELSE NULL
        END AS inferred_value,

        -- inferred_unit: the standardised unit code (everything except excluded targets the canonical unit)
        CASE
            WHEN matched_pass = 'excluded' THEN NULL
            ELSE canonical_unit_code
        END AS inferred_unit,

        -- value_was_converted: TRUE if the numeric value was mathematically changed
        CASE matched_pass
            WHEN 'pass_2' THEN TRUE
            WHEN 'pass_3' THEN TRUE
            ELSE FALSE
        END AS value_was_converted,

        -- conversion_reason: human-readable explanation of action taken
        CASE matched_pass
            WHEN 'excluded' THEN 'Excluded unit on this measurement type'
            WHEN 'pass_1' THEN
                CASE
                    WHEN is_canonical THEN 'No value or unit conversion necessary'
                    WHEN unit_status = 'known' AND seed_multiply_by = 1 THEN 'No value conversion, unit was non-standard but numerically equivalent'
                    ELSE 'No value conversion, unit was assumed to be incorrect'
                END
            WHEN 'pass_2' THEN 'Value converted using known unit factor (x' || seed_multiply_by::VARCHAR || ')'
            WHEN 'pass_3' THEN 'Value divided by 10^' || canonical_exponent::VARCHAR || ' (inferred raw base-unit recording)'
            ELSE 'Value out of plausible range, kept unchanged'
        END AS conversion_reason

    FROM pass_assigned
),

-- Derive model-level flags and the confidence rating from the standardised value
flagged AS (
    SELECT
        *,
        inferred_value < 0 AS is_negative,
        inferred_value > biological_upper AS is_extreme_outlier,
        -- confidence: HIGH if nothing changed; MEDIUM if only the unit changed and the value
        -- is not an extreme outlier; LOW if converted, negative, extreme, or an excluded unit
        CASE
            WHEN inferred_value IS NULL THEN 'LOW'
            WHEN inferred_value < 0 OR inferred_value > biological_upper THEN 'LOW'
            WHEN value_was_converted THEN 'LOW'
            WHEN inferred_unit IS DISTINCT FROM original_result_unit_code THEN 'MEDIUM'
            ELSE 'HIGH'
        END AS confidence
    FROM standardised
)

{% endmacro %}
