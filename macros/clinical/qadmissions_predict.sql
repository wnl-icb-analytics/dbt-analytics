{#
QAdmissions model registry call

qadmissions_predict(alias) calls the QAdmissions model registered in the
Snowflake Model Registry for one row of qadmissions_input_features, or any
relation with the same input columns. The registered model sits in the same
database and schema as qadmissions_input_features (STAT_MODELS.QADMISSIONS,
DEV__STAT_MODELS.QADMISSIONS in dev), so the call follows the dbt target.
It is registered from the qadmission_review repository.

Inputs are passed by name, so a change in argument order cannot pair a value
with the wrong input. The names must match the registered signature, which the
qadmission_review scoring code defines (python_pipeline.validation.ModelInputs,
plus sex).

The call returns an OBJECT with the keys ok, score_percent, errors, engine and
sex. score_percent is NULL and errors is set when the model rejects a row,
for example because an input is NULL or out of range.
#}

{% macro qadmissions_model_version() %}V1{% endmacro %}

{% macro qadmissions_model_inputs() %}
    {{ return([
        'sex', 'age', 'alcohol_cat6',
        'b_af', 'b_ccf', 'b_anticoagulant', 'b_antidepressant', 'b_antipsychotic',
        'b_anycancer', 'b_asthmacopd', 'b_corticosteroids', 'b_cvd', 'b_epilepsy',
        'b_falls', 'b_liverpancreas', 'b_malabsorption', 'b_manicschiz', 'b_nsaid',
        'b_renal', 'b_type1', 'b_type2', 'b_vte',
        'bmi', 'c_hb', 'ethrisk', 'hes_admitprior_cat', 'high_lft', 'high_platelet',
        'sha1', 'smoke_cat', 'surv', 'town'
    ]) }}
{% endmacro %}

{% macro qadmissions_predict(alias) %}
{%- set features = ref('qadmissions_input_features') -%}
MODEL({{ features.database }}.{{ features.schema }}.QADMISSIONS, {{ qadmissions_model_version() }})!PREDICT(
{%- for input in qadmissions_model_inputs() %}
        {{ input | upper }} => {{ alias }}.{{ input }}{{ ',' if not loop.last }}
{%- endfor %}
    )
{%- endmacro %}
