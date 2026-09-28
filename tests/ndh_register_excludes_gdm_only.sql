/* GDM-only QOF members must not be labelled as clinical NDH. */

SELECT qof.person_id
FROM {{ ref('fct_person_qof_ndh_gdm_register') }} AS qof
INNER JOIN {{ ref('fct_person_ndh_register') }} AS clinical
    ON qof.person_id = clinical.person_id
WHERE qof.has_gdm_route = TRUE
    AND qof.has_ndh_route = FALSE
