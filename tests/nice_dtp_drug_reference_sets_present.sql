-- QOF v51 DTP1/2/3 permits orders for all three DTP-containing formulations.
WITH required AS (
    SELECT column1 AS ref_set_id
    FROM VALUES ('72391000001101'), ('72381000001103'), ('72371000001100')
)

SELECT required.ref_set_id
FROM required
LEFT JOIN {{ ref('stg_nhsd_snomed_sct_refset_simple') }} AS member
    ON required.ref_set_id = member.ref_set_id AND member.active
GROUP BY required.ref_set_id
HAVING COUNT(member.id) = 0
