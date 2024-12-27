package database

import (
	"database/sql"
	"fmt"
	"migrationTii/internal/data_loader"
)

func InsertIntoPolicy(db *sql.Tx) error {
	query := `
    INSERT INTO POLICY (
    INSURER_PARTY_ID,
    POLICY_ID,
    SECTION_ID,
    SUB_SECTION_ID,
    ENDORSEMENT_ID,
    ENDORSEMENT_TYPE_ID,
    POLICY_STATUS_ID,
    CONTRACT_ID,
    POLICY_ISSUANCE_DATE,
    ENDORSEMENT_DATE,
    POLICY_ENTRANCE_DATE,
    POLICY_PHISICAL_DATE_DELIVERY,
    POLICY_PHISICAL_DATE_RECEPTION,
    POLICY_ENDORSEMENT_DATE_FROM,
    POLICY_ENDORSEMENT_DATE_TO,
    DATA_SOURCE,
    POLICY_ELECTRONIC_ACCEPTED,
    RENEWED_BY,
    RENEWED_NUMBER,
    POLICY_AGREEMENT_NUMBER,
    GRACE_PERIOD,
    DATE_MODIFIED,
    POLICY_AFFINITY_GROUP_ID,
    UNITED_PREMIUM,
    AGENT_PARTY_ID
)
SELECT
    1020 AS INSURER_PARTY_ID,                 -- Aseguradora fija
    CONCAT(t.RAMO, '-', t.NPOLORI) AS POLICY_ID, -- Número de póliza original (NPOLORI con formato correcto)
    101 AS SECTION_ID,                        -- Sección fija
    3000 AS SUB_SECTION_ID,                   -- Sub-sección fija
    0 AS ENDORSEMENT_ID,                      -- Endoso inicial
    3000 AS ENDORSEMENT_TYPE_ID,              -- Tipo de endoso: Nueva póliza
    1000 AS POLICY_STATUS_ID,                 -- Estado: Vigente
    c.CONTRACT_ID,                            -- ID del contrato asociado
    MIN(t.FINIVIG) AS POLICY_ISSUANCE_DATE,   -- Fecha de emisión original
    MIN(t.FINIVIG) AS ENDORSEMENT_DATE,       -- Fecha de endoso inicial
    MIN(t.FINIVIG) AS POLICY_ENTRANCE_DATE,   -- Fecha de entrada
    NULL AS POLICY_PHISICAL_DATE_DELIVERY,    -- Mantener NULL
    NULL AS POLICY_PHISICAL_DATE_RECEPTION,   -- Mantener NULL
    MIN(t.FINIVIG) AS POLICY_ENDORSEMENT_DATE_FROM, -- Fecha de inicio de vigencia
    MAX(t.FTERVIG) AS POLICY_ENDORSEMENT_DATE_TO, -- Fecha de término de vigencia
    NULL AS DATA_SOURCE,                      -- Fuente de datos
    0 AS POLICY_ELECTRONIC_ACCEPTED,          -- No aceptado electrónicamente
    NULL AS RENEWED_BY,                       -- No renovada por nadie
    NULL AS RENEWED_NUMBER,                   -- No tiene número de renovación
    NULL AS POLICY_AGREEMENT_NUMBER,          -- Sin número de acuerdo
    NULL AS GRACE_PERIOD,                     -- Sin período de gracia
    NOW() AS DATE_MODIFIED,                   -- Fecha de modificación actual
    NULL AS POLICY_AFFINITY_GROUP_ID,         -- Sin grupo de afinidad
    NULL AS UNITED_PREMIUM,                   -- Prima unificada nula
    23869 AS AGENT_PARTY_ID                   -- ID del agente fijo
FROM temp_polizas_data t
         JOIN CONTRACT_HEADER c ON c.CONTRACT_ID = t.NPOLIZA
WHERE t.CODESTADO = '03' -- Solo pólizas originales y vigentes
GROUP BY t.RAMO, t.NPOLORI;
         `
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error insertando en POLICY: %v", err)
	}
	fmt.Println("Datos insertados correctamente en POLICY.")
	data_loader.AddToSqlScript("\n-- Inserta datos en POLICY.\n\n")
	data_loader.AddToSqlScript(query)
	return nil
}
