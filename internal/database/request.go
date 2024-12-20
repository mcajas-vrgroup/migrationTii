package database

import (
	"database/sql"
	"fmt"
)

// Insert Request inserta datos en la tabla REQUEST.
func InsertRequest(db *sql.DB) error {
	query := `
	INSERT INTO REQUEST (
		CONTRACT_ID,
		PENDING_INSPECTION,
		INSURER_ID,
		POLICY_ID,
		SECTION_ID,
		SUB_SECTION_ID,
		ENDORSEMENT_ID,
		CERTIFICATION_NUMBER,
		USER_ID,
		REQUEST_STATUS_ID,
		OBSERVATIONS,
		COMMENTS,
		CREATED_DATE,
		DUE_DATE,
		ACCOUNT_ID,
		AGENT_PARTY_ID
	)
	SELECT
		c.CONTRACT_ID,                         -- Asociado al CONTRACT_HEADER
		1 AS PENDING_INSPECTION,               -- Default a 1
		NULL AS INSURER_ID,                    -- ID de la aseguradora fija (NULL)
		NULL AS POLICY_ID,                     -- Número de póliza (NULL)
		NULL AS SECTION_ID,                    -- Sección por defecto (NULL)
		NULL AS SUB_SECTION_ID,                -- Sub-sección fija (NULL)
		NULL AS ENDORSEMENT_ID,                -- Dejar en NULL (falta información)
		'000-1111111111' AS CERTIFICATION_NUMBER, -- Número de certificación fijo
		NULL AS USER_ID,                       -- Dejar en NULL
		13000 AS REQUEST_STATUS_ID,            -- Estado de la solicitud
		'OBSERVACION DESDE EL PORTAL' AS OBSERVATIONS,
		'COMENTARIO DESDE PORTAL' AS COMMENTS,
		NULL AS CREATED_DATE,                  -- Dejar en NULL
		NULL AS DUE_DATE,                      -- Dejar en NULL
		NULL AS ACCOUNT_ID,                    -- Dejar en NULL
		23869 AS AGENT_PARTY_ID                -- ID del agente fijo
	FROM temp_polizas_data t
	JOIN CONTRACT_HEADER c ON t.NPOLIZA = c.CONTRACT_ID
	WHERE t.CODESTADO = '03'; -- Solo contratos "EN VIGOR"
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error insertando en REQUEST: %v", err)
	}

	fmt.Println("Datos insertados correctamente en REQUEST.")
	return nil
}
