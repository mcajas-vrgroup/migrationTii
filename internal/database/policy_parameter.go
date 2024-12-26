package database

import (
	"database/sql"
	"fmt"
	"log"
	"migrationTii/internal/data_loader"
)

// Insert PolicyParameter inserta los parámetros asociados a las pólizas en la tabla POLICY_PARAMETER.
func InsertPolicyParameter(db *sql.Tx) error {
	query := `
		INSERT INTO POLICY_PARAMETER (
    INSURER_PARTY_ID,
    POLICY_ID,
    SECTION_ID,
    SUB_SECTION_ID,
    ENDORSEMENT_ID,
    POLICY_PARAMETER_KEY,
    POLICY_PARAMETER_DESC,
    POLICY_PARAMETER_VALUE
)
SELECT
    1020 AS INSURER_PARTY_ID,                   -- ID del asegurador
    CONCAT(t.RAMO, '-', t.NPOLORI) AS POLICY_ID, -- ID de la póliza original (RAMO-NPOLORI)
    101 AS SECTION_ID,                         -- Sección fija
    3000 AS SUB_SECTION_ID,                    -- Sub-sección fija
    0 AS ENDORSEMENT_ID,                       -- Endoso inicial
    k.PARAMETER_KEY,                           -- Clave de parámetro
    k.PARAMETER_DESC,                          -- Descripción del parámetro
    CASE
        WHEN k.PARAMETER_KEY = 'COVERAGE_KEY' THEN '3173' -- Valor ajustado para COVERAGE_KEY
        WHEN k.PARAMETER_KEY = 'CONTRACT_TI' THEN t.REQUEST -- Valor del campo REQUEST del CSV para CONTRACT_TI
        ELSE k.PARAMETER_VALUE -- Valores originales para las demás claves
        END AS POLICY_PARAMETER_VALUE
FROM (
         -- Subconsulta para los valores del ejemplo
         SELECT 'BELONGS_TO_BLACK_LIST' AS PARAMETER_KEY, 'BELONGS_TO_BLACK_LIST' AS PARAMETER_DESC, 'false' AS PARAMETER_VALUE
         UNION ALL SELECT 'BLACK_LIST_RESPONSE_CODE', 'BLACK_LIST_RESPONSE_CODE', '0'
         UNION ALL SELECT 'CONTRACT_TI', 'CONTRACT_TI', '' -- Valor será tomado del REQUEST del CSV
         UNION ALL SELECT 'COVERAGE_KEY', 'COVERAGE_KEY', '8888' -- Este será ajustado
         UNION ALL SELECT 'CUMULUS_VALUE', 'CUMULUS_VALUE', '0.0'
         UNION ALL SELECT 'DEPENDENTS', 'DEPENDENTS', '0'
         UNION ALL SELECT 'HAS_ANOTHER_PENDING_REQUEST', 'HAS_ANOTHER_PENDING_REQUEST', 'false'
         UNION ALL SELECT 'HAS_EXTRA_PREMIUM_POLICY', 'HAS_EXTRA_PREMIUM_POLICY', 'false'
         UNION ALL SELECT 'HAS_FINANCIAL_RISK', 'HAS_FINANCIAL_RISK', 'false'
         UNION ALL SELECT 'HAS_REJECTED_REQUEST', 'HAS_REJECTED_REQUEST', 'false'
         UNION ALL SELECT 'HAS_RISKY_ACTIVITY', 'HAS_RISKY_ACTIVITY', 'false'
         UNION ALL SELECT 'HISTORIC_RISK', 'HISTORIC_RISK', 'false'
         UNION ALL SELECT 'IMC_VALUE', 'IMC_VALUE', '26.794938016528928'
         UNION ALL SELECT 'INSURED_AND_HOLDER_RELATIONSHIP_IS_OTHER', 'INSURED_AND_HOLDER_RELATIONSHIP_IS_OTHER', 'MISMO'
         UNION ALL SELECT 'INTEGRATED_WITH_SAM', 'INTEGRATED_WITH_SAM', 'true'
         UNION ALL SELECT 'INTEGRATION_TII', 'INTEGRATION_TII', 'true'
         UNION ALL SELECT 'IS_FOREIGN_PERSON', 'IS_FOREIGN_PERSON', 'false'
         UNION ALL SELECT 'REQUEST_TOKEN', 'REQUEST_TOKEN', '8d57e3821dd307f0da90f1c2bf78ff9a9bd9139e53a49748089afab877943067'
         UNION ALL SELECT 'REQUEST_TOKEN_DATE', 'REQUEST_TOKEN_DATE', '1734444012629'
         UNION ALL SELECT 'REQUIRE_MEDICAL_PROTOCOL', 'REQUIRE_MEDICAL_PROTOCOL', 'false'
         UNION ALL SELECT 'RESOURCE_DATA', 'RESOURCE_DATA', 'SUELDO'
         UNION ALL SELECT 'VALID_IMC', 'VALID_IMC', 'true'
         UNION ALL SELECT 'VALID_QUESTIONNAIRE', 'VALID_QUESTIONNAIRE', 'true'
         UNION ALL SELECT 'VALID_REINSURANCE_AMOUNT_VALIDATION', 'VALID_REINSURANCE_AMOUNT_VALIDATION', 'true'
     ) AS k
         JOIN temp_polizas_data t ON CONCAT(t.RAMO, '-', t.NPOLORI) = CONCAT(t.RAMO, '-', t.NPOLIZA) -- Asociar NPOLORI con el POLICY_ID
WHERE t.CODESTADO = '03'; -- Asociar el REQUEST del CSV con el POLICY_ID
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error insertando en POLICY_PARAMETER: %v", err)
	}

	log.Println("Datos insertados en POLICY_PARAMETER correctamente.")
	data_loader.AddToSqlScript(query)
	return nil
}
