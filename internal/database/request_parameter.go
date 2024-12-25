package database

import (
	"database/sql"
	"fmt"
)

// RequestParameter inserta valores en la tabla REQUEST_COVERAGE_VALUE.
func InsertRequestParameter(db *sql.Tx) error {
	query := `
	INSERT INTO REQUEST_PARAMETER (
    REQUEST_ID,
    REQUEST_PARAMETER_KEY,
    REQUEST_PARAMETER_DESC,
    REQUEST_PARAMETER_VALUE
)
SELECT
    r.REQUEST_ID,                                         -- REQUEST_ID generado
    param.REQUEST_PARAMETER_KEY,                         -- Clave del parámetro
    param.REQUEST_PARAMETER_DESC,                        -- Descripción del parámetro
    CASE
        WHEN param.REQUEST_PARAMETER_KEY = 'PAYMENT_DATE' THEN DATE_FORMAT(ch.CONTRACT_ISSUANCE_DATE, '%d/%m/%Y')
        ELSE param.REQUEST_PARAMETER_VALUE
    END AS REQUEST_PARAMETER_VALUE                       -- Valor del parámetro, ajustando PAYMENT_DATE
FROM (
         SELECT 'BELONGS_TO_BLACK_LIST' AS REQUEST_PARAMETER_KEY, 'BELONGS_TO_BLACK_LIST' AS REQUEST_PARAMETER_DESC, 'false' AS REQUEST_PARAMETER_VALUE UNION ALL
         SELECT 'BLACK_LIST_RESPONSE_CODE', 'BLACK_LIST_RESPONSE_CODE', '0' UNION ALL
         SELECT 'CUMULUS_VALUE', 'CUMULUS_VALUE', '0.0' UNION ALL
         SELECT 'DEPENDENTS', 'DEPENDENTS', '0' UNION ALL
         SELECT 'HAS_ANOTHER_PENDING_REQUEST', 'HAS_ANOTHER_PENDING_REQUEST', 'false' UNION ALL
         SELECT 'HAS_EXTRA_PREMIUM_POLICY', 'HAS_EXTRA_PREMIUM_POLICY', 'false' UNION ALL
         SELECT 'HAS_FINANCIAL_RISK', 'HAS_FINANCIAL_RISK', 'false' UNION ALL
         SELECT 'HAS_REJECTED_REQUEST', 'HAS_REJECTED_REQUEST', 'false' UNION ALL
         SELECT 'HAS_RISKY_ACTIVITY', 'HAS_RISKY_ACTIVITY', 'false' UNION ALL
         SELECT 'HISTORIC_RISK', 'HISTORIC_RISK', 'false' UNION ALL
         SELECT 'IMC_VALUE', 'IMC_VALUE', '26.794938016528928' UNION ALL
         SELECT 'INSURED_AND_HOLDER_RELATIONSHIP_IS_OTHER', 'INSURED_AND_HOLDER_RELATIONSHIP_IS_OTHER', 'MISMO' UNION ALL
         SELECT 'INTEGRATION_TII', 'INTEGRATION_TII', 'false' UNION ALL
         SELECT 'IS_FOREIGN_PERSON', 'IS_FOREIGN_PERSON', 'false' UNION ALL
         SELECT 'PAYMENT_DATE', 'PAYMENT_DATE', NULL UNION ALL -- Se ajustará con CONTRACT_ISSUANCE_DATE
         SELECT 'REQUEST_TOKEN', 'REQUEST_TOKEN', '8d57e3821dd307f0da90f1c2bf78ff9a9bd9139e53a49748089afab877943067' UNION ALL
         SELECT 'REQUEST_TOKEN_DATE', 'REQUEST_TOKEN_DATE', '1734444012629' UNION ALL
         SELECT 'REQUIRE_MEDICAL_PROTOCOL', 'REQUIRE_MEDICAL_PROTOCOL', 'false' UNION ALL
         SELECT 'RESOURCE_DATA', 'RESOURCE_DATA', 'SUELDO' UNION ALL
         SELECT 'VALID_IMC', 'VALID_IMC', 'true' UNION ALL
         SELECT 'VALID_QUESTIONNAIRE', 'VALID_QUESTIONNAIRE', 'true' UNION ALL
         SELECT 'VALID_REINSURANCE_AMOUNT_VALIDATION', 'VALID_REINSURANCE_AMOUNT_VALIDATION', 'true'
     ) param
         CROSS JOIN REQUEST r
         JOIN CONTRACT_HEADER ch ON ch.CONTRACT_ID = r.CONTRACT_ID
WHERE r.REQUEST_ID = (
    SELECT MAX(REQUEST_ID) FROM REQUEST -- Selecciona el REQUEST_ID más reciente
)
  AND NOT EXISTS (
    SELECT 1
    FROM REQUEST_PARAMETER rp
    WHERE rp.REQUEST_ID = r.REQUEST_ID
      AND rp.REQUEST_PARAMETER_KEY = param.REQUEST_PARAMETER_KEY
);
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error inserting into REQUEST_PARAMETER: %v", err)
	}

	fmt.Println("Datos insertados en REQUEST_PARAMETER correctamente.")
	return nil
}
