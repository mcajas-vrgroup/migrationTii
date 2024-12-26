package database

import (
	"database/sql"
	"fmt"
	"migrationTii/internal/data_loader"
)

// RequestCoverageValue inserta valores en la tabla REQUEST_COVERAGE_VALUE.
func InsertRequestCoverageValue(db *sql.Tx) error {
	query := `
	INSERT INTO REQUEST_COVERAGE_VALUE (
    REQUEST_ID,
    IC_INSURER_PARTY_ID,
    IC_SECTION_ID,
    IC_SUB_SECTION_ID,
    IC_COVERAGE_ID,
    INSURED_AMOUNT,
    DEDUCTIBLE,
    INITIAL_COVERAGE_GRACE_PERIOD,
    FUT_PAYR_COVERAGE_GRACE_PERI,
    INIT_COVER_GROUP_GRACE_PERIOD,
    FUT_PAYR_COVER_GR_GRACE_PERIOD,
    MAXIMUM_PERIOD_OF_COVERAGE,
    REINSURED_AMOUNT,
    PREMIUM,
    BASE_PREMIUM,
    TAX_VALUE
)
SELECT
    r.REQUEST_ID,               -- ID de la solicitud generada
    1020 AS IC_INSURER_PARTY_ID, -- ID de la aseguradora (fijo)
    101 AS IC_SECTION_ID,        -- ID de la sección (fijo)
    3000 AS IC_SUB_SECTION_ID,   -- ID de la subsección (fijo)
    1 AS IC_COVERAGE_ID,         -- ID de cobertura (fijo)
    25000 AS INSURED_AMOUNT,     -- Monto asegurado (fijo)
    NULL AS DEDUCTIBLE,          -- Deducible (NULL)
    NULL AS INITIAL_COVERAGE_GRACE_PERIOD,
    NULL AS FUT_PAYR_COVERAGE_GRACE_PERI,
    NULL AS INIT_COVER_GROUP_GRACE_PERIOD,
    NULL AS FUT_PAYR_COVER_GR_GRACE_PERIOD,
    NULL AS MAXIMUM_PERIOD_OF_COVERAGE,
    NULL AS REINSURED_AMOUNT,
    0.46409736 AS PREMIUM,       -- Prima total
    0.38999736 AS BASE_PREMIUM,  -- Prima base
    0.0741 AS TAX_VALUE          -- Valor de impuesto
FROM REQUEST r
JOIN CONTRACT_HEADER c ON r.CONTRACT_ID = c.CONTRACT_ID
JOIN temp_polizas_data t ON t.NPOLIZA = c.CONTRACT_ID
WHERE t.CODESTADO = '03'; -- Solo pólizas vigentes
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error inserting into REQUEST_COVERAGE_VALUE: %v", err)
	}

	fmt.Println("Datos insertados en REQUEST_COVERAGE_VALUE correctamente.")
	data_loader.AddToSqlScript(query)
	return nil
}
