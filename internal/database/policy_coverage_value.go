package database

import (
	"database/sql"
	"fmt"
)

// Insert PolicyCoverageValue inserta datos en la tabla POLICY_COVERAGE_VALUE
func InsertPolicyCoverageValue(db *sql.Tx) error {
	query := `
	INSERT INTO POLICY_COVERAGE_VALUE (
    INSURER_PARTY_ID,
    POLICY_ID,
    SECTION_ID,
    SUB_SECTION_ID,
    ENDORSEMENT_ID,
    IC_INSURER_PARTY_ID,
    IC_SECTION_ID,
    IC_SUB_SECTION_ID,
    IC_COVERAGE_ID,
    INSURED_AMOUNT,
    DEDUCTIBLE,
    INITIAL_COVERAGE_GRACE_PERIOD,
    FUT_PAYROLL_COVER_GRACE_PERIOD,
    INIT_COVER_GROUP_GRACE_PERIOD,
    FUT_PAYROLL_COVER_G_GRACE_PERI,
    MAXIMUM_PERIOD_OF_COVERAGE,
    REINSURED_AMOUNT,
    SALARY_MULTIPLIER,
    PREMIUM,
    BASE_PREMIUM,
    TAX_VALUE
)
SELECT
    1020 AS INSURER_PARTY_ID,               -- Aseguradora fija
    CONCAT(t.RAMO, '-', t.NPOLORI) AS POLICY_ID, -- Número de póliza original (RAMO-NPOLIZA)
    101 AS SECTION_ID,                      -- Sección fija
    3000 AS SUB_SECTION_ID,                 -- Sub-sección fija
    0 AS ENDORSEMENT_ID,                    -- Endoso inicial
    1020 AS IC_INSURER_PARTY_ID,            -- Mismo asegurador
    101 AS IC_SECTION_ID,                   -- Sección fija
    3000 AS IC_SUB_SECTION_ID,              -- Sub-sección fija
    1 AS IC_COVERAGE_ID,                    -- ID de cobertura fija
    25000 AS INSURED_AMOUNT,                -- Monto asegurado fijo
    NULL AS DEDUCTIBLE,                     -- Deductible nulo
    NULL AS INITIAL_COVERAGE_GRACE_PERIOD,  -- Periodo de gracia inicial nulo
    NULL AS FUT_PAYROLL_COVER_GRACE_PERIOD, -- Periodo de gracia futura nulo
    NULL AS INIT_COVER_GROUP_GRACE_PERIOD,  -- Grupo inicial nulo
    NULL AS FUT_PAYROLL_COVER_G_GRACE_PERI, -- Grupo futuro nulo
    NULL AS MAXIMUM_PERIOD_OF_COVERAGE,     -- Máximo periodo nulo
    NULL AS REINSURED_AMOUNT,               -- Monto reasegurado fijo
    NULL AS SALARY_MULTIPLIER,              -- Multiplicador de salario nulo
    0.46409736 AS PREMIUM,                  -- Prima fija
    0.38999736 AS BASE_PREMIUM,             -- Prima base fija
    0.0741 AS TAX_VALUE                     -- Valor de impuesto fijo
FROM temp_polizas_data t
WHERE t.NPOLIZA LIKE '%00' AND t.CODESTADO = '03';`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error insertando en POLICY_COVERAGE_VALUE: %v", err)
	}

	fmt.Println("Datos insertados correctamente en POLICY_COVERAGE_VALUE.")
	return nil
}
