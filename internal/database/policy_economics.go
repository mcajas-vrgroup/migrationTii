package database

import (
	"database/sql"
	"fmt"
)

// Insert PolicyEconomics inserta datos en la tabla POLICY_ECONOMICS.
func InsertPolicyEconomics(db *sql.Tx) error {
	query := `
    INSERT INTO POLICY_ECONOMICS (
    ECONOMIC_ITEM_ID,
    ECONOMIC_VALUE,
    INSURER_PARTY_ID,
    POLICY_ID,
    SECTION_ID,
    SUB_SECTION_ID,
    ENDORSEMENT_ID,
    ECONOMIC_VALUE_DATE,
    TAX_ID
)
SELECT
    ei.ECONOMIC_ITEM_ID,
    CASE
        WHEN ei.ECONOMIC_ITEM_ID = 9000 THEN 25000 -- Ajustar el valor para el ID 9000
        ELSE ei.ECONOMIC_VALUE
        END AS ECONOMIC_VALUE,
    1020 AS INSURER_PARTY_ID,      -- ID fijo del asegurador
    CONCAT(t.RAMO, '-', t.NPOLORI) AS POLICY_ID, -- Generar POLICY_ID como RAMO-NPOLIZA original
    101 AS SECTION_ID,             -- Sección fija
    3000 AS SUB_SECTION_ID,        -- Sub-sección fija
    0 AS ENDORSEMENT_ID,           -- Endoso inicial
    NULL AS ECONOMIC_VALUE_DATE,   -- Sin fecha específica
    NULL AS TAX_ID                 -- Sin impuesto específico
FROM (
         -- Valores base para los ECONOMIC_ITEM_ID
         SELECT 1000 AS ECONOMIC_ITEM_ID, 0 AS ECONOMIC_VALUE
         UNION ALL SELECT 2000, 0.4641
         UNION ALL SELECT 3000, 0
         UNION ALL SELECT 4000, 0
         UNION ALL SELECT 5000, 0
         UNION ALL SELECT 6000, 0.4641
         UNION ALL SELECT 7000, 0
         UNION ALL SELECT 8000, 0.0741
         UNION ALL SELECT 9000, 30000 -- Este será ajustado
         UNION ALL SELECT 10000, 0
         UNION ALL SELECT 11000, 0
         UNION ALL SELECT 12000, 0
         UNION ALL SELECT 13000, 0
         UNION ALL SELECT 14000, 0
         UNION ALL SELECT 15000, 0
         UNION ALL SELECT 16000, 0
         UNION ALL SELECT 17000, 0
         UNION ALL SELECT 18000, 0
         UNION ALL SELECT 19000, 0
         UNION ALL SELECT 20000, 0
         UNION ALL SELECT 21000, 0.39
         UNION ALL SELECT 24000, 0
         UNION ALL SELECT 25000, 0
         UNION ALL SELECT 26000, 0
         UNION ALL SELECT 28000, 0
         UNION ALL SELECT 29000, 5.1056
     ) AS ei
         JOIN temp_polizas_data t ON CONCAT(t.RAMO, '-', t.NPOLORI) = CONCAT(t.RAMO, '-', t.NPOLIZA)
WHERE t.CODESTADO = '03';
    `

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error insertando en POLICY_ECONOMICS: %v", err)
	}

	fmt.Println("Datos insertados correctamente en POLICY_ECONOMICS.")
	return nil
}
