package database

import (
	"database/sql"
	"fmt"
)

// RequestEconomics inserta datos en la tabla REQUEST_ECONOMICS.
func InsertRequestEconomics(db *sql.Tx) error {
	query := `
	INSERT INTO REQUEST_ECONOMICS (
    ECONOMIC_ITEM_ID,
    REQUEST_ID,
    ECONOMIC_VALUE,
    ECONOMIC_VALUE_DATE
)
SELECT
    e.ECONOMIC_ITEM_ID,
    r.REQUEST_ID,
    CASE
        WHEN e.ECONOMIC_ITEM_ID = 9000 THEN 25000.0000 -- Valor asegurado específico
        WHEN e.ECONOMIC_ITEM_ID = 2000 THEN 0.4641     -- Prima ejemplo
        WHEN e.ECONOMIC_ITEM_ID = 8000 THEN 0.0741     -- Impuesto ejemplo
        ELSE 0.0000                                    -- Valor por defecto
    END AS ECONOMIC_VALUE,
    NOW() AS ECONOMIC_VALUE_DATE
FROM (
         SELECT 1000 AS ECONOMIC_ITEM_ID UNION ALL
         SELECT 2000 UNION ALL
         SELECT 3000 UNION ALL
         SELECT 4000 UNION ALL
         SELECT 5000 UNION ALL
         SELECT 6000 UNION ALL
         SELECT 7000 UNION ALL
         SELECT 8000 UNION ALL
         SELECT 9000 UNION ALL
         SELECT 10000 UNION ALL
         SELECT 11000 UNION ALL
         SELECT 12000 UNION ALL
         SELECT 13000 UNION ALL
         SELECT 14000 UNION ALL
         SELECT 15000 UNION ALL
         SELECT 16000 UNION ALL
         SELECT 17000 UNION ALL
         SELECT 18000 UNION ALL
         SELECT 19000 UNION ALL
         SELECT 20000 UNION ALL
         SELECT 21000 UNION ALL
         SELECT 24000 UNION ALL
         SELECT 25000 UNION ALL
         SELECT 26000 UNION ALL
         SELECT 28000 UNION ALL
         SELECT 29000
     ) e
         CROSS JOIN REQUEST r
WHERE r.REQUEST_ID = (SELECT MAX(REQUEST_ID) FROM REQUEST) -- Usar el REQUEST_ID generado
  AND NOT EXISTS (
        SELECT 1
        FROM REQUEST_ECONOMICS re
        WHERE re.ECONOMIC_ITEM_ID = e.ECONOMIC_ITEM_ID
          AND re.REQUEST_ID = r.REQUEST_ID
    );
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error al insertar en REQUEST_ECONOMICS: %v", err)
	}

	fmt.Println("Datos insertados en REQUEST_ECONOMICS correctamente.")
	return nil
}
