package database

import (
	"database/sql"
	"fmt"
)

// Inser tParty Data inserta registros únicos en la tabla PARTY si no existen.
func InsertPartyData(db *sql.Tx) error {
	// Query para insertar PARTY si no existe
	insertPartyQuery := `
	INSERT INTO PARTY (EMAIL, DATE_CREATED, PARTY_SEARCH_AS, CIIU_ID, NATIONALITY, PARTY_ACTIVITY_ID, PRACTICE_ID, 
	                   PARTY_CLASS_ID, COUNTRY_OF_BIRTH, NATIONALITY_DETAIL)
	SELECT COALESCE(EMAIL, 'migracion@bicevida.cl'),
	       NOW(),
	       CONCAT(APEPATERNO, ' ', APEMATERNO, ', ', NOMBRES),
	       1000, 1, 2, 1, 1000, 136, 136
	FROM temp_cleaned_data t
	WHERE NOT EXISTS (
	    SELECT 1
	    FROM PARTY p
	    JOIN PARTY_IDENTIFICATION pi ON p.PARTY_ID = pi.PARTY_ID
	    JOIN IDENTIFICATION i ON pi.IDENTIFICATION_ID = i.IDENTIFICATION_ID
	    WHERE i.IDENTIFICATION = CONCAT(
	            TRIM(LEADING '0' FROM REPLACE(SUBSTRING_INDEX(t.RUT, '-', 1), '.', '')),
	            CASE
	                WHEN RIGHT(t.RUT, 1) = 'K' THEN 'K'
	                ELSE RIGHT(t.RUT, 1)
	            END
	    )
	);
	`

	// Ejecutar la query
	_, err := db.Exec(insertPartyQuery)
	if err != nil {
		return fmt.Errorf("error insertando datos en PARTY: %v", err)
	}
	fmt.Println("Datos insertados en PARTY correctamente.")

	// Query para contar cuántos registros se insertaron
	countQuery := `SELECT COUNT(*) AS total_insertados FROM PARTY;`

	var totalInsertados int
	err = db.QueryRow(countQuery).Scan(&totalInsertados)
	if err != nil {
		return fmt.Errorf("error al contar registros en PARTY: %v", err)
	}
	fmt.Printf("Total de registros insertados en PARTY: %d\n", totalInsertados)

	return nil
}
