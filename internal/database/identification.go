package database

import (
	"database/sql"
	"fmt"
)

// Inserta y asocia IDENTIFICATION con PARTY
func InsertIdentificationData(db *sql.DB) error {
	query := `
	INSERT INTO IDENTIFICATION (IDENTIFICATION, IDENTIFICATION_TYPE_ID)
	SELECT DISTINCT
		CONCAT(
			CAST(TRIM(LEADING '0' FROM REPLACE(RUT, '.', '')) AS UNSIGNED),
			CASE WHEN RIGHT(RUT, 1) = 'K' THEN 'K' ELSE '' END
		),
		1
	FROM temp_csv_data
	WHERE RUT IS NOT NULL AND RUT != ''
	ON DUPLICATE KEY UPDATE IDENTIFICATION=VALUES(IDENTIFICATION);
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error insertando datos en IDENTIFICATION: %v", err)
	}
	fmt.Println("Datos insertados en IDENTIFICATION correctamente.")
	return nil
}

func AssociatePartyIdentification(db *sql.DB) error {
	query := `
	INSERT INTO PARTY_IDENTIFICATION (PARTY_ID, IDENTIFICATION_ID)
	SELECT p.PARTY_ID, i.IDENTIFICATION_ID
	FROM PARTY p
	JOIN temp_csv_data t
		ON p.PARTY_SEARCH_AS = CONCAT_WS(', ', t.APEPATERNO, t.APEMATERNO, t.NOMBRES)
	JOIN IDENTIFICATION i
		ON i.IDENTIFICATION = CONCAT(
			CAST(TRIM(LEADING '0' FROM REPLACE(t.RUT, '.', '')) AS UNSIGNED),
			CASE WHEN RIGHT(t.RUT, 1) = 'K' THEN 'K' ELSE '' END
		)
	WHERE t.RUT IS NOT NULL AND t.RUT != ''
	ON DUPLICATE KEY UPDATE PARTY_ID=VALUES(PARTY_ID), IDENTIFICATION_ID=VALUES(IDENTIFICATION_ID);
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error asociando PARTY_IDENTIFICATION: %v", err)
	}
	fmt.Println("PARTY_IDENTIFICATION asociada correctamente.")
	return nil
}
