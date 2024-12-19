package database

import (
	"database/sql"
	"fmt"
)

// Inserta y asocia IDENTIFICATION con PARTY
func InsertIdentificationData(db *sql.DB) error {
	identificationQuery := `
	INSERT INTO IDENTIFICATION (IDENTIFICATION, IDENTIFICATION_TYPE_ID)
	SELECT DISTINCT RUT, 1 FROM temp_csv_data
	WHERE RUT IS NOT NULL
	ON DUPLICATE KEY UPDATE IDENTIFICATION=VALUES(IDENTIFICATION);`

	associateQuery := `
	INSERT INTO PARTY_IDENTIFICATION (PARTY_ID, IDENTIFICATION_ID)
	SELECT p.PARTY_ID, i.IDENTIFICATION_ID
	FROM PARTY p
	JOIN temp_csv_data t ON p.EMAIL = t.EMAIL
	JOIN IDENTIFICATION i ON i.IDENTIFICATION = t.RUT
	ON DUPLICATE KEY UPDATE PARTY_ID=VALUES(PARTY_ID), IDENTIFICATION_ID=VALUES(IDENTIFICATION_ID);`

	if _, err := db.Exec(identificationQuery); err != nil {
		return fmt.Errorf("error en IDENTIFICATION: %v", err)
	}
	if _, err := db.Exec(associateQuery); err != nil {
		return fmt.Errorf("error al asociar IDENTIFICATION: %v", err)
	}
	fmt.Println("IDENTIFICATION y PARTY_IDENTIFICATION completados.")
	return nil
}
