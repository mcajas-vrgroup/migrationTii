package database

import (
	"database/sql"
	"fmt"
)

// Inserta datos en PARTY
func InsertPartyData(db *sql.DB) error {
	query := `
	INSERT INTO PARTY (EMAIL, DATE_CREATED, PARTY_SEARCH_AS)
	SELECT DISTINCT COALESCE(EMAIL, 'default@correo.com'), NOW(), 
	       CONCAT(APEPATERNO, ' ', APEMATERNO, ', ', NOMBRES)
	FROM temp_csv_data
	WHERE EMAIL IS NOT NULL
	ON DUPLICATE KEY UPDATE EMAIL=VALUES(EMAIL);`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error al insertar datos en PARTY: %v", err)
	}
	fmt.Println("Datos insertados en PARTY.")
	return nil
}
