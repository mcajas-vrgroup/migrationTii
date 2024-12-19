package database

import (
	"database/sql"
	"fmt"
)

// Inserta datos en PARTY
func InsertPartyData(db *sql.DB) error {
	query := `
	INSERT INTO PARTY (EMAIL, DATE_CREATED, PARTY_SEARCH_AS, CIIU_ID, NATIONALITY, PARTY_ACTIVITY_ID, PRACTICE_ID,
					   PARTY_CLASS_ID, COUNTRY_OF_BIRTH, NATIONALITY_DETAIL)
	SELECT COALESCE(EMAIL, 'correo@default.cl'), NOW(),
		   CONCAT(APEPATERNO, ' ', APEMATERNO, ', ', NOMBRES), 1000, 1, 2, 1, 1000, 136, 136
	FROM (SELECT DISTINCT RUT, EMAIL, NOMBRES, APEPATERNO, APEMATERNO FROM temp_csv_data GROUP BY RUT) AS cleaned_data;
	`

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error insertando PARTY: %v", err)
	}
	fmt.Println("Datos insertados en PARTY correctamente.")
	return nil
}
