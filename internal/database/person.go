package database

import (
	"database/sql"
	"fmt"
)

// Inserta datos en PERSON
func InsertPersonData(db *sql.DB) error {
	personQuery := `
	INSERT INTO PERSON (PARTY_ID, GENDER_ID, MARITAL_STATUS_ID, FIRST_NAME, LAST_NAME, MOTHER_LAST_NAME, BIRTH_DATE)
	SELECT p.PARTY_ID,
	       CASE t.CLAVESEXO WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE NULL END,
	       CASE t.ESTCIVIL WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 4 WHEN 4 THEN 8 ELSE NULL END,
	       t.NOMBRES, t.APEPATERNO, t.APEMATERNO, t.FECNAC
	FROM temp_csv_data t
	JOIN PARTY p ON p.EMAIL = t.EMAIL;`

	_, err := db.Exec(personQuery)
	if err != nil {
		return fmt.Errorf("error al insertar en PERSON: %v", err)
	}
	fmt.Println("Datos insertados en PERSON.")
	return nil
}
