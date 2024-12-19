package database

import (
	"database/sql"
	"fmt"
)

// Insert Phone inserta números de teléfono únicos en la tabla PHONE.
func InsertPhone(db *sql.DB) error {
	query := `
	INSERT INTO PHONE (PHONE_TYPE_ID, COUNTRY_CODE, AREA_CODE, PHONE_NUMBER, PHONE_DEFAULT)
	SELECT DISTINCT 1, 56, 9, SUBSTRING_INDEX(TELEFONO, '-', -1), NULL
	FROM temp_csv_data
	WHERE TELEFONO IS NOT NULL;
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error insertando PHONE: %v", err)
	}
	fmt.Println("Datos insertados en PHONE correctamente.")
	return nil
}

// Associate Party Phone asocia PHONE con PARTY en PARTY_PHONE.
func AssociatePartyPhone(db *sql.DB) error {
	query := `
	INSERT INTO PARTY_PHONE (PHONE_ID, PARTY_ID)
	SELECT ph.PHONE_ID, p.PARTY_ID
	FROM PHONE ph
	JOIN PARTY p ON p.EMAIL = temp_csv_data.EMAIL
	WHERE ph.PHONE_NUMBER = SUBSTRING_INDEX(temp_csv_data.TELEFONO, '-', -1);
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error asociando PARTY_PHONE: %v", err)
	}
	fmt.Println("PARTY_PHONE asociada correctamente.")
	return nil
}
