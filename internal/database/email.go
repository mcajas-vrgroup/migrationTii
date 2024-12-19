package database

import (
	"database/sql"
	"fmt"
)

// Insert Email inserta correos electrónicos únicos en la tabla EMAIL.
func InsertEmail(db *sql.DB) error {
	query := `
	INSERT INTO EMAIL (EMAIL, EMAIL_TYPE_ID, EMAIL_DEFAULT)
	SELECT DISTINCT EMAIL, 1, NULL
	FROM temp_csv_data
	WHERE EMAIL IS NOT NULL
	ON DUPLICATE KEY UPDATE EMAIL=VALUES(EMAIL);
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error insertando EMAIL: %v", err)
	}
	fmt.Println("Datos insertados en EMAIL correctamente.")
	return nil
}

// Associate Party Email asocia EMAIL con PARTY en PARTY_EMAIL.
func AssociatePartyEmail(db *sql.DB) error {
	query := `
	INSERT INTO PARTY_EMAIL (EMAIL_ID, PARTY_ID)
	SELECT e.EMAIL_ID, p.PARTY_ID
	FROM EMAIL e
	JOIN PARTY p ON p.EMAIL = e.EMAIL;
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error asociando PARTY_EMAIL: %v", err)
	}
	fmt.Println("PARTY_EMAIL asociada correctamente.")
	return nil
}
