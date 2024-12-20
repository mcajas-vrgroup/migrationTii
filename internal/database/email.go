package database

import (
	"database/sql"
	"fmt"
)

// InsertEmailData inserta registros únicos en EMAIL.
func InsertEmail(db *sql.DB) error {
	// Insertar en EMAIL
	insertEmailQuery := `
	INSERT INTO EMAIL (EMAIL, EMAIL_TYPE_ID, EMAIL_DEFAULT)
	SELECT DISTINCT EMAIL, 1, NULL
	FROM temp_csv_data
	WHERE EMAIL IS NOT NULL
	ON DUPLICATE KEY UPDATE EMAIL=VALUES(EMAIL);
	`

	// Deshabilitar índices de PARTY_EMAIL para optimizar
	disableKeys := `ALTER TABLE PARTY_EMAIL DISABLE KEYS;`
	enableKeys := `ALTER TABLE PARTY_EMAIL ENABLE KEYS;`

	// Asociar EMAIL con PARTY en PARTY_EMAIL
	insertPartyEmailQuery := `
	INSERT IGNORE INTO PARTY_EMAIL (EMAIL_ID, PARTY_ID)
	SELECT e.EMAIL_ID, p.PARTY_ID
	FROM EMAIL e
	JOIN PARTY p ON p.EMAIL = e.EMAIL;
	`

	// Ejecutar las querys
	if _, err := db.Exec(insertEmailQuery); err != nil {
		return fmt.Errorf("error insertando en EMAIL: %v", err)
	}

	if _, err := db.Exec(disableKeys); err != nil {
		return fmt.Errorf("error deshabilitando índices de PARTY_EMAIL: %v", err)
	}

	if _, err := db.Exec(insertPartyEmailQuery); err != nil {
		return fmt.Errorf("error asociando PARTY_EMAIL: %v", err)
	}

	if _, err := db.Exec(enableKeys); err != nil {
		return fmt.Errorf("error habilitando índices de PARTY_EMAIL: %v", err)
	}

	fmt.Println("EMAIL y PARTY_EMAIL insertados correctamente.")
	return nil
}
