package database

import (
	"database/sql"
	"fmt"
	"log"
)

// InsertEmailData inserta registros únicos en EMAIL.
//func InsertEmail(db *sql.Tx) error {
//	// Insertar en EMAIL
//	insertEmailQuery := `
//	INSERT INTO EMAIL (EMAIL, EMAIL_TYPE_ID, EMAIL_DEFAULT)
//	SELECT DISTINCT EMAIL, 1, NULL
//	FROM temp_csv_data
//	WHERE EMAIL IS NOT NULL
//	ON DUPLICATE KEY UPDATE EMAIL=VALUES(EMAIL);
//	`
//
//	// Deshabilitar índices de PARTY_EMAIL para optimizar
//	disableKeys := `ALTER TABLE PARTY_EMAIL DISABLE KEYS;`
//	enableKeys := `ALTER TABLE PARTY_EMAIL ENABLE KEYS;`
//
//	// Asociar EMAIL con PARTY en PARTY_EMAIL
//	insertPartyEmailQuery := `
//	INSERT IGNORE INTO PARTY_EMAIL (EMAIL_ID, PARTY_ID)
//	SELECT e.EMAIL_ID, p.PARTY_ID
//	FROM EMAIL e
//	JOIN PARTY p ON p.EMAIL = e.EMAIL;
//	`
//
//	// Ejecutar las querys
//	if _, err := db.Exec(insertEmailQuery); err != nil {
//		return fmt.Errorf("error insertando en EMAIL: %v", err)
//	}
//
//	if _, err := db.Exec(disableKeys); err != nil {
//		return fmt.Errorf("error deshabilitando índices de PARTY_EMAIL: %v", err)
//	}
//
//	if _, err := db.Exec(insertPartyEmailQuery); err != nil {
//		return fmt.Errorf("error asociando PARTY_EMAIL: %v", err)
//	}
//
//	if _, err := db.Exec(enableKeys); err != nil {
//		return fmt.Errorf("error habilitando índices de PARTY_EMAIL: %v", err)
//	}
//
//	fmt.Println("EMAIL y PARTY_EMAIL insertados correctamente.")
//	return nil
//}

func InsertEmail(db *sql.Tx) error {
	// Insertar directamente en EMAIL
	insertEmailQuery := `
	INSERT INTO EMAIL (EMAIL, EMAIL_TYPE_ID, EMAIL_DEFAULT)
	SELECT DISTINCT EMAIL, 1, NULL
	FROM temp_csv_data
	WHERE EMAIL IS NOT NULL;
	`

	// Asociar el nuevo EMAIL_ID al PARTY_ID en PARTY_EMAIL
	insertPartyEmailQuery := `
	INSERT INTO PARTY_EMAIL (EMAIL_ID, PARTY_ID)
	SELECT e.EMAIL_ID, p.PARTY_ID
	FROM EMAIL e
	JOIN temp_csv_data t ON e.EMAIL = t.EMAIL
	JOIN PARTY p ON p.PARTY_ID = (
	    SELECT PARTY_ID
	    FROM PARTY
	    WHERE EMAIL = t.EMAIL
	)
	WHERE t.EMAIL IS NOT NULL;
	`

	log.Println("Iniciando inserción en EMAIL...")

	// Ejecutar la query para insertar emails
	if _, err := db.Exec(insertEmailQuery); err != nil {
		return fmt.Errorf("error insertando en EMAIL: %v", err)
	}

	log.Println("Emails insertados correctamente.")

	// Ejecutar la query para asociar emails con PARTY
	if _, err := db.Exec(insertPartyEmailQuery); err != nil {
		return fmt.Errorf("error asociando PARTY_EMAIL: %v", err)
	}

	log.Println("PARTY_EMAIL asociado correctamente.")
	return nil
}
