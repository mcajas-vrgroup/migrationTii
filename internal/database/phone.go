package database

import (
	"database/sql"
	"fmt"
	"log"
	"migrationTii/internal/data_loader"
)

// InsertPhoneData inserta registros únicos en PHONE y asocia con PARTY_PHONE.
//func InsertPhone(db *sql.Tx) error {
//	// Insertar en PHONE
//	insertPhoneQuery := `
//	ALTER TABLE PHONE DISABLE KEYS;
//
//	INSERT INTO PHONE (PHONE_TYPE_ID, COUNTRY_CODE, AREA_CODE, PHONE_NUMBER, PHONE_DEFAULT)
//	SELECT DISTINCT 1, 56, 9, SUBSTRING_INDEX(TELEFONO, '-', -1), NULL
//	FROM temp_csv_data
//	WHERE TELEFONO IS NOT NULL;
//
//	ALTER TABLE PHONE ENABLE KEYS;
//	`
//
//	// Asociar PHONE con PARTY en PARTY_PHONE
//	disableKeys := `ALTER TABLE PARTY_PHONE DISABLE KEYS;`
//	enableKeys := `ALTER TABLE PARTY_PHONE ENABLE KEYS;`
//
//	insertPartyPhoneQuery := `
//	INSERT INTO PARTY_PHONE (PHONE_ID, PARTY_ID)
//	SELECT ph.PHONE_ID, p.PARTY_ID
//	FROM PHONE ph
//	JOIN temp_csv_data t ON ph.PHONE_NUMBER = SUBSTRING_INDEX(t.TELEFONO, '-', -1)
//	JOIN PARTY p ON p.EMAIL = t.EMAIL
//	WHERE t.TELEFONO IS NOT NULL;
//	`
//
//	// Ejecutar las querys
//	if _, err := db.Exec(insertPhoneQuery); err != nil {
//		return fmt.Errorf("error insertando en PHONE: %v", err)
//	}
//
//	if _, err := db.Exec(disableKeys); err != nil {
//		return fmt.Errorf("error deshabilitando índices de PARTY_PHONE: %v", err)
//	}
//
//	if _, err := db.Exec(insertPartyPhoneQuery); err != nil {
//		return fmt.Errorf("error asociando PARTY_PHONE: %v", err)
//	}
//
//	if _, err := db.Exec(enableKeys); err != nil {
//		return fmt.Errorf("error habilitando índices de PARTY_PHONE: %v", err)
//	}
//
//	fmt.Println("PHONE y PARTY_PHONE insertados correctamente.")
//	return nil
//}

func InsertPhone(db *sql.Tx) error {
	// Insertar directamente en PHONE
	insertPhoneQuery := `
	INSERT INTO PHONE (PHONE_TYPE_ID, COUNTRY_CODE, AREA_CODE, PHONE_NUMBER, PHONE_DEFAULT)
	SELECT DISTINCT 1, 56, 9, SUBSTRING_INDEX(TELEFONO, '-', -1), NULL
	FROM temp_csv_data
	WHERE TELEFONO IS NOT NULL;
	`

	// Asociar los nuevos PHONE_ID al PARTY_ID en PARTY_PHONE
	insertPartyPhoneQuery := `
	INSERT INTO PARTY_PHONE (PHONE_ID, PARTY_ID)
	SELECT ph.PHONE_ID, p.PARTY_ID
	FROM PHONE ph
	JOIN temp_csv_data t ON ph.PHONE_NUMBER = SUBSTRING_INDEX(t.TELEFONO, '-', -1)
	JOIN PARTY p ON p.EMAIL = t.EMAIL
	WHERE t.TELEFONO IS NOT NULL;
	`

	log.Println("Iniciando inserción en PHONE...")

	// Ejecutar la query para insertar teléfonos
	if _, err := db.Exec(insertPhoneQuery); err != nil {
		return fmt.Errorf("error insertando en PHONE: %v", err)
	}

	log.Println("Teléfonos insertados correctamente.")
	data_loader.AddToSqlScript("\n-- InsertPhoneData inserta registros únicos en PHONE.\n\n")
	data_loader.AddToSqlScript(insertPhoneQuery)

	// Ejecutar la query para asociar teléfonos con PARTY
	if _, err := db.Exec(insertPartyPhoneQuery); err != nil {
		return fmt.Errorf("error asociando PARTY_PHONE: %v", err)
	}

	log.Println("PARTY_PHONE asociado correctamente.")
	data_loader.AddToSqlScript("\n-- Asocia con PARTY_PHONE.\n\n")
	data_loader.AddToSqlScript(insertPartyPhoneQuery)
	return nil
}
