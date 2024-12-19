package database

import (
	"database/sql"
	"fmt"
)

// Inserta en PHONE y asocia a PARTY_PHONE
func InsertPhoneData(db *sql.DB, phoneData map[string]string) error {
	phoneQuery := `
	INSERT INTO PHONE (PHONE_TYPE_ID, COUNTRY_CODE, AREA_CODE, PHONE_NUMBER, PHONE_DEFAULT)
	VALUES (1, 56, 9, ?, NULL)
	ON DUPLICATE KEY UPDATE PHONE_NUMBER=VALUES(PHONE_NUMBER);`

	partyPhoneQuery := `
	INSERT IGNORE INTO PARTY_PHONE (PHONE_ID, PARTY_ID)
	SELECT ph.PHONE_ID, p.PARTY_ID
	FROM PHONE ph
	JOIN PARTY p ON p.EMAIL = ?
	WHERE ph.PHONE_NUMBER = ?;`

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	phoneStmt, _ := tx.Prepare(phoneQuery)
	partyPhoneStmt, _ := tx.Prepare(partyPhoneQuery)

	for email, phone := range phoneData {
		_, err := phoneStmt.Exec(phone)
		if err != nil {
			return fmt.Errorf("error insertando PHONE: %v", err)
		}
		_, err = partyPhoneStmt.Exec(email, phone)
		if err != nil {
			return fmt.Errorf("error asociando PARTY_PHONE: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error al confirmar transacción: %v", err)
	}
	fmt.Println("PHONE y PARTY_PHONE insertados correctamente.")
	return nil
}
