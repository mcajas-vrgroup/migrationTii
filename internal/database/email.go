package database

import (
	"database/sql"
	"fmt"
)

// Insertar EMAIL y asociar PARTY_EMAIL
func InsertEmailData(db *sql.DB, emails []string) error {
	emailQuery := `
	INSERT INTO EMAIL (EMAIL, EMAIL_TYPE_ID, EMAIL_DEFAULT)
	VALUES (?, 1, NULL)
	ON DUPLICATE KEY UPDATE EMAIL=VALUES(EMAIL);`

	partyEmailQuery := `
	INSERT IGNORE INTO PARTY_EMAIL (EMAIL_ID, PARTY_ID)
	SELECT e.EMAIL_ID, p.PARTY_ID
	FROM EMAIL e
	JOIN PARTY p ON p.EMAIL = e.EMAIL
	WHERE e.EMAIL = ?;`

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmtEmail, _ := tx.Prepare(emailQuery)
	stmtPartyEmail, _ := tx.Prepare(partyEmailQuery)

	for _, email := range emails {
		if _, err := stmtEmail.Exec(email); err != nil {
			tx.Rollback()
			return fmt.Errorf("error insertando EMAIL: %v", err)
		}
		if _, err := stmtPartyEmail.Exec(email); err != nil {
			tx.Rollback()
			return fmt.Errorf("error asociando PARTY_EMAIL: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error al commitear transacción: %v", err)
	}
	fmt.Println("EMAIL y PARTY_EMAIL insertados correctamente.")
	return nil
}
