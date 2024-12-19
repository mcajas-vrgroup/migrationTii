package database

import (
	"database/sql"
	"fmt"
)

// Insert PaymentTerm inserta datos en PAYMENT_TERM asociados a PARTY_ID.
func InsertPaymentTerm(db *sql.DB, paymentData []map[string]string) error {
	query := `
	INSERT INTO PAYMENT_TERM (
		PARTY_ID, PAYMENT_TYPE_ID, CURRENCY_ID, BANK_ID, EXPIRATION,
		ACCOUNT_NBR, INTER_ACCOUNT_NBR, PAYMENT_KEY, REQUIRES_RECEIPT
	)
	SELECT p.PARTY_ID, ?, 3000, ?, ?, NULL, NULL, NULL, NULL
	FROM POLICY po
	JOIN PARTY p ON p.PARTY_ID = po.PARTY_ID
	WHERE po.POLICY_NUMBER = ?
	ON DUPLICATE KEY UPDATE 
		PAYMENT_TYPE_ID=VALUES(PAYMENT_TYPE_ID), 
		BANK_ID=VALUES(BANK_ID);`

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error al iniciar la transacción: %v", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("error al preparar la consulta: %v", err)
	}
	defer stmt.Close()

	// Iterar sobre los datos y ejecutar la inserción
	for _, row := range paymentData {
		_, err := stmt.Exec(
			row["PAYMENT_TYPE_ID"],
			row["BANK_ID"],
			row["EXPIRATION"],
			row["NPOLIZA"],
		)
		if err != nil {
			return fmt.Errorf("error insertando PAYMENT_TERM: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error al confirmar la transacción: %v", err)
	}

	fmt.Println("Datos insertados en PAYMENT_TERM correctamente.")
	return nil
}
