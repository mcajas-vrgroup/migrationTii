package services

import (
	"database/sql"
	"fmt"
	"migrationTii/internal/data_loader"
)

func InsertPartyData(db *sql.DB, records [][]string) error {
	query := `
	INSERT INTO PARTY (EMAIL, DATE_CREATED, PARTY_SEARCH_AS)
	SELECT ?, NOW(), CONCAT(?, ', ', ?)
	ON DUPLICATE KEY UPDATE EMAIL=VALUES(EMAIL);`

	for i, row := range records {
		if i == 0 {
			continue // Saltar encabezado
		}

		_, err := db.Exec(query, row[10], row[4], row[2]) // EMAIL, APEPATERNO, NOMBRES
		if err != nil {
			return fmt.Errorf("error al insertar PARTY: %v", err)
		}
	}

	fmt.Println("Datos insertados en PARTY correctamente.")
	data_loader.AddToSqlScript("-- insertar PARTY")
	data_loader.AddToSqlScript(query)
	return nil
}
