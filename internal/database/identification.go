package database

import (
	"database/sql"
	"fmt"
)

// Create Temp Cleaned RUT crea una tabla temporal con RUT limpios.
func CreateTempCleanedRUT(db *sql.Tx) error {
	query := `
	CREATE TEMPORARY TABLE temp_cleaned_rut AS
	SELECT DISTINCT
	    RUT,
	    CONCAT(
	            TRIM(LEADING '0' FROM REPLACE(SUBSTRING_INDEX(RUT, '-', 1), '.', '')),
	            CASE
	                WHEN RIGHT(RUT, 1) = 'K' THEN 'K'
	                ELSE RIGHT(RUT, 1)
	            END
	    ) AS CLEAN_RUT
	FROM temp_csv_data
	WHERE RUT IS NOT NULL AND RUT != '';
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error creando temp_cleaned_rut: %v", err)
	}
	fmt.Println("Tabla temp_cleaned_rut creada correctamente.")
	return nil
}

// Insert Identification inserta los valores limpios en IDENTIFICATION.
func InsertIdentification(db *sql.Tx) error {
	query := `
	INSERT INTO IDENTIFICATION (IDENTIFICATION, IDENTIFICATION_TYPE_ID)
	SELECT CLEAN_RUT, 1
	FROM temp_cleaned_rut
	ON DUPLICATE KEY UPDATE IDENTIFICATION=VALUES(IDENTIFICATION);
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error insertando en IDENTIFICATION: %v", err)
	}
	fmt.Println("Datos insertados en IDENTIFICATION correctamente.")
	return nil
}

// Associate Party Identification asocia PARTY con IDENTIFICATION.
func AssociatePartyIdentification(db *sql.Tx) error {
	query := `
	INSERT INTO PARTY_IDENTIFICATION (PARTY_ID, IDENTIFICATION_ID)
	SELECT p.PARTY_ID, i.IDENTIFICATION_ID
	FROM PARTY p
	JOIN temp_csv_data t ON p.PARTY_SEARCH_AS = CONCAT_WS(', ', t.APEPATERNO, t.APEMATERNO, t.NOMBRES)
	JOIN IDENTIFICATION i ON i.IDENTIFICATION = CONCAT(
	            TRIM(LEADING '0' FROM REPLACE(SUBSTRING_INDEX(t.RUT, '-', 1), '.', '')),
	            CASE
	                WHEN RIGHT(t.RUT, 1) = 'K' THEN 'K'
	                ELSE RIGHT(t.RUT, 1)
	            END
	)
	WHERE t.RUT IS NOT NULL AND t.RUT != ''
	ON DUPLICATE KEY UPDATE IDENTIFICATION_ID = i.IDENTIFICATION_ID;
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error asociando PARTY_IDENTIFICATION: %v", err)
	}
	fmt.Println("PARTY_IDENTIFICATION asociada correctamente.")
	return nil
}
