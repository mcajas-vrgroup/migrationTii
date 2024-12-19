package database

import (
	"database/sql"
	"fmt"
)

// Crea la tabla temporal
func CreateTempTable(db *sql.DB) error {
	query := `
	CREATE TEMPORARY TABLE IF NOT EXISTS temp_csv_data (
		RAMO INT, NPOLIZA VARCHAR(50), NOMBRES VARCHAR(255), 
		APEMATERNO VARCHAR(100), APEPATERNO VARCHAR(100), RUT VARCHAR(50), 
		FECNAC DATE, CLAVESEXO INT, ESTCIVIL VARCHAR(50), 
		TELEFONO VARCHAR(50), EMAIL VARCHAR(200), DIRECCION VARCHAR(255), 
		CODREGION INT, REGION VARCHAR(100), CODCOMUNA INT, 
		COMUNA VARCHAR(100), CODCIUDAD INT, CIUDAD VARCHAR(100)
	) ENGINE=InnoDB;`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error al crear la tabla temporal: %v", err)
	}
	fmt.Println("Tabla temporal creada exitosamente.")
	return nil
}

// Carga los datos del CSV en la tabla temporal
func LoadDataToTempTable(db *sql.DB, records [][]string) error {
	// Query para insertar datos en la tabla temporal
	query := `
	INSERT INTO temp_csv_data (
		RAMO, NPOLIZA, NOMBRES, APEMATERNO, APEPATERNO, RUT, FECNAC, CLAVESEXO, 
		ESTCIVIL, TELEFONO, EMAIL, DIRECCION, CODREGION, REGION, CODCOMUNA, 
		COMUNA, CODCIUDAD, CIUDAD
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	// Preparar la consulta SQL
	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("error al preparar la consulta: %v", err)
	}
	defer stmt.Close()

	// Iterar sobre los registros y ejecutar la inserción
	for i, row := range records {
		if i == 0 {
			continue // Saltar la fila de encabezados
		}
		_, err := stmt.Exec(
			row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
			row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17],
		)
		if err != nil {
			return fmt.Errorf("error al insertar fila %d en la tabla temporal: %v", i, err)
		}
	}

	fmt.Printf("Se insertaron %d registros en la tabla temporal.\n", len(records)-1)
	return nil
}
