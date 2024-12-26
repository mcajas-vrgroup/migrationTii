package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
)

// file Exists verifica si un archivo existe en la ruta proporcionada.
func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

// Create TempTables crea tablas temporales para asegurados y pólizas.
func CreateTempTable(db *sql.Tx) error {
	queries := []string{
		`CREATE TEMPORARY TABLE temp_csv_data (
			RAMO INT, NPOLIZA VARCHAR(50), NOMBRES VARCHAR(255),
			APEMATERNO VARCHAR(100), APEPATERNO VARCHAR(100), RUT VARCHAR(50),
			FECNAC DATE, CLAVESEXO INT, DESCSEXO VARCHAR(50), CODCIVIL INT,
			ESTCIVIL VARCHAR(50), TELEFONO VARCHAR(50), EMAIL VARCHAR(200),
			DIRECCION VARCHAR(255), CODREGION INT, REGION VARCHAR(100),
			CODCOMUNA INT, COMUNA VARCHAR(100), CODCIUDAD INT, CIUDAD VARCHAR(100)
		) CHARSET=utf8mb4;`,

		`CREATE TEMPORARY TABLE temp_polizas_data (
			RAMO INT, NPOLIZA VARCHAR(50), REQUEST VARCHAR(50), CODESTADO VARCHAR(10),
			ESTADO VARCHAR(50), NPOLORI VARCHAR(50), FINIVIG DATE, FTERVIG DATE,
			IDCONDCOBRO VARCHAR(50), DESCCONDCOBRO VARCHAR(100), TPCONDCOBRO VARCHAR(10),
			DESCTPCONDCOBRO VARCHAR(50),NROCONDCOBRO INT, IDPERIODPAGO VARCHAR(10), DESCPERPAGO VARCHAR(50)
		);`,
	}

	fmt.Println("Creando tablas temporales...")

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("error creando tabla temporal: %v", err)
		}
	}
	fmt.Println("Tablas temporales creadas exitosamente.")
	log.Println(queries)
	return nil
}

// Create Cleaned Temp Table crea una tabla temporal con datos únicos.
func CreateCleanedTempTable(db *sql.Tx) error {
	query := `
	CREATE TEMPORARY TABLE temp_cleaned_data AS
	SELECT RAMO, NPOLIZA, NOMBRES, APEMATERNO, APEPATERNO, RUT, FECNAC, CLAVESEXO,
	       DESCSEXO, CODCIVIL, ESTCIVIL,
	       MAX(TELEFONO)  AS TELEFONO,
	       MAX(EMAIL)     AS EMAIL,
	       MAX(DIRECCION) AS DIRECCION,
	       CODREGION, REGION, CODCOMUNA, COMUNA, CODCIUDAD, CIUDAD
	FROM temp_csv_data
	GROUP BY RUT;
	`

	fmt.Println("Creando tabla limpia temporal (temp_cleaned_data)...")

	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error creando temp_cleaned_data: %v", err)
	}
	fmt.Println("Tabla temp_cleaned_data creada correctamente.")
	log.Println(query)
	return nil
}

// Load AseguradosData carga los datos procesados a la tabla temp_csv_data.
func LoadAseguradosData(db *sql.Tx, records []map[string]string) error {
	query := `INSERT INTO temp_csv_data (
		RAMO, NPOLIZA, NOMBRES, APEMATERNO, APEPATERNO, RUT, FECNAC, CLAVESEXO, 
		ESTCIVIL, TELEFONO, EMAIL, DIRECCION, CODREGION, REGION, CODCOMUNA, 
		COMUNA, CODCIUDAD, CIUDAD
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("error preparando la consulta: %v", err)
	}
	//defer stmt.Close()

	for i, row := range records {

		queryData := strings.Replace(query, "?", "'%v'", -1)
		log.Println(fmt.Sprintf(queryData, row["RAMO"], row["NPOLIZA"], row["NOMBRES"], row["APEMATERNO"], row["APEPATERNO"], row["RUT"],
			row["FECNAC"], row["CLAVESEXO"], row["ESTCIVIL"], row["TELEFONO"], row["EMAIL"], row["DIRECCION"],
			row["CODREGION"], row["REGION"], row["CODCOMUNA"], row["COMUNA"], row["CODCIUDAD"], row["CIUDAD"]))

		_, err := stmt.Exec(
			row["RAMO"], row["NPOLIZA"], row["NOMBRES"], row["APEMATERNO"], row["APEPATERNO"], row["RUT"],
			row["FECNAC"], row["CLAVESEXO"], row["ESTCIVIL"], row["TELEFONO"], row["EMAIL"], row["DIRECCION"],
			row["CODREGION"], row["REGION"], row["CODCOMUNA"], row["COMUNA"], row["CODCIUDAD"], row["CIUDAD"],
		)

		if err != nil {
			log.Printf("Error insertando fila #%d: %v. Datos: %+v", i+1, err, row)
			return fmt.Errorf("error insertando fila: %v", err)
		}
	}
	log.Println("Datos de asegurados insertados correctamente en temp_csv_data.")
	log.Println(query)
	return nil
}

// Load PolizasData carga los datos procesados a la tabla temp_polizas_data.
func LoadPolizasData(db *sql.Tx, data []map[string]string) error {
	query := `INSERT INTO temp_polizas_data (
		RAMO, NPOLIZA, REQUEST, CODESTADO, ESTADO, NPOLORI, FINIVIG, FTERVIG,
		IDCONDCOBRO, DESCCONDCOBRO, TPCONDCOBRO, DESCTPCONDCOBRO, NROCONDCOBRO, IDPERIODPAGO, DESCPERPAGO
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`

	stmt, err := db.Prepare(query)
	if err != nil {
		return fmt.Errorf("error preparando la consulta: %v", err)
	}
	//defer stmt.Close()

	for _, row := range data {
		_, err := stmt.Exec(
			row["RAMO"], row["NPOLIZA"], row["REQUEST"], row["CODESTADO"],
			row["ESTADO"], row["NPOLORI"], row["FINIVIG"], row["FTERVIG"],
			row["IDCONDCOBRO"], row["DESCCONDCOBRO"], row["TPCONDCOBRO"],
			row["DESCTPCONDCOBRO"], row["NROCONDCOBRO"], row["IDPERIODPAGO"], row["DESCPERPAGO"],
		)
		log.Println(row)
		if err != nil {
			return fmt.Errorf("error insertando pólizas: %v", err)
		}
	}
	fmt.Println("Datos de pólizas insertados correctamente.")
	log.Println(query)
	return nil
}

func CreateTempOriginalPolicyTable(db *sql.Tx) error {
	query := `
    CREATE TEMPORARY TABLE temp_original_policy AS
    SELECT
        RAMO,
        NPOLORI,
        MIN(FINIVIG) AS POLICY_ISSUANCE_DATE,
        MIN(FTERVIG) AS POLICY_ENDORSEMENT_DATE_TO
    FROM temp_polizas_data
    WHERE CODESTADO = '03'
    GROUP BY RAMO, NPOLORI;`
	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error creando temp_original_policy: %v", err)
	}
	fmt.Println("Tabla temporal temp_original_policy creada correctamente.")
	return nil
}
