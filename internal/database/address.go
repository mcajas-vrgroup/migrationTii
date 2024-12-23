package database

import (
	"database/sql"
	"fmt"
)

// Insert Address inserta datos únicos en la tabla ADDRESS.
func InsertAddress(db *sql.Tx) error {
	insertAddressQuery := `
	INSERT INTO ADDRESS (
		ADDRESS_TYPE_ID, ADDRESS_STREET, ADDRESS_NUMBER, ADDRESS_APARTMENT, 
		CITY_ID, ADDRESS_COMMENT, PROVINCE_ID, ADDRESS_DEFAULT
	)
	SELECT DISTINCT
		3, -- ADDRESS_TYPE_ID = 3 (particular)
		TRIM(REGEXP_REPLACE(DIRECCION, '[0-9].*$', '')) AS ADDRESS_STREET, -- Todo antes del primer número
		REGEXP_SUBSTR(DIRECCION, '[0-9]+') AS ADDRESS_NUMBER, -- Primer número encontrado
		CASE
			WHEN DIRECCION REGEXP 'depto|piso' THEN SUBSTRING_INDEX(DIRECCION, ' ', -1)
			ELSE NULL
		END AS ADDRESS_APARTMENT,
		c.id AS CITY_ID,
		CONCAT(t.REGION, ', ', t.COMUNA, ', ', t.CIUDAD) AS ADDRESS_COMMENT,
		pr.id AS PROVINCE_ID,
		NULL AS ADDRESS_DEFAULT
	FROM temp_csv_data t
		LEFT JOIN province pr ON pr.name = t.COMUNA -- Mapeo de provincia
		LEFT JOIN city c ON c.name = t.CIUDAD; -- Mapeo de ciudad
	`

	if _, err := db.Exec(insertAddressQuery); err != nil {
		return fmt.Errorf("error insertando en ADDRESS: %v", err)
	}
	fmt.Println("Datos insertados en ADDRESS correctamente.")
	return nil
}

// Associate Party Address asocia las direcciones con PARTY en la tabla PARTY_ADDRESS.
func AssociatePartyAddress(db *sql.Tx) error {
	associateQuery := `
	INSERT INTO PARTY_ADDRESS (ADDRESS_ID, PARTY_ID)
	SELECT a.ADDRESS_ID, p.PARTY_ID
	FROM ADDRESS a
		JOIN temp_csv_data t
			ON a.ADDRESS_STREET = TRIM(REGEXP_REPLACE(t.DIRECCION, '[0-9].*$', ''))
			AND a.ADDRESS_NUMBER = REGEXP_SUBSTR(t.DIRECCION, '[0-9]+')
		JOIN PARTY p ON p.EMAIL = t.EMAIL
	WHERE t.DIRECCION IS NOT NULL;
	`

	if _, err := db.Exec(associateQuery); err != nil {
		return fmt.Errorf("error asociando PARTY_ADDRESS: %v", err)
	}
	fmt.Println("PARTY_ADDRESS asociado correctamente.")
	return nil
}
