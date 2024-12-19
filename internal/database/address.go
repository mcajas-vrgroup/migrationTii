package database

import (
	"database/sql"
	"fmt"
)

// Insert Address inserta direcciones en la tabla ADDRESS.
func InsertAddress(db *sql.DB) error {
	query := `
	INSERT INTO ADDRESS (
		ADDRESS_TYPE_ID, ADDRESS_STREET, ADDRESS_NUMBER, ADDRESS_APARTMENT, 
		CITY_ID, ADDRESS_COMMENT, PROVINCE_ID, ADDRESS_DEFAULT
	)
	SELECT DISTINCT
		3,
		TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(DIRECCION, ' ', 2), ' ', -2)),
		SUBSTRING_INDEX(DIRECCION, ' ', -1),
		CASE
			WHEN DIRECCION LIKE '%depto%' OR DIRECCION LIKE '%piso%' THEN SUBSTRING_INDEX(DIRECCION, ' ', -1)
			ELSE NULL
		END,
		p.CITY_ID,
		CONCAT(t.REGION, ', ', t.COMUNA, ', ', t.CIUDAD),
		pr.id,
		NULL
	FROM temp_csv_data t
	LEFT JOIN province pr ON pr.name = t.COMUNA
	LEFT JOIN city p ON p.name = t.CIUDAD
	WHERE DIRECCION IS NOT NULL;
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error insertando ADDRESS: %v", err)
	}
	fmt.Println("Datos insertados en ADDRESS correctamente.")
	return nil
}

// Associate PartyAddress asocia ADDRESS con PARTY en PARTY_ADDRESS.
func AssociatePartyAddress(db *sql.DB) error {
	query := `
	INSERT INTO PARTY_ADDRESS (ADDRESS_ID, PARTY_ID)
	SELECT a.ADDRESS_ID, p.PARTY_ID
	FROM ADDRESS a
	JOIN PARTY p ON p.EMAIL = t.EMAIL
	JOIN temp_csv_data t
	ON a.ADDRESS_STREET = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(t.DIRECCION, ' ', 2), ' ', -2))
	AND a.ADDRESS_NUMBER = SUBSTRING_INDEX(t.DIRECCION, ' ', -1)
	WHERE t.DIRECCION IS NOT NULL;
	`
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("error asociando PARTY_ADDRESS: %v", err)
	}
	fmt.Println("PARTY_ADDRESS asociada correctamente.")
	return nil
}

//// Inserta en ADDRESS y asocia a PARTY_ADDRESS
//func InsertAddressData(db *sql.DB, addressData []map[string]string, regionMapping map[string]string) error {
//	addressQuery := `
//	INSERT INTO ADDRESS (ADDRESS_TYPE_ID, ADDRESS_STREET, ADDRESS_NUMBER, ADDRESS_APARTMENT,
//						 CITY_ID, ADDRESS_COMMENT, PROVINCE_ID, ADDRESS_DEFAULT)
//	VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
//	ON DUPLICATE KEY UPDATE ADDRESS_STREET=VALUES(ADDRESS_STREET);`
//
//	partyAddressQuery := `
//	INSERT IGNORE INTO PARTY_ADDRESS (ADDRESS_ID, PARTY_ID)
//	SELECT a.ADDRESS_ID, p.PARTY_ID
//	FROM ADDRESS a
//	JOIN PARTY p ON p.EMAIL = ?
//	WHERE a.ADDRESS_STREET = ? AND a.ADDRESS_NUMBER = ?;`
//
//	tx, err := db.Begin()
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback()
//
//	addressStmt, _ := tx.Prepare(addressQuery)
//	partyAddressStmt, _ := tx.Prepare(partyAddressQuery)
//
//	for _, row := range addressData {
//		// Dividir la dirección usando SplitAddress
//		addressStreet, addressNumber, addressApartment := data_loader.SplitAddress(row["DIRECCION"])
//
//		// Obtener provinceID usando clave compuesta "REGION-COMUNA"
//		regionKey := fmt.Sprintf("%s-%s", row["REGION"], row["COMUNA"])
//		provinceID := regionMapping[regionKey]
//
//		// Insertar en ADDRESS
//		_, err := addressStmt.Exec(
//			3, addressStreet, addressNumber, addressApartment,
//			row["CODCIUDAD"], fmt.Sprintf("%s, %s", row["REGION"], row["COMUNA"]), provinceID,
//		)
//		if err != nil {
//			return fmt.Errorf("error insertando ADDRESS: %v", err)
//		}
//
//		// Asociar ADDRESS con PARTY
//		_, err = partyAddressStmt.Exec(row["EMAIL"], addressStreet, addressNumber)
//		if err != nil {
//			return fmt.Errorf("error asociando PARTY_ADDRESS: %v", err)
//		}
//	}
//
//	if err := tx.Commit(); err != nil {
//		return fmt.Errorf("error al confirmar transacción: %v", err)
//	}
//	fmt.Println("ADDRESS y PARTY_ADDRESS insertados correctamente.")
//	return nil
//}
//
//// Get Region Province Mapping obtiene un mapeo de regiones y provincias desde la base de datos.
//func GetRegionProvinceMapping(db *sql.DB) (map[string]string, error) {
//	query := `
//	SELECT r.REGION_NAME, p.PROVINCE_DESC, p.PROVINCE_ID
//	FROM REGION r
//	JOIN PROVINCE p ON p.REGION_ID = r.REGION_ID;`
//
//	rows, err := db.Query(query)
//	if err != nil {
//		return nil, fmt.Errorf("error al obtener mapeo de regiones y provincias: %v", err)
//	}
//	defer rows.Close()
//
//	// Mapa: clave "REGION-COMUNA" → valor "PROVINCE_ID"
//	regionMapping := make(map[string]string)
//
//	for rows.Next() {
//		var regionName, provinceName string
//		var provinceID int
//
//		if err := rows.Scan(&regionName, &provinceName, &provinceID); err != nil {
//			return nil, fmt.Errorf("error escaneando fila: %v", err)
//		}
//
//		// Crear clave compuesta "REGION-COMUNA"
//		key := fmt.Sprintf("%s-%s", regionName, provinceName)
//		regionMapping[key] = fmt.Sprintf("%d", provinceID) // Convertir provinceID a string
//	}
//
//	if err := rows.Err(); err != nil {
//		return nil, fmt.Errorf("error iterando filas: %v", err)
//	}
//
//	fmt.Println("Mapeo de regiones y provincias obtenido correctamente.")
//	return regionMapping, nil
//}
