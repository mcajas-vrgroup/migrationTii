package data_loader

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
)

func GroupByPartyID(records []map[string]string) map[string][]map[string]string {
	grouped := make(map[string][]map[string]string)
	for _, record := range records {
		partyID := record["RUT"] // Usar "RUT" o el campo que identifica al PARTY_ID
		grouped[partyID] = append(grouped[partyID], record)
	}
	return grouped
}

// Limpia el RUT
func CleanRUT(rut string) string {
	rut = strings.ToUpper(strings.TrimSpace(rut))
	re := regexp.MustCompile(`[^0-9Kk]`)
	rut = re.ReplaceAllString(rut, "")
	return strings.TrimLeft(rut, "0")
}

// Limpia el teléfono
func CleanPhone(phone string) string {
	re := regexp.MustCompile(`\D`)
	phone = re.ReplaceAllString(phone, "")
	if len(phone) > 8 {
		return phone[len(phone)-8:]
	}
	return phone
}

// Divide la dirección
func SplitAddress(address string) (string, string, string) {
	if address == "" {
		return "", "", ""
	}

	address = strings.TrimSpace(address)
	numberRe := regexp.MustCompile(`(\d+)`)
	apartmentRe := regexp.MustCompile(`(?i)(depto\.?\d*|piso\s?\d*)`)

	number := numberRe.FindString(address)
	apartment := apartmentRe.FindString(address)
	street := numberRe.ReplaceAllString(address, "")
	street = apartmentRe.ReplaceAllString(street, "")

	return strings.TrimSpace(street), number, apartment
}

// Carga y procesa datos desde CSV
func CleanAndProcessData(csvPath string) ([]map[string]string, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("error al abrir el archivo CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';' // Delimitador de columna

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error al leer el CSV: %v", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("el archivo CSV está vacío")
	}

	headers := append(records[0], "ADDRESS_STREET", "ADDRESS_NUMBER", "ADDRESS_APARTMENT")
	data := make([]map[string]string, 0)

	for _, row := range records[1:] {
		entry := make(map[string]string)
		for i, value := range row {
			entry[headers[i]] = strings.TrimSpace(value)
		}

		// Procesar RUT y teléfono
		entry["RUT"] = CleanRUT(entry["RUT"])
		entry["TELEFONO"] = CleanPhone(entry["TELEFONO"])

		// Dividir dirección
		street, number, apartment := SplitAddress(entry["DIRECCION"])
		entry["ADDRESS_STREET"] = street
		entry["ADDRESS_NUMBER"] = number
		entry["ADDRESS_APARTMENT"] = apartment

		data = append(data, entry)
	}
	return data, nil
}

// cleanDataPolizas procesa el CSV de pólizas y devuelve un slice de mapas.
func CleanDataPolizas(csvPath string) ([]map[string]string, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("error al abrir el archivo CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';' // Delimitador

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error al leer el CSV: %v", err)
	}

	// Encabezados
	headers := records[0]
	data := make([]map[string]string, 0)

	// Mapear valores para PAYMENT_TYPE_ID y BANK_ID
	paymentTypeMap := map[string]string{
		"CARGO A CUENTA":  "2000",
		"CARGO A TARJETA": "3000",
		"COBRO DIRECTO":   "1000",
	}
	bankMap := map[string]string{
		"ITAU": "9", "CREDITO": "5", "SANTANDER": "8",
		"TRANSBANK": "4", "CHILE": "1",
	}

	// Iterar sobre los registros
	for _, row := range records[1:] {
		entry := make(map[string]string)
		for i, value := range row {
			entry[headers[i]] = strings.TrimSpace(value)
		}

		// Asignar PAYMENT_TYPE_ID
		if val, exists := paymentTypeMap[entry["DESCTPCONDCOBRO"]]; exists {
			entry["PAYMENT_TYPE_ID"] = val
		}

		// Asignar BANK_ID
		for key, id := range bankMap {
			if strings.Contains(entry["DESCCONDCOBRO"], key) {
				entry["BANK_ID"] = id
				break
			}
		}

		// Asignar EXPIRATION
		if entry["DESCTPCONDCOBRO"] == "CARGO A TARJETA" {
			entry["EXPIRATION"] = "2028-10-01 00:00:00"
		} else {
			entry["EXPIRATION"] = ""
		}

		data = append(data, entry)
	}
	return data, nil
}

func AddToSqlScript(sqlQuery string) string {
	queryFile, err := os.OpenFile("query-tii-vulcano.sql", os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Error al abrir archivo query: %v", err)
	}
	defer queryFile.Close()
	content := []byte(sqlQuery)
	if _, err := queryFile.Write(content); err != nil {
		log.Fatalf("Error escribir query: %v", err)
	}
	return ""
}
