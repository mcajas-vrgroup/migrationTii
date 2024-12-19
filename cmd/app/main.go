package main

import (
	"log"
	"migrationTii/config"
	"migrationTii/internal/data_loader"
	"migrationTii/internal/database"
)

func main() {
	// 1. Cargar configuración
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Error cargando configuración: %v", err)
	}

	// 2. Crear conexión a la base de datos
	db, err := database.CreateConnection(cfg)
	if err != nil {
		log.Fatalf("Error conectando a la DB: %v", err)
	}
	defer db.Close()

	// 3. Crear tabla temporal
	if err := database.CreateTempTable(db); err != nil {
		log.Fatalf("Error creando tabla temporal: %v", err)
	}

	// 4. Cargar y limpiar datos del CSV de asegurados
	aseguradosData, err := data_loader.CleanAndProcessData("data.csv")
	if err != nil {
		log.Fatalf("Error procesando CSV de asegurados: %v", err)
	}

	// 5. Insertar datos en la tabla temporal
	if err := database.LoadDataToTempTable(db, aseguradosData); err != nil {
		log.Fatalf("Error cargando datos en la tabla temporal: %v", err)
	}

	// 6. Insertar en PARTY
	if err := database.InsertPartyData(db); err != nil {
		log.Fatalf("Error insertando PARTY: %v", err)
	}

	// 7. Insertar en IDENTIFICATION y asociar a PARTY_IDENTIFICATION
	if err := database.InsertIdentificationData(db); err != nil {
		log.Fatalf("Error insertando IDENTIFICATION: %v", err)
	}

	// 8. Insertar en EMAIL y asociar a PARTY_EMAIL
	if err := database.InsertEmailData(db, extractEmails(aseguradosData)); err != nil {
		log.Fatalf("Error insertando EMAIL: %v", err)
	}

	// 9. Insertar en PHONE y asociar a PARTY_PHONE
	if err := database.InsertPhoneData(db, extractPhoneData(aseguradosData)); err != nil {
		log.Fatalf("Error insertando PHONE: %v", err)
	}

	// 10. Obtener mapeo de regiones y provincias
	regionMapping, err := database.GetRegionProvinceMapping(db)
	if err != nil {
		log.Fatalf("Error obteniendo mapeo de regiones: %v", err)
	}

	// 11. Insertar en ADDRESS y asociar a PARTY_ADDRESS
	if err := database.InsertAddressData(db, extractAddressData(aseguradosData), regionMapping); err != nil {
		log.Fatalf("Error insertando ADDRESS: %v", err)
	}

	// 12. Insertar en PERSON
	if err := database.InsertPersonData(db); err != nil {
		log.Fatalf("Error insertando PERSON: %v", err)
	}

	// 13. Cargar y limpiar datos del CSV de pólizas
	polizasData, err := data_loader.CleanDataPolizas("polizas.csv")
	if err != nil {
		log.Fatalf("Error procesando CSV de pólizas: %v", err)
	}

	// 14. Insertar en PAYMENT_TERM
	if err := database.InsertPaymentTerm(db, polizasData); err != nil {
		log.Fatalf("Error insertando PAYMENT_TERM: %v", err)
	}

	// 15. Insertar en CONTRACT_HEADER
	if err := database.InsertContractHeader(db, extractContractData(polizasData)); err != nil {
		log.Fatalf("Error insertando CONTRACT_HEADER: %v", err)
	}

	log.Println("Proceso completado correctamente.")
}

func extractEmails(records [][]string) []string {
	emailSet := make(map[string]struct{})
	for _, row := range records[1:] { // Saltamos encabezado
		email := row[10] // EMAIL está en la columna 10
		if email != "" {
			emailSet[email] = struct{}{}
		}
	}
	var emails []string
	for email := range emailSet {
		emails = append(emails, email)
	}
	return emails
}

func extractPhoneData(records [][]string) map[string]string {
	phoneData := make(map[string]string)
	for _, row := range records[1:] {
		email := row[10] // EMAIL
		phone := row[9]  // TELEFONO
		if email != "" && phone != "" {
			phoneData[email] = phone
		}
	}
	return phoneData
}

func extractAddressData(records [][]string) []map[string]string {
	var addressData []map[string]string
	for _, row := range records[1:] {
		address := map[string]string{
			"EMAIL":             row[10],
			"ADDRESS_STREET":    row[18],
			"ADDRESS_NUMBER":    row[19],
			"ADDRESS_APARTMENT": row[20],
			"REGION":            row[13],
			"COMUNA":            row[15],
			"CODCIUDAD":         row[16],
		}
		addressData = append(addressData, address)
	}
	return addressData
}

func extractContractData(records []map[string]string) []map[string]string {
	var contractData []map[string]string
	for _, row := range records {
		contract := map[string]string{
			"EMAIL":         row["EMAIL"],
			"CONTRACT_FROM": row["CONTRACT_FROM"],
			"CONTRACT_TO":   row["CONTRACT_TO"],
		}
		contractData = append(contractData, contract)
	}
	return contractData
}
