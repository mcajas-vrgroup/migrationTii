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

	// Crear tabla temporal para limpiar registros únicos
	if err := database.CreateCleanedTempTable(db); err != nil {
		log.Fatalf("Error creando tabla limpia temporal: %v", err)
	}

	// 4. Cargar y limpiar datos de asegurados
	log.Println("Cargando y procesando datos de asegurados...")
	aseguradosData, err := data_loader.CleanAndProcessData("data/asegurados.csv")
	if err != nil {
		log.Fatalf("Error procesando CSV de asegurados: %v", err)
	}

	// 5. Insertar en tabla temporal asegurados
	if err := database.LoadAseguradosData(db, aseguradosData); err != nil {
		log.Fatalf("Error insertando datos en temp_csv_data: %v", err)
	}

	// 6. Cargar y limpiar datos de pólizas
	log.Println("Cargando y procesando datos de pólizas...")
	polizasData, err := data_loader.CleanDataPolizas("data/polizas.csv")
	if err != nil {
		log.Fatalf("Error procesando CSV de pólizas: %v", err)
	}

	// 7. Insertar en tabla temporal pólizas
	if err := database.LoadPolizasData(db, polizasData); err != nil {
		log.Fatalf("Error insertando datos en temp_polizas_data: %v", err)
	}

	// 8. Insertar en PARTY
	if err := database.InsertPartyData(db); err != nil {
		log.Fatalf("Error insertando PARTY: %v", err)
	}

	// 9. Crear tabla temporal de RUT limpios
	if err := database.CreateTempCleanedRUT(db); err != nil {
		log.Fatalf("Error creando tabla temp_cleaned_rut: %v", err)
	}

	// 10. Insertar en IDENTIFICATION
	if err := database.InsertIdentification(db); err != nil {
		log.Fatalf("Error insertando en IDENTIFICATION: %v", err)
	}

	// 11. Asociar PARTY con IDENTIFICATION
	if err := database.AssociatePartyIdentification(db); err != nil {
		log.Fatalf("Error asociando PARTY_IDENTIFICATION: %v", err)
	}

	// 12. Insertar en EMAIL y asociar a PARTY_EMAIL
	if err := database.InsertEmail(db); err != nil {
		log.Fatalf("Error insertando EMAIL y asociando PARTY_EMAIL: %v", err)
	}

	// 13. Insertar en PHONE y asociar a PARTY_PHONE
	if err := database.InsertPhone(db); err != nil {
		log.Fatalf("Error insertando PHONE y asociando PARTY_PHONE: %v", err)
	}

	// 14. Insertar en ADDRESS y asociar a PARTY_ADDRESS
	if err := database.InsertAddress(db); err != nil {
		log.Fatalf("Error insertando ADDRESS: %v", err)
	}
	if err := database.AssociatePartyAddress(db); err != nil {
		log.Fatalf("Error asociando PARTY_ADDRESS: %v", err)
	}

	// 15. Insertar en PERSON
	if err := database.InsertPersonData(db); err != nil {
		log.Fatalf("Error insertando PERSON: %v", err)
	}

	// 16. Insertar en PAYMENT_TERM
	if err := database.InsertPaymentTerm(db); err != nil {
		log.Fatalf("Error insertando PAYMENT_TERM: %v", err)
	}

	// 17. Insertar en CONTRACT_HEADER
	if err := database.InsertContractHeader(db, extractContractData(polizasData)); err != nil {
		log.Fatalf("Error insertando CONTRACT_HEADER: %v", err)
	}

	log.Println("Proceso completado correctamente.")
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
