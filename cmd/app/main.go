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

	// 3. Crear tablas temporales para datos base
	if err := database.CreateTempTable(db); err != nil {
		log.Fatalf("Error creando tabla temporal: %v", err)
	}

	// Crear tabla temporal para limpiar registros únicos
	if err := database.CreateCleanedTempTable(db); err != nil {
		log.Fatalf("Error creando tabla limpia temporal: %v", err)
	}

	// 4. Procesar y cargar datos base
	log.Println("Cargando y procesando datos de asegurados...")
	aseguradosData, err := data_loader.CleanAndProcessData("pkg/utils/data/MIGSA_ASEGURADOS_CSV.csv")
	if err != nil {
		log.Fatalf("Error procesando CSV de asegurados: %v", err)
	}
	if err := database.LoadAseguradosData(db, aseguradosData); err != nil {
		log.Fatalf("Error insertando datos en temp_csv_data: %v", err)
	}

	log.Println("Cargando y procesando datos de pólizas...")
	polizasData, err := data_loader.CleanDataPolizas("pkg/utils/data/MIGSA_POLIZAS-18-12_CSV.csv")
	if err != nil {
		log.Fatalf("Error procesando CSV de pólizas: %v", err)
	}
	if err := database.LoadPolizasData(db, polizasData); err != nil {
		log.Fatalf("Error insertando datos en temp_polizas_data: %v", err)
	}

	// 5. Procesar datos de asegurados
	log.Println("Procesando datos de asegurados...")
	if err := database.InsertPartyData(db); err != nil {
		log.Fatalf("Error insertando PARTY: %v", err)
	}
	if err := database.CreateTempCleanedRUT(db); err != nil {
		log.Fatalf("Error creando tabla temp_cleaned_rut: %v", err)
	}
	if err := database.InsertIdentification(db); err != nil {
		log.Fatalf("Error insertando en IDENTIFICATION: %v", err)
	}
	if err := database.AssociatePartyIdentification(db); err != nil {
		log.Fatalf("Error asociando PARTY_IDENTIFICATION: %v", err)
	}
	if err := database.InsertEmail(db); err != nil {
		log.Fatalf("Error insertando EMAIL y asociando PARTY_EMAIL: %v", err)
	}
	if err := database.InsertPhone(db); err != nil {
		log.Fatalf("Error insertando PHONE y asociando PARTY_PHONE: %v", err)
	}
	if err := database.InsertAddress(db); err != nil {
		log.Fatalf("Error insertando ADDRESS y asociando PARTY_ADDRESS: %v", err)
	}
	if err := database.InsertPersonData(db); err != nil {
		log.Fatalf("Error insertando PERSON: %v", err)
	}

	// 6. Procesar datos de pólizas y contratos
	log.Println("Procesando datos de pólizas y contratos...")
	if err := database.CreateTempIssuanceDates(db); err != nil {
		log.Fatalf("Error creando temp_issuance_dates: %v", err)
	}
	if err := database.InsertContractHeader(db); err != nil {
		log.Fatalf("Error insertando CONTRACT_HEADER: %v", err)
	}

	// 7. Procesar datos relacionados con REQUEST
	log.Println("Procesando datos de REQUEST...")
	if err := database.InsertRequest(db); err != nil {
		log.Fatalf("Error insertando en REQUEST: %v", err)
	}
	if err := database.InsertRequestCoverageValue(db); err != nil {
		log.Fatalf("Error inserting REQUEST_COVERAGE_VALUE: %v", err)
	}
	if err := database.InsertRequestEconomics(db); err != nil {
		log.Fatalf("Error insertando en REQUEST_ECONOMICS: %v", err)
	}
	if err := database.InsertRequestParameter(db); err != nil {
		log.Fatalf("Error insertando en REQUEST_PARAMETER: %v", err)
	}

	// 8. Procesar datos de pólizas finales
	log.Println("Procesando datos de POLICY...")
	if err := database.CreateTempOriginalPolicyTable(db); err != nil {
		log.Fatalf("Error creando tabla temporal temp_original_policy: %v", err)
	}
	if err := database.InsertIntoPolicy(db); err != nil {
		log.Fatalf("Error insertando datos en POLICY: %v", err)
	}

	if err := database.InsertPolicyCoverageValue(db); err != nil {
		log.Fatalf("Error insertando en POLICY_COVERAGE_VALUE: %v", err)
	}

	if err := database.InsertPolicyParameter(db); err != nil {
		log.Fatalf("Error insertando en POLICY_PARAMETER: %v", err)
	}

	log.Println("Procesando datos de POLICY_ECONOMICS...")
	if err := database.InsertPolicyEconomics(db); err != nil {
		log.Fatalf("Error insertando datos en POLICY_ECONOMICS: %v", err)
	}

	// 9. Insertar en BILLING_STATEMENT
	log.Println("Procesando datos de BILLING_STATEMENT...")
	if err := database.InsertBillingStatement(db); err != nil {
		log.Fatalf("Error insertando datos en BILLING_STATEMENT: %v", err)
	}

	log.Println("Proceso completado correctamente.")
}
