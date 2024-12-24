package main

import (
	"fmt"
	"log"
	"migrationTii/config"
	"migrationTii/internal/data_loader"
	"migrationTii/internal/database"
	"migrationTii/pkg/report"
	"os"
	"time"
)

func main() {
	// Crear reporte
	r := report.NewReport("pkg/report/reporte/execution_report.log")

	// 1. Cargar configuración
	cfg, err := config.LoadConfig()
	if err != nil {
		r.MarkFailure(err)
		r.Save()
		log.Fatalf("Error cargando configuración: %v", err)
	}
	r.Add("Configuración cargada correctamente.")

	// 2. Crear conexión a la base de datos
	db, err := database.CreateConnection(cfg)
	if err != nil {
		r.MarkFailure(err)
		r.Save()
		log.Fatalf("Error conectando a la DB: %v", err)
	}
	defer db.Close()
	r.Add("Conexión a la base de datos establecida.")

	// 3. Iniciar transacción
	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		r.MarkFailure(err)
		r.Save()
		log.Fatalf("Error iniciando transacción: %v", err)
	}
	r.Add("Transacción iniciada.")

	defer func() {
		if err != nil {
			r.MarkFailure(err)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Fatalf("Error al hacer rollback: %v", rollbackErr)
			}
			r.Add("Rollback realizado exitosamente.")
			r.Save()
			log.Fatalf("Error detectado: %v", err)
		}
	}()

	// 4. Crear tablas temporales
	blockStart := time.Now()

	// Verificar existencia de archivos
	if !fileExists("pkg/utils/data/MIGSA_ASEGURADOS_CSV.csv") {
		log.Fatalf("Archivo no encontrado: MIGSA_ASEGURADOS_CSV.csv")
	} else {
		log.Println("Archivo encontrado: MIGSA_ASEGURADOS_CSV.csv")
	}

	if !fileExists("pkg/utils/data/MIGSA_POLIZAS-18-12_CSV.csv") {
		log.Fatalf("Archivo no encontrado: MIGSA_POLIZAS-18-12_CSV.csv")
	} else {
		log.Println("Archivo encontrado: MIGSA_POLIZAS-18-12_CSV.csv")
	}

	if err := database.CreateTempTable(tx); err != nil {
		return
	}
	r.Add(fmt.Sprintf("Tabla temporal creada en %v.", time.Since(blockStart)))

	blockStart = time.Now()
	if err := database.CreateCleanedTempTable(tx); err != nil {
		return
	}
	r.Add(fmt.Sprintf("Tabla temporal limpia creada en %v.", time.Since(blockStart)))

	// 5. Procesar y cargar datos base
	log.Println("Cargando y procesando datos base...")
	blockStart = time.Now()

	aseguradosData, err := data_loader.CleanAndProcessData("pkg/utils/data/MIGSA_ASEGURADOS_CSV.csv")
	if err != nil {
		log.Fatalf("Error procesando CSV de asegurados: %v", err)
	}

	if err := database.LoadAseguradosData(tx, aseguradosData); err != nil {
		log.Fatalf("Error insertando datos de asegurados en temp_csv_data: %v", err)
	}

	r.Add(fmt.Sprintf("Datos de asegurados cargados en %v.", time.Since(blockStart)))

	blockStart = time.Now()
	polizasData, err := data_loader.CleanDataPolizas("pkg/utils/data/MIGSA_POLIZAS-18-12_CSV.csv")
	if err != nil {
		return
	}
	if err := database.LoadPolizasData(tx, polizasData); err != nil {
		return
	}
	r.Add(fmt.Sprintf("Datos de pólizas cargados en %v.", time.Since(blockStart)))

	// 6. Procesar datos de asegurados
	log.Println("Procesando datos de asegurados...")
	blockStart = time.Now()
	if err := database.InsertPartyData(tx); err != nil {
		log.Fatalf("Error insertando PARTY: %v", err)
	}
	if err := database.CreateTempCleanedRUT(tx); err != nil {
		return
	}
	if err := database.InsertIdentification(tx); err != nil {
		log.Fatalf("Error insertando IDENTIFICATION: %v", err)
	}
	if err := database.AssociatePartyIdentification(tx); err != nil {
		log.Fatalf("Error insertando PARTY_IDENTIFICATION: %v", err)
	}
	log.Println("Iniciando inserción en EMAIL...")
	if err := database.InsertEmail(tx); err != nil {
		log.Fatalf("Error insertando EMAIL: %v", err)
	}
	log.Println("EMAIL insertado correctamente.")
	if err := database.InsertPhone(tx); err != nil {
		log.Fatalf("Error insertando PHONE: %v", err)
	}
	if err := database.InsertAddress(tx); err != nil {
		log.Fatalf("Error insertando ADDRESS: %v", err)
	}
	if err := database.AssociatePartyAddress(tx); err != nil {
		log.Fatalf("Error insertando PARTY_ADDRESS: %v", err)
	}
	if err := database.InsertPersonData(tx); err != nil {
		log.Fatalf("Error insertando PERSON: %v", err)
	}

	if err := database.InsertPaymentTerm(tx); err != nil {
		log.Fatalf("Error insertando PAYMENT_TERM: %v", err)
	}

	r.Add(fmt.Sprintf("Datos de asegurados procesados en %v.", time.Since(blockStart)))

	// 7. Procesar datos de pólizas y contratos
	log.Println("Procesando datos de pólizas y contratos...")
	blockStart = time.Now()
	if err := database.CreateTempIssuanceDates(tx); err != nil {
		log.Fatalf("Error creando tabla temporal: %v", err)
	}
	if err := database.InsertContractHeader(tx); err != nil {
		log.Fatalf("Error insertando en CONTRACT_HEADER: %v", err)
	}
	r.Add(fmt.Sprintf("Datos de pólizas y contratos procesados en %v.", time.Since(blockStart)))

	// 8. Procesar datos relacionados con REQUEST
	log.Println("Procesando datos de REQUEST...")
	blockStart = time.Now()
	if err := database.InsertRequest(tx); err != nil {
		log.Fatalf("Error insertando REQUEST: %v", err)
	}
	if err := database.InsertRequestCoverageValue(tx); err != nil {
		log.Fatalf("Error insertando REQUEST_COVERAGE_VALUE: %v", err)
	}
	if err := database.InsertRequestEconomics(tx); err != nil {
		log.Fatalf("Error insertando REQUEST_ECONOMICS: %v", err)
	}
	if err := database.InsertRequestParameter(tx); err != nil {
		log.Fatalf("Error insertando REQUEST_PARAMETER: %v", err)
	}
	r.Add(fmt.Sprintf("Datos de REQUEST procesados en %v.", time.Since(blockStart)))

	// 9. Procesar datos de pólizas finales
	log.Println("Procesando datos de POLICY...")
	blockStart = time.Now()
	if err := database.CreateTempOriginalPolicyTable(tx); err != nil {
		log.Fatalf("Error creando tabla temporal: %v", err)
	}
	if err := database.InsertIntoPolicy(tx); err != nil {
		log.Fatalf("Error insertando en POLICY: %v", err)
	}
	if err := database.InsertPolicyCoverageValue(tx); err != nil {
		log.Fatalf("Error insertando en POLICY_COVERAGE_VALUE: %v", err)
	}
	if err := database.InsertPolicyParameter(tx); err != nil {
		log.Fatalf("Error insertando en POLICY_PARAMETER: %v", err)
	}
	if err := database.InsertPolicyEconomics(tx); err != nil {
		log.Fatalf("Error insertando en POLICY_ECONOMICS: %v", err)
	}
	r.Add(fmt.Sprintf("Datos de POLICY procesados en %v.", time.Since(blockStart)))

	// 10. Insertar en BILLING_STATEMENT
	log.Println("Procesando datos de BILLING_STATEMENT...")
	blockStart = time.Now()
	if err := database.InsertBillingStatement(tx); err != nil {
		log.Fatalf("Error insertando en BILLING_STATEMENT: %v", err)
	}
	r.Add(fmt.Sprintf("Datos de BILLING_STATEMENT procesados en %v.", time.Since(blockStart)))

	// 11. Confirmar transacción
	if err := tx.Commit(); err != nil {
		r.MarkFailure(err)
		r.Save()
		log.Fatalf("Error al confirmar la transacción: %v", err)
	}
	r.MarkSuccess()
	r.Add(fmt.Sprintf("Transacción completada exitosamente en %v.", time.Since(start)))

	// Guardar reporte final
	if err := r.Save(); err != nil {
		log.Fatalf("Error guardando el reporte: %v", err)
	}

	log.Println("Proceso completado correctamente.")
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}
