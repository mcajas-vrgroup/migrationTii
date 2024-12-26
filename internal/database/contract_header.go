package database

import (
	"database/sql"
	"fmt"
	"log"
)

// Create TempIssuanceDates crea una tabla temporal con las fechas de emisión.
func CreateTempIssuanceDates(db *sql.Tx) error {
	query := `
	CREATE TEMPORARY TABLE temp_issuance_dates AS
	SELECT
		NPOLORI,
		MIN(FINIVIG) AS CONTRACT_ISSUANCE_DATE
	FROM temp_polizas_data
	WHERE CODESTADO = '03' -- Solo "EN VIGOR"
	  AND NPOLIZA LIKE '%00' -- Identificar NPOLIZA raíz
	GROUP BY NPOLORI;
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error creando temp_issuance_dates: %v", err)
	}

	fmt.Println("Tabla temporal temp_issuance_dates creada correctamente.")
	log.Println(query)
	return nil
}

// Insert ContractHeader inserta datos en CONTRACT_HEADER usando la tabla temporal temp_issuance_dates.
func InsertContractHeader(db *sql.Tx) error {
	query := `
	INSERT INTO CONTRACT_HEADER (
		AGENCY_ID,
		INSURER_ID,
		PAYMENT_PLAN_ID,
		PAYMENT_TERM_ID,
		INSURED_PARTY_ID,
		HOLDER_PARTY_ID,
		SECTION_ID,
		SUB_SECTION,
		COVERAGE_PLAN_ID,
		CONTRACT_FROM,
		CONTRACT_TO,
		CURRENCY_ID,
		CONTRACT_ISSUANCE_DATE
	)
	SELECT
		1 AS AGENCY_ID,                      -- Agencia por defecto
		1020 AS INSURER_ID,                  -- Aseguradora fija
		CASE t.IDPERIODPAGO
			WHEN '004' THEN 1000  -- MENSUAL
			WHEN '003' THEN 2000  -- TRIMESTRAL
			WHEN '002' THEN 3000  -- SEMESTRAL
			WHEN '001' THEN 4000  -- ANUAL
			ELSE 5000             -- PRIMA UNICA por defecto
		END AS PAYMENT_PLAN_ID,
		pt.PAYMENT_TERM_ID,                  -- PAYMENT_TERM_ID asociado
		p.PARTY_ID AS INSURED_PARTY_ID,      -- Asegurado
		p.PARTY_ID AS HOLDER_PARTY_ID,       -- Titular (el mismo PARTY_ID)
		101 AS SECTION_ID,                   -- Sección por defecto
		3000 AS SUB_SECTION,                 -- Sub-sección fija
		7 AS COVERAGE_PLAN_ID,               -- Plan de cobertura fijo
		t.FINIVIG AS CONTRACT_FROM,          -- Fecha inicio de vigencia
		t.FTERVIG AS CONTRACT_TO,            -- Fecha fin de vigencia
		4000 AS CURRENCY_ID,                 -- Moneda CLP
		i.CONTRACT_ISSUANCE_DATE             -- Fecha de emisión (primera FINIVIG)
	FROM temp_polizas_data t
	JOIN PAYMENT_TERM pt ON pt.ACCOUNT_NBR = t.NROCONDCOBRO
	JOIN PARTY p ON p.PARTY_ID = pt.PARTY_ID
	LEFT JOIN temp_issuance_dates i ON i.NPOLORI = t.NPOLIZA
	WHERE t.CODESTADO = '03'; -- Solo contratos "EN VIGOR"
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("error insertando en CONTRACT_HEADER: %v", err)
	}

	fmt.Println("Datos insertados correctamente en CONTRACT_HEADER.")
	log.Println(query)
	return nil
}
