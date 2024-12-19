package database

import (
	"database/sql"
	"fmt"
)

func InsertContractHeader(db *sql.DB, contractData []map[string]string) error {
	query := `
	INSERT INTO CONTRACT_HEADER (AGENCY_ID, INSURER_ID, PAYMENT_PLAN_ID, PAYMENT_TERM_ID, 
								 INSURED_PARTY_ID, HOLDER_PARTY_ID, SECTION_ID, SUB_SECTION, 
								 COVERAGE_PLAN_ID, CONTRACT_FROM, CONTRACT_TO, CURRENCY_ID, CONTRACT_ISSUANCE_DATE)
	SELECT 1, 1020, 1000, pt.PAYMENT_TERM_ID, p.PARTY_ID, p.PARTY_ID, 
		   101, 3000, 7, ?, ?, 3000, NOW()
	FROM PARTY p
	JOIN PAYMENT_TERM pt ON pt.PARTY_ID = p.PARTY_ID
	WHERE p.EMAIL = ?;`

	for _, row := range contractData {
		_, err := db.Exec(query, row["CONTRACT_FROM"], row["CONTRACT_TO"], row["EMAIL"])
		if err != nil {
			return fmt.Errorf("error insertando CONTRACT_HEADER: %v", err)
		}
	}
	fmt.Println("Datos insertados en CONTRACT_HEADER.")
	return nil
}
