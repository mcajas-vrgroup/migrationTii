package database

import (
	"database/sql"
	"fmt"
	"migrationTii/config"

	_ "github.com/go-sql-driver/mysql"
)

func CreateConnection(cfg *config.Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error al conectar a la base de datos: %v", err)
	}

	return db, nil
}
