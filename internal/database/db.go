package database

import (
	"database/sql"
	"fmt"
	"migrationTii/config"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func CreateConnection(cfg *config.Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("error al conectar a la base de datos: %v", err)
	}

	// Validar la conexión
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("error al validar la conexión a la base de datos: %v", err)
	}

	// Configurar límites del pool
	db.SetMaxOpenConns(25)                 // Número máximo de conexiones abiertas
	db.SetMaxIdleConns(25)                 // Conexiones inactivas
	db.SetConnMaxLifetime(5 * time.Minute) // Tiempo de vida máximo para una conexión

	return db, nil
}
