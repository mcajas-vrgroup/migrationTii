package report

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Report estructura para mantener detalles del reporte
type Report struct {
	entries    []string
	startTime  time.Time
	successful bool
}

// NewReport crea un nuevo reporte y registra el tiempo de inicio
func NewReport(filename string) *Report {
	return &Report{
		entries:    []string{},
		startTime:  time.Now(),
		successful: true,
	}
}

// Add añade un mensaje al reporte junto con un timestamp
func (r *Report) Add(message string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	r.entries = append(r.entries, fmt.Sprintf("[%s] %s", timestamp, message))
}

// MarkFailure registra que ocurrió un error y detiene el éxito del reporte
func (r *Report) MarkFailure(err error) {
	r.successful = false
	r.Add(fmt.Sprintf("ERROR: %v", err))
}

// MarkSuccess asegura que el reporte se considere exitoso
func (r *Report) MarkSuccess() {
	r.successful = true
	r.Add("Proceso completado exitosamente.")
}

// Save guarda el reporte en un archivo y calcula el tiempo total de ejecución
func (r *Report) Save() error {
	duration := time.Since(r.startTime)
	r.entries = append(r.entries, fmt.Sprintf("Tiempo total de ejecución: %v", duration))

	status := "ÉXITO"
	if !r.successful {
		status = "FALLO"
	}
	r.entries = append(r.entries, fmt.Sprintf("Estado final: %s", status))

	reportContent := strings.Join(r.entries, "\n")
	return os.WriteFile("execution_report.log", []byte(reportContent), 0644)
}
