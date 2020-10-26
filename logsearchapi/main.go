package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
)

func loadEnv() (*LogSearch, error) {
	pgConnStr := os.Getenv("PG_CONN_STR")
	if pgConnStr == "" {
		return nil, errors.New("PG_CONN_STR env variable is required.")
	}
	auditAuthToken := os.Getenv("AUDIT_AUTH_TOKEN")
	if auditAuthToken == "" {
		return nil, errors.New("AUDIT_AUTH_TOKEN env variable is required.")
	}
	retentionMonths, err := strconv.Atoi(os.Getenv("RETENTION_MONTHS"))
	if err != nil {
		return nil, errors.New("RETENTION_MONTHS env variable is required and must be an integer.")
	}

	return NewLogSearch(pgConnStr, auditAuthToken, retentionMonths)
}

func main() {
	ls, err := loadEnv()
	if err != nil {
		log.Fatal(err)
	}
	s := &http.Server{
		Addr:    ":8080",
		Handler: ls,
	}
	log.Fatal(s.ListenAndServe())
}
