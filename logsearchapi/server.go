package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httputil"
	"time"
)

type LogSearch struct {
	// Configuration
	PGConnStr       string
	AuditAuthToken  string
	RetentionMonths int

	// Runtime
	DBClient *DBClient
	*http.ServeMux
}

func NewLogSearch(pgConnStr, auditAuthToken string, retentionMonths int) (ls *LogSearch, err error) {
	ls = &LogSearch{
		PGConnStr:       pgConnStr,
		AuditAuthToken:  auditAuthToken,
		RetentionMonths: retentionMonths,
	}

	// Initialize DB Client
	ls.DBClient, err = NewDBClient(context.Background(), ls.PGConnStr)
	if err != nil {
		return nil, fmt.Errorf("Error connecting to db: %v", err)
	}

	// Initialize tables in db
	err = ls.DBClient.InitDBTables(context.Background())
	if err != nil {
		return nil, fmt.Errorf("Error initializing tables: %v", err)
	}

	// Initialize muxer
	ls.ServeMux = http.NewServeMux()
	ls.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		return
	})
	ls.HandleFunc("/api/ingest", ls.authorize(ls.ingestHandler))
	ls.HandleFunc("/api/query", ls.authorize(ls.queryHandler))

	return ls, nil
}

func (ls *LogSearch) writeErrorResponse(w http.ResponseWriter, status int, msg string, err error) {
	w.WriteHeader(status)
	w.Write([]byte(fmt.Sprintf("%s: %v", msg, err)))
	log.Printf("%s: %v (%d)", msg, err, status)
}

func (ls *LogSearch) ingestHandler(w http.ResponseWriter, r *http.Request) {
	err := printReq(r)
	if err != nil {
		log.Print(err)
	}

	if r.Method != "POST" {
		ls.writeErrorResponse(w, 400, "Non post request", nil)
		return
	}

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		ls.writeErrorResponse(w, 500, "Error reading request body", err)
		return
	}

	var v map[string]interface{} = make(map[string]interface{})
	err = json.Unmarshal(buf, &v)
	if err != nil {
		ls.writeErrorResponse(w, 500, "Error parsing request body", err)
		return
	}

	if len(v) == 0 {
		// Empty test request sent by minio
		return
	}

	var ts time.Time
	if timeI, ok := v["time"]; ok {
		if timeStr, ok := timeI.(string); !ok {
			ls.writeErrorResponse(w, 400, "Unexpected non-string time parameter", err)
			return
		} else if timestamp, err := time.Parse(time.RFC3339Nano, timeStr); err != nil {
			ls.writeErrorResponse(w, 400, "Bad time parameter format in json request body", err)
			return
		} else {
			ts = timestamp
		}
	} else {
		ls.writeErrorResponse(w, 400, "Missing time parameter in json request body", err)
		return
	}

	err = ls.DBClient.InsertEvent(context.Background(), ts, string(buf))
	if err != nil {
		ls.writeErrorResponse(w, 500, "Error writing to DB", err)
	}
}

func (ls *LogSearch) queryHandler(w http.ResponseWriter, r *http.Request) {
}

// debug helper
func printReq(r *http.Request) error {
	b, err := httputil.DumpRequest(r, false)
	if err != nil {
		return err
	} else {
		fmt.Println(string(b))
	}

	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	var out bytes.Buffer
	json.Indent(&out, buf, "", "  ")
	fmt.Println(out.String())

	newBuf := bytes.NewBuffer(buf)
	r.Body = ioutil.NopCloser(newBuf)
	return nil
}
