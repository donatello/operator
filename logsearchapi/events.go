package main

import (
	"time"

	"github.com/minio/minio/cmd/logger/message/audit"
)

// Entry is an alias for the type from the minio code.
type Entry audit.Entry

type API struct {
	Name            string `json:"name,omitempty"`
	Bucket          string `json:"bucket,omitempty"`
	Object          string `json:"object,omitempty"`
	Status          string `json:"status,omitempty"`
	StatusCode      int    `json:"statusCode,omitempty"`
	TimeToFirstByte uint64 `json:"timeToFirstByte,omitempty"`
	TimeToResponse  uint64 `json:"timeToResponse,omitempty"`
}

// Event is the same as Entry but with more typed values.
type Event struct {
	Version      string                 `json:"version"`
	DeploymentID string                 `json:"deploymentid,omitempty"`
	Time         time.Time              `json:"time"`
	API          API                    `json:"api"`
	RemoteHost   string                 `json:"remotehost,omitempty"`
	RequestID    string                 `json:"requestID,omitempty"`
	UserAgent    string                 `json:"userAgent,omitempty"`
	ReqClaims    map[string]interface{} `json:"requestClaims,omitempty"`
	ReqQuery     map[string]string      `json:"requestQuery,omitempty"`
	ReqHeader    map[string]string      `json:"requestHeader,omitempty"`
	RespHeader   map[string]string      `json:"responseHeader,omitempty"`
}

// EventFromEntry performs a type conversion
func EventFromEntry(e Entry) (Event, error) {
	ret := Event{
		Version:      e.Version,
		DeploymentID: e.DeploymentID,
		API: API{
			Name:       e.API.Name,
			Bucket:     e.API.Bucket,
			Object:     e.API.Object,
			Status:     e.API.Status,
			StatusCode: e.API.StatusCode,
		},
		RemoteHost: e.RemoteHost,
		RequestID:  e.RequestID,
		UserAgent:  e.UserAgent,
		ReqClaims:  e.ReqClaims,
		ReqQuery:   e.ReqQuery,
		ReqHeader:  e.ReqHeader,
		RespHeader: e.RespHeader,
	}
	// TODO
	return ret, nil
}
