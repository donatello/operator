package main

import (
	"crypto/subtle"
	"net/http"
)

func (ls *LogSearch) authorize(h func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.FormValue("token")
		if subtle.ConstantTimeCompare([]byte(token), []byte(ls.AuditAuthToken)) == 1 {
			h(w, r)
		} else {
			w.WriteHeader(http.StatusForbidden)
		}
	}
}
