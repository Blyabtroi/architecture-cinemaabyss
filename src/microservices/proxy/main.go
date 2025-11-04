package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	monolithURL      *url.URL
	moviesServiceURL *url.URL
	eventsServiceURL *url.URL

	gradualMigration bool
	moviesPercent    int
)

func init() {
	var err error

	monolithRaw := os.Getenv("MONOLITH_URL")
	if monolithRaw == "" {
		log.Fatal("MONOLITH_URL environment variable is required")
	}
	monolithURL, err = url.Parse(monolithRaw)
	if err != nil {
		log.Fatalf("Invalid MONOLITH_URL: %v", err)
	}

	moviesRaw := os.Getenv("MOVIES_SERVICE_URL")
	if moviesRaw == "" {
		log.Fatal("MOVIES_SERVICE_URL environment variable is required")
	}
	moviesServiceURL, err = url.Parse(moviesRaw)
	if err != nil {
		log.Fatalf("Invalid MOVIES_SERVICE_URL: %v", err)
	}

	eventsRaw := os.Getenv("EVENTS_SERVICE_URL")
	if eventsRaw == "" {
		log.Fatal("EVENTS_SERVICE_URL environment variable is required")
	}
	eventsServiceURL, err = url.Parse(eventsRaw)
	if err != nil {
		log.Fatalf("Invalid EVENTS_SERVICE_URL: %v", err)
	}

	gradualMigration = strings.ToLower(os.Getenv("GRADUAL_MIGRATION")) == "true"

	p := os.Getenv("MOVIES_MIGRATION_PERCENT")
	if p == "" {
		moviesPercent = 0
	} else {
		moviesPercent, err = strconv.Atoi(p)
		if err != nil || moviesPercent < 0 || moviesPercent > 100 {
			log.Fatalf("Invalid MOVIES_MIGRATION_PERCENT: must be 0..100")
		}
	}

	rand.Seed(time.Now().UnixNano())
}

func main() {
	http.HandleFunc("/", handleProxy)
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting Proxy Service on :%s ...", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleProxy(w http.ResponseWriter, r *http.Request) {
	log.Printf("gradualMigration:%t ...", gradualMigration)
	log.Printf("moviesPercent:%d ...", moviesPercent)

	// 1. If request path starts with /api/movies or /movies => route either to movies-service or monolith depending on percentage and flag
	if strings.HasPrefix(r.URL.Path, "/api/movies") || strings.HasPrefix(r.URL.Path, "/movies") {

		if gradualMigration {
			if rand.Intn(100) < moviesPercent {

				log.Printf("Request path :%s ...", moviesServiceURL)

				proxyRequest(moviesServiceURL, w, r)
				return
			}
		} else {
			log.Printf("Request to monolith 1 :%s ...", monolithURL)

			proxyRequest(monolithURL, w, r)
			return
		}
		log.Printf("Request to monolith 2 :%s ...", monolithURL)

		proxyRequest(monolithURL, w, r)
		return
	}

	// 2. If request path starts with /api/events or /events => route to events service
	if strings.HasPrefix(r.URL.Path, "/api/events") || strings.HasPrefix(r.URL.Path, "/events") {

		log.Printf("Request path :%s ...", eventsServiceURL)

		proxyRequest(eventsServiceURL, w, r)
		return
	}

	log.Printf("Request to monolith 3 :%s ...", monolithURL)

	// 3. Otherwise default route to monolith
	proxyRequest(monolithURL, w, r)
}

func proxyRequest(target *url.URL, w http.ResponseWriter, r *http.Request) {
	proxy := httputil.NewSingleHostReverseProxy(target)

	log.Printf("Incoming request: %s %s routed to: %s", r.Method, r.URL.Path, target.String())

	proxy.ServeHTTP(w, r)
}
