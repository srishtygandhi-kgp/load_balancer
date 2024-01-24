package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

func homeHandler(w http.ResponseWriter, r *http.Request) {
	serverID := os.Getenv("SERVER_ID")
	message := fmt.Sprintf("Hello from Server: %s", serverID)

	response := map[string]interface{}{
		"message": message,
		"status":  "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	// Respond with an empty body and 200 OK status for heartbeat
	w.WriteHeader(http.StatusOK)
}

func main() {
	http.HandleFunc("/home", homeHandler)
	http.HandleFunc("/heartbeat", heartbeatHandler)

	port := 5000
	addr := fmt.Sprintf(":%d", port)

	fmt.Printf("Server is running on http://localhost:%d\n", port)
	http.ListenAndServe(addr, nil)
}
