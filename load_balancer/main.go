package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-co-op/gocron"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"
)

func contains(servers []string, hostname string) bool {
	for _, server := range servers {
		if server == hostname {
			return true
		}
	}
	return false
}

// Constants and data structures for consistent hashing
// (reuse the constants and functions from Task 2)

// Constants for hashing parameters
const (
	N        = 3   // Number of server containers
	M        = 512 // Total number of slots in the consistent hash map
	K        = 9   // Number of virtual servers for each server container
	HASH_MOD = 512 // A large prime number to ensure non-negative hash results
	EMPTY    = -1  // Value to represent an empty slot
)

// Entry represents an entry in the consistent hash map
type Entry struct {
	IsEmpty   bool
	IsServer  bool
	ServerID  int
	ReplicaID int
}

// Hash function for request mapping
func requestHash(requestID int) int {
	return (requestID*requestID + 2*requestID + 17) % HASH_MOD
}

// Hash function for virtual server mapping
func virtualServerHash(serverID, replicaID int) int {
	return (serverID*serverID + replicaID*replicaID + 2*replicaID + 25) % HASH_MOD
}

// AddServer adds a server to the consistent hash map
func AddServer(serverID int) error {
	cmd := exec.Command("docker", "run", "-d", "--name", fmt.Sprintf("Server%d", serverID), "-p", fmt.Sprintf("%d:5000", getNextPort(serverID)), "--network", "assign1_net1", "--network-alias", fmt.Sprintf("Server%d", serverID), "-e", fmt.Sprintf("SERVER_ID=%d", serverID), "server_image")
	err := cmd.Run()
	if err != nil {
		log.Printf("Error spawning new server container for Server%d: %v", serverID, err)
		return err
	}
	fmt.Printf("\nServer%d spawned successfully. \n", serverID)
	return nil
}

func addReplicas(chMap []Entry, serverID int) {
	for i := 0; i < K; i++ {
		virtualServerID := virtualServerHash(serverID, i)
		slot := virtualServerID % M
		if chMap[slot].IsEmpty {
			chMap[slot] = Entry{IsEmpty: false, IsServer: true, ServerID: serverID, ReplicaID: i}
		} else {
			// Apply linear probing in case of collision

			for j := 1; j < M; j++ {
				newSlot := (slot + 11*j) % M
				if chMap[newSlot].IsEmpty {
					chMap[newSlot] = Entry{IsEmpty: false, IsServer: true, ServerID: serverID, ReplicaID: i}
					break
				}
			}
		}
	}
}

func spawnNewServerContainer(containerName string, port int) error {
	sid := port - 5000
	cmd := exec.Command("docker", "run", "-d", "--name", containerName, "-p", fmt.Sprintf("%d:5000", port), "--network", "assign1_net1", "--network-alias", containerName, "-e", fmt.Sprintf("SERVER_ID=%d", sid), "server_image")

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("error spawning new server container: %v", err)
	}

	return nil
}

// RemoveServer removes a server from the consistent hash map
func RemoveServer(chMap []Entry, serverID int) {
	for i := 0; i < M; i++ {
		if chMap[i].IsServer && chMap[i].ServerID == serverID {
			chMap[i] = Entry{IsEmpty: true}
		}
	}
}

// AddRequest adds a request to the consistent hash map
func AddRequest(chMap []Entry, requestID int) int {
	slot := requestHash(requestID%HASH_MOD) % M
	if chMap[slot].IsEmpty {
		nextNearestServer := GetNextNearestServer(chMap, slot)
		// chMap[slot] = Entry{IsEmpty: false, IsServer: false, ServerID: requestID, ReplicaID: nextNearestServer}
		return chMap[nextNearestServer].ServerID
	} else {
		// Apply linear probing to find the next empty slot
		for i := 1; i < M; i++ {
			newSlot := (slot + i) % M
			if chMap[newSlot].IsEmpty {
				nextNearestServer := GetNextNearestServer(chMap, newSlot)
				// chMap[newSlot] = Entry{IsEmpty: false, IsServer: false, ServerID: requestID, ReplicaID: nextNearestServer}
				return chMap[nextNearestServer].ServerID
				// break
			}
		}
	}
	// If no empty slot is found, the request will not be added to avoid overwriting existing data
	return 0
}

// GetNextNearestServer finds the index of the next nearest server in the circular hash map
func GetNextNearestServer(chMap []Entry, slot int) int {
	for i := 1; i <= M; i++ {
		nextSlot := (slot + i) % M
		if chMap[nextSlot].IsServer {
			return nextSlot
		}
	}
	return -1 // No server found (should not happen in a valid configuration)
}

// GetServer finds the nearest server for a given request ID
func GetServer(chMap []Entry, requestID int) int {
	slot := requestHash(requestID) % M

	// Apply linear probing to find the slot of the request
	for i := 0; i < M; i++ {
		currentSlot := (slot + i) % M
		if !chMap[currentSlot].IsEmpty {
			// Find the next nearest server based on the found slot
			nextNearestServer := GetNextNearestServer(chMap, currentSlot)
			return chMap[nextNearestServer].ServerID
		}
	}

	return -1 // No server found (should not happen in a valid configuration)
}

// PrintMap prints the consistent hash map
func PrintMap(chMap []Entry) {
	fmt.Println("Consistent Hash Map:")
	for i, entry := range chMap {
		if entry.IsEmpty {
			fmt.Printf("Slot %d: Empty\n", i)
		} else if entry.IsServer {
			fmt.Printf("Slot %d: Server %d, Replica %d\n", i, entry.ServerID, entry.ReplicaID)
		} else {
			fmt.Printf("Slot %d: Request %d, Next Nearest Replica %d\n", i, entry.ServerID, entry.ReplicaID)
		}
	}
}

// LoadBalancer represents the load balancer
type LoadBalancer struct {
	servers []string   // Names of web server containers
	hashMap []Entry    // Consistent hash map
	mu      sync.Mutex // Mutex for concurrent access
}

// NewLoadBalancer creates a new LoadBalancer instance
func NewLoadBalancer() *LoadBalancer {
	return &LoadBalancer{
		servers: make([]string, 0),
		hashMap: make([]Entry, M),
	}
}

var (
	portCounter = 5001
	serverPorts = make(map[int]int)
)

// Function to get the next available port for a new server container
func getNextPort(serverID int) int {
	if port, exists := serverPorts[serverID]; exists {
		return port
	}

	// Allocate a new port and store the mapping
	port := portCounter
	portCounter++
	serverPorts[serverID] = port
	return port
}

// Endpoints

func (lb *LoadBalancer) getActiveServers() []string {
	var activeServers []string
	for i, serverName := range lb.servers {
		if lb.serverIDexists(i + 1) {
			activeServers = append(activeServers, serverName)
		}
	}
	return activeServers
}

func (lb *LoadBalancer) wrongPathHandler(w http.ResponseWriter, r *http.Request) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	response := map[string]interface{}{
		"message": "<Error> '/other' endpoint does not exist in server replicas",
		"status":  "failure",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(response)
}

func (lb *LoadBalancer) replicasHandler(w http.ResponseWriter, r *http.Request) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	replicas := lb.getActiveServers()
	response := map[string]interface{}{
		"message": map[string]interface{}{
			"N":        len(replicas),
			"replicas": replicas,
		},
		"status": "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (lb *LoadBalancer) addHandler(w http.ResponseWriter, r *http.Request) {
	// Parse JSON payload
	var payload struct {
		N         int      `json:"n"`
		Hostnames []string `json:"hostnames"`
	}

	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Validate payload
	if len(payload.Hostnames) != payload.N {
		response := map[string]interface{}{
			"message": fmt.Sprintf("<Error> Length of hostname list is not equal to the specified count (%d)", payload.N),
			"status":  "failure",
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Add new server instances
	// lb.mu.Lock()
	// defer lb.mu.Unlock()

	for _, hostname := range payload.Hostnames {
		err := AddServer(len(lb.servers) + 1)
		if err != nil {
			return
		}
		lb.servers = append(lb.servers, hostname)
		addReplicas(lb.hashMap, len(lb.servers))
	}

	// Respond with the updated replicas
	replicas := lb.getActiveServers()
	response := map[string]interface{}{
		"message": map[string]interface{}{
			"N":        len(replicas),
			"replicas": replicas,
		},
		"status": "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (lb *LoadBalancer) serverIDexists(serverID int) bool {
	for _, server := range lb.hashMap {
		if server.ServerID == serverID {
			return !server.IsEmpty
		}
	}
	return false
}

func (lb *LoadBalancer) removeHandler(w http.ResponseWriter, r *http.Request) {
	// Parse JSON payload
	var payload struct {
		N         int      `json:"n"`
		Hostnames []string `json:"hostnames"`
	}

	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Validate payload
	if len(payload.Hostnames) > payload.N {
		response := map[string]interface{}{
			"message": fmt.Sprintf("<Error> Length of hostname list is more than removable instances"),
			"status":  "failure",
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Remove server instances
	// lb.mu.Lock()
	// defer lb.mu.Unlock()
	removed := 0
	for i, hostname := range lb.servers {
		if removed == payload.N {
			break
		}
		// check if hostname is in lb.servers
		if contains(payload.Hostnames, hostname) && lb.serverIDexists(i+1) {
			// remove hostname from lb.servers
			cmd := exec.Command("docker", "stop", "-f", fmt.Sprintf("Server%d", i+1))
			cmd.Run()
			cmd = exec.Command("docker", "rm", "-f", fmt.Sprintf("Server%d", i+1))
			cmd.Run()
			// lb.servers = removeElement(lb.servers, hostname)
			RemoveServer(lb.hashMap, i+1)
			removed++
		}
		// lb.servers = removeElement(lb.servers, hostname)
	}

	for i := range lb.servers {
		if removed == payload.N {
			break
		}
		if lb.serverIDexists(i + 1) {
			cmd := exec.Command("docker", "stop", "-f", fmt.Sprintf("Server%d", i+1))
			cmd.Run()
			cmd = exec.Command("docker", "rm", "-f", fmt.Sprintf("Server%d", i+1))
			cmd.Run()
			// lb.servers = lb.servers[:len(lb.servers)-1]
			RemoveServer(lb.hashMap, i+1)
			removed++
		}
	}

	// Respond with the updated replicas
	replicas := lb.getActiveServers()
	response := map[string]interface{}{
		"message": map[string]interface{}{
			"N":        len(replicas),
			"replicas": replicas,
		},
		"status": "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func (lb *LoadBalancer) routeHandler(w http.ResponseWriter, r *http.Request) {
	// Generate a random 6-digit integer as the request ID
	requestID := generateRequestID()
	// get the path of the request
	path := r.URL.Path
	path = path[strings.LastIndex(path, "/")+1:]

	// Use consistent hashing to find the next nearest server
	serverIndex := AddRequest(lb.hashMap, requestID)
	// Check if there are available servers
	if serverIndex > 0 && serverIndex <= len(lb.servers) {
		fmt.Printf("\nServer%d is chosen. ", serverIndex)
		url := fmt.Sprintf("http://server%v:5000/%v", serverIndex, path)
		response, err := http.Get(url)
		if err != nil {
			fmt.Printf("Error sending HTTP request: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer response.Body.Close()

		// Copy the received HTTP response headers to the client's response writer
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}

		// Set the HTTP status code for the client's response
		w.WriteHeader(response.StatusCode)

		// Copy the received HTTP response body to the client's response writer
		_, err = io.Copy(w, response.Body)
		if err != nil {
			fmt.Printf("Error copying response body: %v\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

	} else {
		// Handle the case when no servers are available
		response := map[string]interface{}{
			"message": fmt.Sprintf("<Error> No servers available for routing"),
			"status":  "failure",
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(response)
	}
}

// generateRequestID generates a random 6-digit integer as the request ID
func generateRequestID() int {
	return rand.Intn(900000) + 100000 // Generate a random 6-digit ID
}

func main() {
	loadBalancer := NewLoadBalancer()

	for i := 0; i < M; i++ {
		loadBalancer.hashMap[i] = Entry{IsEmpty: true}
	}

	// Define HTTP endpoints
	http.HandleFunc("/rep", loadBalancer.replicasHandler)
	http.HandleFunc("/add", loadBalancer.addHandler)
	http.HandleFunc("/rm", loadBalancer.removeHandler)
	http.HandleFunc("/home", loadBalancer.routeHandler)
	//http.HandleFunc("/heartbeat", loadBalancer.routeHandler)
	http.HandleFunc("/", loadBalancer.wrongPathHandler)

	//check heartbeat and respwan if needed
	s := gocron.NewScheduler(time.UTC)
	_, err := s.Every(5).Seconds().SingletonMode().Do(checkHeartbeat, loadBalancer)
	if err != nil {
		return
	}
	s.StartAsync()

	// Start HTTP server
	port := 5000
	log.Printf("Load balancer listening on port %d...\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), nil))
}

func checkHeartbeat(lb *LoadBalancer) {
	for i := range lb.servers {
		serverID := i + 1
		if !lb.serverIDexists(serverID) {
			continue
		}
		url := fmt.Sprintf("http://server%v:5000/heartbeat", serverID)
		response, err := http.Get(url)

		if err != nil || response.StatusCode != 200 {
			startTime := time.Now()
			fmt.Printf("Server %v is down\n", serverID)
			cmd := exec.Command("docker", "stop", "-f", fmt.Sprintf("Server%d", serverID))
			err := cmd.Run()
			// if err != nil {
			// 	break
			// }
			cmd = exec.Command("docker", "rm", "-f", fmt.Sprintf("Server%d", serverID))
			err = cmd.Run()
			// if err != nil {
			// 	break
			// }
			RemoveServer(lb.hashMap, serverID)
			//spawn new server
			err = AddServer(len(lb.servers) + 1)
			if err != nil {
				break
			}
			lb.servers = append(lb.servers, lb.servers[i])
			addReplicas(lb.hashMap, len(lb.servers))
			endTime := time.Now()
			fmt.Printf("Server %v is respawned in %v ms\n", serverID, endTime.Sub(startTime).Milliseconds())
		}
	}
}
