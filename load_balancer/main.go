package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-co-op/gocron"
	"log"
	"net/http"
	"io"
	"math/rand"
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
	fmt.Printf("\nServer%d spawned successfully. ", serverID)
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
				newSlot := (slot + j) % M
				if chMap[newSlot].IsEmpty {
					chMap[newSlot] = Entry{IsEmpty: false, IsServer: true, ServerID: serverID, ReplicaID: i}
					break
				}
			}
		}
	}
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
