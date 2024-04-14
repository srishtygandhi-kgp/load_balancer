package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	"log"
	"net/http"
	"os/exec"
	"time"
)

var (
	g_shard_server_map = make(map[string]map[string]bool)
	g_port_mapping     = make(map[string]int)
	g_port_counter     = 5010
)

func getServerPortMapping(server string) int {
	if port, ok := g_port_mapping[server]; ok {
		return port
	}
	g_port_mapping[server] = g_port_counter
	g_port_counter++
	return g_port_mapping[server]
}

func psinfoHandler(c *gin.Context) {
	c.JSON(http.StatusOK, g_shard_server_map)
}

func eraseServerPortMapping(server string) {
	delete(g_port_mapping, server)
}

func broadcastShardServerMap() {
	for server := range g_port_mapping {
		jsonBody, err := json.Marshal(g_shard_server_map)
		if err != nil {
			fmt.Print("Error marshalling g_shard_server_map: %v", err)
		}
		configEndpoint := fmt.Sprintf("http://%s:5000/updatepsinfo", server)
		_, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		if err != nil {
			fmt.Print("Error sending g_shard_server_map to server: %v", server)
		}
	}
}

func spawnContainer(server string) error {
	cmd := exec.Command("docker", "run", "-d", "--name", server, "-p", fmt.Sprintf("%d:5000", getServerPortMapping(server)), "--network", "assign3_net1", "--network-alias", server, "-e", fmt.Sprintf("SERVER_ID=%s", server), "server_image")
	err := cmd.Run()
	if err != nil {
		log.Printf("Error spawning new server container for %s: %v", server, err)
		return err
	}
	fmt.Printf("\n%v %s spawned successfully. \n", time.Now(), server)
	return nil
}

func removeContainer(server string) error {
	cmd := exec.Command("docker", "rm", "-f", server)
	err := cmd.Run()
	if err != nil {
		log.Printf("Error removing server container for %s: %v", server, err)
		return err
	}
	fmt.Printf("\n%s removed successfully. \n", server)

	eraseServerPortMapping(server)
	return nil
}

func initHandler(c *gin.Context) {
	jsonData := getJSONstring(c)
	//fmt.Printf("\n%v", jsonData)
	var payload initPayload
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	for _, shard := range payload.Shards {
		g_shard_server_map[shard.Shard_id] = make(map[string]bool)
	}
	var spawned []string
	for server := range payload.Servers {
		err := spawnContainer(server)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
		spawned = append(spawned, server)
	}
	for _, server := range spawned {
		shards := payload.Servers[server]
		var body configPayload
		body.Shards = shards
		body.Map_data = make(map[string]map[string]bool)
		jsonBody, err := json.Marshal(body)
		configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
		post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		for {
			if err == nil {
				break
			}
			post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		}
		if post.StatusCode != http.StatusOK {
			log.Printf("\nError configuring server: %v, %v\n", server, err)
		}
		for _, shard_ := range payload.Servers[server] {
			g_shard_server_map[shard_][server] = false
		}
	}
	for _, shard_ := range payload.Shards {
		reElect(shard_.Shard_id)
	}
	broadcastShardServerMap()

	// return OK
	c.JSON(http.StatusOK, gin.H{"message": "Configured Database", "status": "success"})
}

func addHandler(c *gin.Context) {
	jsonString := getJSONstring(c)
	var payload addPayload
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	for _, shard_ := range payload.New_shards {
		if _, ok := g_shard_server_map[shard_.Shard_id]; !ok {
			g_shard_server_map[shard_.Shard_id] = make(map[string]bool)
		}
	}

	for server, shards := range payload.Servers {
		if _, ok := g_port_mapping[server]; !ok {
			err := spawnContainer(server)
			if err != nil {
				log.Printf("%v", err)
				continue
			}
			var body configPayload
			body.Shards = shards
			body.Map_data = g_shard_server_map
			jsonBody, err := json.Marshal(body)
			configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
			post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
			for {
				if err == nil {
					break
				}
				post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
			}
			if post.StatusCode != http.StatusOK {
				fmt.Printf("Error configuring server %s: %v", server, err)
			}
			for _, shard_ := range shards {
				g_shard_server_map[shard_][server] = false
			}
		} else {
			for _, shard_ := range shards {
				if _, ok := g_shard_server_map[shard_][server]; !ok {
					// add server to shard
					res, err := http.Post(fmt.Sprintf("http://%s:5000/add", server), "application/json", bytes.NewBuffer([]byte(fmt.Sprintf(`{"shard": "%s"}`, shard_))))
					if err != nil || res.StatusCode != http.StatusOK {
						log.Printf("Error adding shard %s to server %s: %v", shard_, server, err)
						continue
					}
					g_shard_server_map[shard_][server] = false
				}
			}
		}
	}
	for _, shard_ := range payload.New_shards {
		reElect(shard_.Shard_id)
	}
	broadcastShardServerMap()

	c.JSON(http.StatusOK, gin.H{"message": "Added new servers", "status": "success"})
}

func rmHandler(c *gin.Context) {
	jsonString := getJSONstring(c)
	var payload struct {
		N       int
		Servers []string
	}
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	// remove servers
	for _, server := range payload.Servers {
		for shard_ := range g_shard_server_map {
			delete(g_shard_server_map[shard_], server)
		}
		err := removeContainer(server)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
	}
	// re-elect primary servers if necessary
	for shard_, servers := range g_shard_server_map {
		noPrimary := true
		for server := range servers {
			if servers[server] {
				noPrimary = false
				break
			}
		}
		if noPrimary {
			reElect(shard_)
		}
	}
	broadcastShardServerMap()
	c.JSON(http.StatusOK, gin.H{"message": "Removed servers", "status": "success"})
}
