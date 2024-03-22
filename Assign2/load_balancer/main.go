package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"time"
)

const (
	M   = 512
	K   = 9
	__p = 31
	__m = 1000000007
)

var (
	// one time read
	g_schema schema
	// all updated at the same time
	g_server_shards_mapping map[string]map[string]bool
	g_shard_servers_mapping map[string]map[string]bool
	g_server_port_mapping   map[string]int
	g_port_server_mapping   map[int]string
	g_server_count          = 0
	// hashmap
	g_servers_hashmap [M]string
	// shard info
	g_shards            map[string]shardMetaData
	g_shard_current_idx map[string]int
)

func stringHash(s string) int {
	res := 0
	p := 1
	for _, c := range s {
		res += (int(c) * p) % __m
		p *= __p
	}
	return res
}

func H(i uint32) uint32 {
	i = ((i >> 16) ^ i) * 0x45d9f3b
	i = ((i >> 16) ^ i) * 0x45d9f3b
	i = (i >> 16) ^ i
	return i
}

func Phi(i, j uint32) uint32 {
	return H(i + H(j))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type schema struct {
	Columns []string
	Dtypes  []string
}

type shard struct {
	Stud_id_low int
	Shard_id    string
	Shard_size  int
}

type shardMetaData struct {
	Stud_id_low  int
	Stud_id_high int
	Mutex        *sync.Mutex
}

type addPayload struct {
	N          int
	New_shards []shard
	Servers    map[string][]string
}

type student struct {
	Stud_id    int
	Stud_name  string
	Stud_marks int
}

func getJSONstring(c *gin.Context) string {
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Println("Error reading request body:", err)
		return "}"
	}
	return string(body)
}

func getServerPortMapping(server string) int {
	if _, ok := g_server_port_mapping[server]; ok {
		return g_server_port_mapping[server]
	}
	h := stringHash(server) % 64000
	for {
		if _, ok := g_port_server_mapping[h]; !ok {
			g_port_server_mapping[h] = server
			g_server_port_mapping[server] = h
			return h
		}
		h = int(H(uint32(h))) % 64000
	}
}

func eraseServerPortMapping(server string) {
	port := g_server_port_mapping[server]
	delete(g_server_port_mapping, server)
	delete(g_port_server_mapping, port)
}

func spawnContainer(server string) error {
	cmd := exec.Command("docker", "run", "-d", "--name", server, "-p", fmt.Sprintf("%d:5000", getServerPortMapping(server)), "--network", "assign2_net1", "--network-alias", server, "-e", fmt.Sprintf("SERVER_ID=%s", server), "server_image")
	err := cmd.Run()
	if err != nil {
		log.Printf("Error spawning new server container for %s: %v", server, err)
		return err
	}
	fmt.Printf("\n%v %s spawned successfully. \n", time.Now(), server)

	// fit into hashmap
	i := stringHash(server)
	for j := 0; j < K; j++ {
		pos := Phi(uint32(i), uint32(j)) % M
		for ; g_servers_hashmap[pos] != ""; pos = (pos + 1) % M {
		} // linear probing
		g_servers_hashmap[pos] = server
	}
	g_server_count++ // increment server count
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

	// remove from hashmap
	i := stringHash(server)
	for j := 0; j < K; j++ {
		pos := Phi(uint32(i), uint32(j)) % M
		for ; g_servers_hashmap[pos] != server; pos = (pos + 1) % M {
		} // linear probing
		if g_servers_hashmap[pos] == server {
			g_servers_hashmap[pos] = ""
			break
		}
	}
	eraseServerPortMapping(server)
	g_server_count-- // decrement server count
	return nil
}

func main() {

	g_server_shards_mapping = make(map[string]map[string]bool)
	g_shard_servers_mapping = make(map[string]map[string]bool)
	g_server_port_mapping = make(map[string]int)
	g_port_server_mapping = make(map[int]string)
	g_shards = make(map[string]shardMetaData)
	g_shard_current_idx = make(map[string]int)

	for i := 0; i < M; i++ {
		g_servers_hashmap[i] = ""
	}
	fmt.Printf("Proxy starting...")

	r := gin.Default()

	r.POST("/init", initHandler)
	r.GET("/status", statusHandler)
	r.POST("/add", addHandler)
	r.DELETE("/rm", rmHandler)
	r.POST("/read", readHandler)
	r.POST("/write", writeHandler)
	r.PUT("/update", updateHandler)
	r.DELETE("/del", delHandler)

	//check heartbeat and respwan if needed
	s := gocron.NewScheduler(time.UTC)
	_, err := s.Every(5).Seconds().SingletonMode().Do(checkHeartbeat)
	if err != nil {
		return
	}
	s.StartAsync()

	port := "5000"
	err = r.Run(":" + port)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

func delHandler(c *gin.Context) {

}

func updateHandler(c *gin.Context) {

}

func writeHandler(c *gin.Context) {

}

type readPayload struct {
	Shard   string         `json:"shard" binding:"required"`
	Stud_id map[string]int `json:"Stud_id" binding:"required"`
}

type readResponse struct {
	Data   []student
	Status string
}

func readHandler(c *gin.Context) {
	var payload struct {
		Stud_id map[string]int
	}
	jsonString := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	var response struct {
		shards_queried []string
		Data           []student
		Status         string
	}
	for shard_ := range g_shards {
		if !(payload.Stud_id["low"] >= g_shards[shard_].Stud_id_high || payload.Stud_id["high"] <= g_shards[shard_].Stud_id_low) {
			for server := range g_shard_servers_mapping[shard_] {
				fmt.Printf("\nForwarding to %s\n", server)
				readEndpoint := fmt.Sprintf("http://%s:5000/read", server)
				var body readPayload
				body.Shard = shard_
				body.Stud_id = make(map[string]int)
				body.Stud_id["low"] = max(payload.Stud_id["low"], g_shards[shard_].Stud_id_low)
				body.Stud_id["high"] = min(payload.Stud_id["high"], g_shards[shard_].Stud_id_high)
				fmt.Printf("\n%v\n", body) // TODO: remove
				jsonBody, _ := json.Marshal(body)
				// mutex for this shard
				g_shards[shard_].Mutex.Lock()
				post, err := http.Post(readEndpoint, "application/json", bytes.NewReader(jsonBody))
				g_shards[shard_].Mutex.Unlock()
				if err == nil {
					resp, _ := io.ReadAll(post.Body)
					var respBody readResponse
					err := json.Unmarshal(resp, &respBody)
					if err != nil {
						continue
					}
					response.shards_queried = append(response.shards_queried, shard_)
					response.Data = append(response.Data, respBody.Data...)
					break
				}
			}
		}
	}
	response.Status = "success"
	// TODO: need to change
	c.JSON(http.StatusOK, gin.H{"shards_queried": response.shards_queried, "data": response.Data, "status": response.Status})
}

func rmHandler(c *gin.Context) {

}

func addHandler(c *gin.Context) {

}

func statusHandler(c *gin.Context) {
	var shardlist []shard
	for shard_ := range g_shards {
		shardlist = append(shardlist, shard{g_shards[shard_].Stud_id_low, shard_, g_shards[shard_].Stud_id_high - g_shards[shard_].Stud_id_low})
	}
	server_shards_mapping := make(map[string][]string)
	for server, shards := range g_server_shards_mapping {
		for shard_ := range shards {
			server_shards_mapping[server] = append(server_shards_mapping[server], shard_)
		}
	}
	c.JSON(http.StatusOK, gin.H{"N": g_server_count, "schema": g_schema, "shards": shardlist, "servers": server_shards_mapping})

}

func initHandler(c *gin.Context) {

}

func checkHeartbeat() {

}
