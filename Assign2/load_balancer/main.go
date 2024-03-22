package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	"io/ioutil"
	"log"
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

func delHandler(context *gin.Context) {

}

func updateHandler(context *gin.Context) {

}

func writeHandler(context *gin.Context) {

}

func readHandler(context *gin.Context) {

}

func rmHandler(context *gin.Context) {

}

func addHandler(context *gin.Context) {

}

func statusHandler(context *gin.Context) {

}

func initHandler(context *gin.Context) {

}

func checkHeartbeat() {

}
