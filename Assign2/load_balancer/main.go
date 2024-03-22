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
	"strconv"
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

type configPayload struct {
	Schema schema   `json:"schema" binding:"required"`
	Shards []string `json:"shards" binding:"required"`
}

type initPayload struct {
	N       int
	Schema  schema
	Shards  []shard
	Servers map[string][]string
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
	g_schema = payload.Schema
	for _, shard := range payload.Shards {
		g_shards[shard.Shard_id] = shardMetaData{shard.Stud_id_low, shard.Stud_id_low + shard.Shard_size, &sync.Mutex{}}
	}
	//fmt.Printf("\n%v", payload.N)
	//fmt.Printf("\n%v", payload.Schema)
	//fmt.Printf("\n%v", payload.Shards)
	//fmt.Printf("\n%v\n", payload.Servers)
	for server := range payload.Servers {
		err := spawnContainer(server)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
	}
	for server, shards := range payload.Servers {
		var body configPayload
		body.Schema = g_schema
		body.Shards = shards
		jsonBody, err := json.Marshal(body)
		configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
		post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		for {
			if err == nil {
				break
			}
			post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		}
		//if err != nil {
		//	//TODO: Handle post request failure
		//	log.Printf("%v", err)
		//	continue
		//}
		//if status not success print
		if post.StatusCode != http.StatusOK {
			//TODO: print error
		}
		for _, shard_ := range shards {
			if _, ok := g_shard_servers_mapping[shard_]; !ok {
				g_shard_servers_mapping[shard_] = make(map[string]bool)
			}
			g_shard_servers_mapping[shard_][server] = true
			if _, ok := g_server_shards_mapping[server]; !ok {
				g_server_shards_mapping[server] = make(map[string]bool)
			}
			g_server_shards_mapping[server][shard_] = true
		}
	}

	// return OK
	c.JSON(http.StatusOK, gin.H{"message": "Configured Database", "status": "success"})
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
		g_shards[shard_.Shard_id] = shardMetaData{shard_.Stud_id_low, shard_.Stud_id_low + shard_.Shard_size, &sync.Mutex{}}
	}

	for serverGiven, shards := range payload.Servers {
		server := ""
		for _, c := range serverGiven {
			if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
				server += string(c)
			}
		}
		err := spawnContainer(server)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
		var body configPayload
		body.Schema = g_schema
		body.Shards = shards
		jsonBody, err := json.Marshal(body)
		configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
		post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		for {
			if err == nil {
				break
			}
			post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		}
		//if err != nil {
		//	//TODO: Handle post request failure
		//	log.Printf("%v", err)
		//	continue
		//}
		//if status not success print
		if post.StatusCode != http.StatusOK {
			//TODO: print error
			continue
		}
		for _, shard_ := range shards {
			if _, ok := g_shard_servers_mapping[shard_]; !ok {
				g_shard_servers_mapping[shard_] = make(map[string]bool)
			}
			g_shard_servers_mapping[shard_][server] = true
			if _, ok := g_server_shards_mapping[server]; !ok {
				g_server_shards_mapping[server] = make(map[string]bool)
			}
			g_server_shards_mapping[server][shard_] = true
		}
	}

	var toSend struct {
		N       int
		Message string
		Status  string
	}
	toSend.N = g_server_count
	toSend.Message = "Added"
	for serv := range payload.Servers {
		toSend.Message += " " + serv
	}
	toSend.Status = "success"
	// return OK
	c.JSON(http.StatusOK, toSend)
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
	// sanity check
	if len(payload.Servers) > payload.N {
		c.JSON(http.StatusBadRequest, gin.H{"message": "<Error> Length of server list is more than removable instances", "status": "failure"})
		return
	}
	// add servers to the list if not enough
	for server := range g_server_shards_mapping {
		if len(payload.Servers) == payload.N {
			break
		}
		payload.Servers = append(payload.Servers, server)
	}
	// remove servers from the list
	for _, server := range payload.Servers {
		err := removeContainer(server)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
		// remove server from shard mapping
		for shard_ := range g_server_shards_mapping[server] {
			delete(g_shard_servers_mapping[shard_], server)
		}
		delete(g_server_shards_mapping, server)
	}
	c.JSON(http.StatusOK, gin.H{"message": gin.H{"N": g_server_count, "servers": payload.Servers}, "status": "success"})
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

func writeHandler(c *gin.Context) {
	var payload struct {
		Data []student
	}
	jsonString := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	dataToWriteToShards := make(map[string][]student)
	for _, row := range payload.Data {
		for shard_ := range g_shards {
			if row.Stud_id >= g_shards[shard_].Stud_id_low && row.Stud_id < g_shards[shard_].Stud_id_high {
				dataToWriteToShards[shard_] = append(dataToWriteToShards[shard_], row)
				break
			}
		}
	}
	for shard_, rows := range dataToWriteToShards {
		fmt.Printf("\nFor data %v\n", rows)
		done := true
		for server := range g_shard_servers_mapping[shard_] {
			fmt.Printf("\nForwarding to %s\n", server)
			writeEndpoint := fmt.Sprintf("http://%s:5000/write", server)
			var body struct {
				Shard    string
				Curr_idx int
				Data     []student
			}
			body.Shard = shard_
			// check if shard_ is in g_shard_current_idx
			if _, ok := g_shard_current_idx[shard_]; !ok {
				body.Curr_idx = g_shard_current_idx[shard_]
			} else {
				body.Curr_idx = 0
				g_shard_current_idx[shard_] = 0
			}
			body.Data = rows
			jsonBody, _ := json.Marshal(body)
			// lock mutex for this shard
			g_shards[shard_].Mutex.Lock()
			res, err := http.Post(writeEndpoint, "application/json", bytes.NewReader(jsonBody))
			g_shards[shard_].Mutex.Unlock()
			if err != nil {
				fmt.Printf("\n%v\n%v", err, res)
				done = false
				continue
			}
		}
		if done {
			g_shard_current_idx[shard_] += len(rows)
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": strconv.Itoa(len(payload.Data)) + " Data entries added", "status": "success"})
}

type updatePayload struct {
	Shard   string  `json:"shard" binding:"required"`
	Stud_id int     `json:"Stud_id" binding:"required"`
	Data    student `json:"data" binding:"required"`
}

func updateHandler(c *gin.Context) {
	var payload struct {
		Stud_id int
		Data    student
	}
	jsonString := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	for shard_ := range g_shards {
		if payload.Stud_id >= g_shards[shard_].Stud_id_low && payload.Stud_id < g_shards[shard_].Stud_id_high {
			for server := range g_shard_servers_mapping[shard_] {
				fmt.Printf("\nForwarding to %s\n", server)
				updateEndpoint := fmt.Sprintf("http://%s:5000/update", server)
				var body updatePayload
				body.Data = payload.Data
				body.Stud_id = payload.Stud_id
				body.Shard = shard_
				jsonBody, err := json.Marshal(body)
				if err != nil {
					//TODO: handle json error
				}
				// mutex for this shard
				g_shards[shard_].Mutex.Lock()
				put, err := http.NewRequest(http.MethodPut, updateEndpoint, bytes.NewReader(jsonBody))
				g_shards[shard_].Mutex.Unlock()
				if err != nil {
					//TODO: handle request error
					continue
				}
				client := http.Client{}
				g_shards[shard_].Mutex.Lock()
				do, err := client.Do(put)
				g_shards[shard_].Mutex.Unlock()
				if err != nil {
					//TODO: handle put error
					continue
				}
				if do.StatusCode != http.StatusOK {
					//TODO: handle update failed
					continue
				}
			}
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "Data entry for Stud_id: " + strconv.Itoa(payload.Stud_id) + " updated", "status": "success"})
}

type delPayload struct {
	Shard   string `json:"shard" binding:"required"`
	Stud_id int    `json:"Stud_id" binding:"required"`
}

func delHandler(c *gin.Context) {
	var payload struct {
		Stud_id int
	}
	jsonString := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	for shard_ := range g_shards {
		if payload.Stud_id >= g_shards[shard_].Stud_id_low && payload.Stud_id < g_shards[shard_].Stud_id_high {
			for server := range g_shard_servers_mapping[shard_] {
				fmt.Printf("\nForwarding to %s\n", server)
				delEndpoint := fmt.Sprintf("http://%s:5000/del", server)
				var body delPayload
				body.Shard = shard_
				body.Stud_id = payload.Stud_id
				jsonBody, _ := json.Marshal(body)
				g_shards[shard_].Mutex.Lock()
				del, err := http.NewRequest(http.MethodDelete, delEndpoint, bytes.NewReader(jsonBody))
				g_shards[shard_].Mutex.Unlock()
				if err != nil {
					//TODO: handle request error
					continue
				}
				client := http.Client{}
				g_shards[shard_].Mutex.Lock()
				do, err := client.Do(del)
				g_shards[shard_].Mutex.Unlock()
				if err != nil {
					//TODO: handle del error
					continue
				}
				if do.StatusCode != http.StatusOK {
					//TODO: handle delete failed
					continue
				}
			}
			break
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "Data entry for Stud_id: " + strconv.Itoa(payload.Stud_id) + " removed from all replicas", "status": "success"})
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

func checkHeartbeat() {
	failure_server_shard_mapping := make(map[string]map[string]bool)
	for server := range g_server_shards_mapping {
		get, err := http.Get(fmt.Sprintf("http://%s:5000/heartbeat", server))
		if err != nil || get.StatusCode != http.StatusOK {
			//print current time with the log
			log.Default().Printf("%v %s is down\n", time.Now(), server)
			failure_server_shard_mapping[server] = g_server_shards_mapping[server]
			delete(g_server_shards_mapping, server)
			for shard := range failure_server_shard_mapping[server] {
				delete(g_shard_servers_mapping[shard], server)
			}
		}
	}
	for server := range failure_server_shard_mapping {
		err := removeContainer(server)
		if err != nil {
			log.Default().Printf("Error removing server container for %s: %v", server, err)
			continue
		}
		err = spawnContainer(server)
		if err != nil {
			log.Default().Printf("Error spawning new server container for %s: %v", server, err)
			continue
		}
		//make map keyset to array
		var shards []string
		for sh := range failure_server_shard_mapping[server] {
			shards = append(shards, sh)
		}
		var body configPayload
		body.Schema = g_schema
		body.Shards = shards
		jsonBody, err := json.Marshal(body)
		configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
		post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		for {
			if err == nil {
				break
			}
			post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		}
		//if err != nil {
		//	//TODO: Handle post request failure
		//	log.Printf("%v", err)
		//	continue
		//}
		//if status not success print
		if post.StatusCode != http.StatusOK {
			//TODO: print error
			log.Default().Printf("Error configuring server %s: %v", server, err)
			continue
		}
		g_server_shards_mapping[server] = make(map[string]bool)
		for sh := range failure_server_shard_mapping[server] {
			if len(g_shard_servers_mapping[sh]) != 0 {
				log.Default().Printf("Copying data for %s, downed server = %s", sh, server)
				var backupServer string
				for k := range g_shard_servers_mapping[sh] {
					backupServer = k
					break
				}
				var copyPayload struct {
					Shards []string `json:"shards" binding:"required"`
				}
				copyPayload.Shards = append(copyPayload.Shards, sh)
				jsonBody, _ := json.Marshal(copyPayload)
				copyReq, err := http.NewRequest(http.MethodGet, fmt.Sprintf("http://%s:5000/copy", backupServer), bytes.NewReader(jsonBody))
				if err != nil {
					log.Default().Printf("Error creating copy request for %s: %v", server, err)
					continue
					//TODO: handle request error
				}
				client := http.Client{}
				do, err := client.Do(copyReq)
				if err != nil {
					//TODO: handle copy failed
					log.Default().Printf("Error copying data to %s: %v", server, err)
					continue
				}
				if do.StatusCode != http.StatusOK {
					log.Default().Printf("Error copying data to %s: %v", server, "Status not OK")
					continue
				}
				all, err := ioutil.ReadAll(do.Body)
				if err != nil {
					log.Default().Printf("Error reading response body: %v", err)
					continue
				}
				var result map[string]interface{}
				err = json.Unmarshal(all, &result)
				if err != nil {
					log.Default().Printf("Error decoding JSON: %v", err)
					continue
				}
				if len(result[sh].([]interface{})) != 0 {

					writeEndpoint := fmt.Sprintf("http://%s:5000/write", server)
					var body struct {
						Shard    string
						Curr_idx int
						Data     []student
					}
					body.Shard = sh
					//convert result[sh] from []interface to []student type
					var data []student
					for _, v := range result[sh].([]interface{}) {
						data = append(data, student{
							Stud_id:    int(v.(map[string]interface{})["Stud_id"].(float64)),
							Stud_name:  v.(map[string]interface{})["Stud_name"].(string),
							Stud_marks: int(v.(map[string]interface{})["Stud_marks"].(float64)),
						})
					}
					body.Data = data
					body.Curr_idx = 0
					jsonBody, _ = json.Marshal(body)
					res, err := http.Post(writeEndpoint, "application/json", bytes.NewReader(jsonBody))
					if err != nil {
						log.Default().Printf("Error writing data to %s: %v", server, err)
						continue
					}
					if res.StatusCode != http.StatusOK {
						log.Default().Printf("Error writing data to %s: %v", server, "Status not OK")
						continue
					}

				}
			}
			g_server_shards_mapping[server][sh] = true
			g_shard_servers_mapping[sh][server] = true
			log.Default().Printf("shard %s Data copied to %s", sh, server)
		}
	}
}
