package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
)

var (
	lb = &loadBalancer{}
)

func getJSONstring(c *gin.Context) string {
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Println("Error reading request body:", err)
		return "}"
	}
	return string(body)
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
	// send the payload to the shard manager as it is
	// the shard manager will spawn the containers and configure them
	_, err = http.Post("http://shard_manager:5000/init", "application/json", bytes.NewReader([]byte(jsonData)))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error sending request to shard manager", "status": "failure"})
		return
	}
	for _, shard_ := range payload.Shards {
		if lb.shards == nil {
			lb.shards = make(map[string]*shardMetaData)
		}
		lb.shards[shard_.Shard_id] = &shardMetaData{
			Stud_id_low:  shard_.Stud_id_low,
			Shard_id:     shard_.Shard_id,
			Stud_id_high: shard_.Stud_id_low + shard_.Shard_size,
			Shard_size:   shard_.Shard_size,
			hashmap:      &[M]string{},
			servers:      make(map[string]bool),
			rw:           &sync.RWMutex{},
			req_count:    0,
		}
		//lock mutex
		lb.shards[shard_.Shard_id].rw.Lock()
		//defer unlock mutex
		defer lb.shards[shard_.Shard_id].rw.Unlock()
	}
	for server, shard_list := range payload.Servers {
		for _, shard_ := range shard_list {
			lb.insertServer(server, shard_)
		}
	}

	// return OK
	c.JSON(http.StatusOK, gin.H{"message": "Configured Database", "status": "success"})
}

func statusHandler(c *gin.Context) {
	var shard_list []shard
	for _, shard_ := range lb.shards {
		shard_list = append(shard_list, shard{
			Stud_id_low: shard_.Stud_id_low,
			Shard_id:    shard_.Shard_id,
			Shard_size:  shard_.Shard_size,
		})
		//read lock
		shard_.rw.RLock()
		//defer unlock
		defer shard_.rw.RUnlock()
	}
	c.JSON(http.StatusOK, gin.H{"N": len(lb.server_shard_mapping), "shards": shard_list, "servers": lb.server_shard_mapping})
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
	// send the payload to the shard manager as it is
	// the shard manager will spawn the containers and configure them
	_, err = http.Post("http://shard_manager:5000/add", "application/json", bytes.NewReader([]byte(jsonString)))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error sending request to shard manager", "status": "failure"})
		return
	}
	for _, shard_ := range payload.New_shards {
		if _, ok := lb.shards[shard_.Shard_id]; !ok {
			lb.shards[shard_.Shard_id] = &shardMetaData{
				Stud_id_low:  shard_.Stud_id_low,
				Shard_id:     shard_.Shard_id,
				Stud_id_high: shard_.Stud_id_low + shard_.Shard_size,
				Shard_size:   shard_.Shard_size,
				hashmap:      &[M]string{},
				servers:      make(map[string]bool),
				rw:           &sync.RWMutex{},
				req_count:    0,
			}
		}
		//lock mutex
		lb.shards[shard_.Shard_id].rw.Lock()
		//defer unlock mutex
		defer lb.shards[shard_.Shard_id].rw.Unlock()

	}
	for server, shard_list := range payload.Servers {
		for _, shard_ := range shard_list {
			lb.insertServer(server, shard_)
		}
	}
	// return OK
	msgStr := "Added Servers "
	for server, _ := range payload.Servers {
		msgStr += server
		msgStr += ", "
	}
	c.JSON(http.StatusOK, gin.H{"N": len(lb.server_shard_mapping), "message": msgStr, "status": "success"})
}

func rmHandler(c *gin.Context) {
	jsonString := getJSONstring(c)
	var payload rmPayload
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
	// change metadata here
	deleted := 0
	for _, server := range payload.Servers {
		for _, shard_ := range lb.server_shard_mapping[server] {
			delete(lb.server_shard_mapping, server)
			lb.removeServer(server, shard_)
		}
		deleted++
	}
	for server, _ := range lb.server_shard_mapping {
		if deleted == payload.N {
			break
		}
		for _, shard_ := range lb.server_shard_mapping[server] {
			delete(lb.server_shard_mapping, server)
			lb.removeServer(server, shard_)
		}
		deleted++
		payload.Servers = append(payload.Servers, server)
	}
	// send the payload as it is to shard manager
	jsonValue, _ := json.Marshal(payload)
	_, err = http.Post("http://shard_manager:5000/rm", "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error sending request to shard manager", "status": "failure"})
		return
	}
	// return OK
	c.JSON(http.StatusOK, gin.H{"N": len(lb.server_shard_mapping), "servers": payload.Servers, "status": "success"})
}

func main() {
	// initialise the shard manager

	r := gin.Default()

	r.POST("/init", initHandler)
	r.GET("/status", statusHandler)
	r.POST("/add", addHandler)
	r.DELETE("/rm", rmHandler)
	r.POST("/read", readHandler)
	r.POST("/write", writeHandler)
	r.PUT("/update", updateHandler)
	r.DELETE("/del", delHandler)
	r.GET("/read/:server_id", getallHandler)

	port := "5000"
	err := r.Run(":" + port)
	if err != nil {
		log.Fatalf("Error starting Load Balancer: %v", err)
	}
}
