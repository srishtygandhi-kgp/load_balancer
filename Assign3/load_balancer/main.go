package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
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
	for shard_ := range lb.shards {
		if payload.Stud_id >= lb.shards[shard_].Stud_id_low && payload.Stud_id < lb.shards[shard_].Stud_id_high {
			// lock shard
			lb.shards[shard_].rw.Lock()
			//defer unlock
			defer lb.shards[shard_].rw.Unlock()
			// get psinfo from shard manager
			resp, err := http.Get("http://shard_manager:5000/psinfo")
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting psinfo from shard manager", "status": "failure"})
				return
			}
			var psinfo map[string]map[string]bool
			err = json.NewDecoder(resp.Body).Decode(&psinfo)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error decoding psinfo from shard manager", "status": "failure"})
				return
			}
			// send delete request to primary servers
			for server, isPrimary := range psinfo[shard_] {
				if isPrimary {
					// send data to this server
					dataToSend, err := json.Marshal(payload)
					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{"message": "Error marshalling data", "status": "failure"})
						return
					}
					del, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s:5000/del", server), bytes.NewReader(dataToSend))
					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating delete request", "status": "failure"})
						return
					}
					del.Header.Set("Content-Type", "application/json")
					lb.shards[shard_].req_count++
					del.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
					do, err := http.DefaultClient.Do(del)
					for err != nil && do.StatusCode != http.StatusOK {
						// retry
						do, err = http.DefaultClient.Do(del)
					}
					break
				}
			}
			break
		}

	}
	c.JSON(http.StatusOK, gin.H{"message": "Data entry for Stud_id: " + strconv.Itoa(payload.Stud_id) + " removed from all replicas", "status": "success"})

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
	for shard_ := range lb.shards {
		if payload.Stud_id >= lb.shards[shard_].Stud_id_low && payload.Stud_id < lb.shards[shard_].Stud_id_high {
			// lock shard
			lb.shards[shard_].rw.Lock()
			//defer unlock
			defer lb.shards[shard_].rw.Unlock()

			// get psinfo from shard manager
			resp, err := http.Get("http://shard_manager:5000/psinfo")
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting psinfo from shard manager", "status": "failure"})
				return
			}
			var psinfo map[string]map[string]bool
			err = json.NewDecoder(resp.Body).Decode(&psinfo)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error decoding psinfo from shard manager", "status": "failure"})
				return
			}
			// send update request to primary servers
			for server, isPrimary := range psinfo[shard_] {
				if isPrimary {
					// send data to this server as put request
					dataToSend, err := json.Marshal(payload.Data)
					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{"message": "Error marshalling data", "status": "failure"})
						return
					}
					put, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s:5000/update", server), bytes.NewReader(dataToSend))
					if err != nil {
						c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating put request", "status": "failure"})
						return
					}
					put.Header.Set("Content-Type", "application/json")
					lb.shards[shard_].req_count++
					put.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
					do, err := http.DefaultClient.Do(put)
					for err != nil && do.StatusCode != http.StatusOK {
						// retry
						do, err = http.DefaultClient.Do(put)
					}
					break
				}
			}
			break
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "Data entry for Stud_id: " + strconv.Itoa(payload.Stud_id) + " updated", "status": "success"})
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
		for shard_ := range lb.shards {
			if row.Stud_id >= lb.shards[shard_].Stud_id_low && row.Stud_id < lb.shards[shard_].Stud_id_high {
				dataToWriteToShards[shard_] = append(dataToWriteToShards[shard_], row)
				break
			}
		}
	}
	for shard_ := range dataToWriteToShards {
		// lock shard
		lb.shards[shard_].rw.Lock()
		//defer unlock
		defer lb.shards[shard_].rw.Unlock()
	}

	// get psinfo from shard manager
	resp, err := http.Get("http://shard_manager:5000/psinfo")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting psinfo from shard manager", "status": "failure"})
		return
	}
	var psinfo map[string]map[string]bool
	err = json.NewDecoder(resp.Body).Decode(&psinfo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error decoding psinfo from shard manager", "status": "failure"})
		return
	}
	// send write request to primary servers
	for shard_, data := range dataToWriteToShards {
		for server, isPrimary := range psinfo[shard_] {
			if isPrimary {
				// send data to this server
				var payload writePayload
				payload.Shard = shard_
				payload.Data = data
				dataToSend, err := json.Marshal(payload)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error marshalling data", "status": "failure"})
					return
				}
				request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s:5000/write", server), bytes.NewReader(dataToSend))
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating write request", "status": "failure"})
					return
				}
				lb.shards[shard_].req_count++
				request.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
				do, err := http.DefaultClient.Do(request)
				for err != nil && do.StatusCode != http.StatusOK {
					// retry
					do, err = http.DefaultClient.Do(request)
				}
				break
			}
		}

	}
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
	for shard_, shardMetaData_ := range lb.shards {
		if !(payload.Stud_id["low"] >= shardMetaData_.Stud_id_high || payload.Stud_id["high"] <= shardMetaData_.Stud_id_low) {
			// acquire shared lock
			shardMetaData_.rw.RLock()

			server := lb.getServerID(shard_)
			fmt.Printf("\nForwarding to %s\n", server)
			readEndpoint := fmt.Sprintf("http://%s:5000/read", server)
			var body readPayload
			body.Shard = shard_
			body.Stud_id = make(map[string]int)
			body.Stud_id["low"] = max(payload.Stud_id["low"], shardMetaData_.Stud_id_low)
			body.Stud_id["high"] = min(payload.Stud_id["high"], shardMetaData_.Stud_id_high)
			// fmt.Printf("\n%v\n", body)
			jsonBody, _ := json.Marshal(body)
			post, err := http.Post(readEndpoint, "application/json", bytes.NewReader(jsonBody))
			// release shared lock
			shardMetaData_.rw.RUnlock()

			if err == nil {
				resp, _ := io.ReadAll(post.Body)
				var respBody readResponse
				err := json.Unmarshal(resp, &respBody)
				if err != nil {
					continue
				}
				response.shards_queried = append(response.shards_queried, shard_)
				response.Data = append(response.Data, respBody.Data...)
			}
		}
	}
	response.Status = "success"
	c.JSON(http.StatusOK, gin.H{"shards_queried": response.shards_queried, "data": response.Data, "status": response.Status})
}

func getallHandler(c *gin.Context) {
	server := c.Param("server_id")
	url := fmt.Sprintf("http://%s:5000/getall", server)
	resp, err := http.Get(url)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting data from server", "status": "failure"})
		return
	}
	// relay the response to the client
	c.JSON(http.StatusOK, resp.Body)
}
