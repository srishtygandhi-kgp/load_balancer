package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"sync"
)

var (
	lb        = &loadBalancer{}
	mapdb     = &gorm.DB{}
	addRmLock = &sync.Mutex{}
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
	if lb.shards != nil {
		c.JSON(http.StatusBadRequest, gin.H{"message": "Database already configured", "status": "failure"})
		return
	}
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
	res, err := http.Post("http://shard_manager:5000/init", "application/json", bytes.NewReader([]byte(jsonData)))
	if err != nil || res.StatusCode != http.StatusOK {
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
	var server_list map[string][]string
	server_list = make(map[string][]string)
	for server, shard_list := range lb.server_shard_mapping {
		server_list[server] = make([]string, 0)
		for shard_ := range shard_list {
			server_list[server] = append(server_list[server], shard_)
		}
	}
	c.JSON(http.StatusOK, gin.H{"N": len(server_list), "shards": shard_list, "servers": server_list})
}

func addHandler(c *gin.Context) {
	addRmLock.Lock()
	defer addRmLock.Unlock()
	jsonString := getJSONstring(c)
	var payload addPayload
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	var effectedShards map[string]bool
	effectedShards = make(map[string]bool)
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
		effectedShards[shard_.Shard_id] = true
	}
	for _, shard_list := range payload.Servers {
		for _, shard_ := range shard_list {
			if _, ok := lb.shards[shard_]; !ok {
				c.JSON(http.StatusBadRequest, gin.H{"message": "<Error> Shard not found", "status": "failure"})
				return
			}
			effectedShards[shard_] = true
		}
	}
	// lock effected shards
	for shard_ := range effectedShards {
		lb.shards[shard_].rw.Lock()
		defer lb.shards[shard_].rw.Unlock()
	}
	// send the payload to the shard manager as it is
	// the shard manager will spawn the containers and configure them
	_, err = http.Post("http://shard_manager:5000/add", "application/json", bytes.NewReader([]byte(jsonString)))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error sending request to shard manager", "status": "failure"})
		return
	}
	for server, shard_list := range payload.Servers {
		for _, shard_ := range shard_list {
			lb.insertServer(server, shard_)
		}
	}
	// return OK
	msgStr := "Added Servers "
	for server := range payload.Servers {
		msgStr += server
		msgStr += ", "
	}
	c.JSON(http.StatusOK, gin.H{"N": len(lb.server_shard_mapping), "message": msgStr, "status": "success"})
}

func rmHandler(c *gin.Context) {
	addRmLock.Lock()
	defer addRmLock.Unlock()
	jsonString := getJSONstring(c)
	var payload rmPayload
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	if payload.N == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"message": "<Error> Number of Servers to Remove cannot be zero", "status": "failure"})
		return
	}
	var toRemove map[string]bool
	toRemove = make(map[string]bool)
	for _, server := range payload.Servers {
		toRemove[server] = true
	}
	// sanity check
	if len(toRemove) > payload.N {
		c.JSON(http.StatusBadRequest, gin.H{"message": "<Error> Length of server list is more than removable instances", "status": "failure"})
		return
	}
	if payload.N >= len(lb.server_shard_mapping) {
		c.JSON(http.StatusBadRequest, gin.H{"message": "<Error> Cannot remove all servers", "status": "failure"})
		return
	}
	var shard_list map[string]map[string]bool
	shard_list = make(map[string]map[string]bool)

	var remainingServers map[string]bool
	remainingServers = make(map[string]bool)

	k := payload.N - len(toRemove)
	for server := range lb.server_shard_mapping {
		if k > 0 {
			if _, ok := toRemove[server]; !ok {
				toRemove[server] = false
				payload.Servers = append(payload.Servers, server)
			}
			k--
		}
		if _, ok := toRemove[server]; !ok {
			remainingServers[server] = false
		}
	}
	var addReq addPayload
	addReq.New_shards = make([]shard, 0)
	addReq.Servers = make(map[string][]string)
	addReq.N = 0
	var backupServer string
	// select one of remaining servers to add shards
	for server := range remainingServers {
		backupServer = server
		addReq.Servers[backupServer] = make([]string, 0)
		break
	}
	for _, shard_ := range lb.shards {
		shard_list[shard_.Shard_id] = make(map[string]bool)
		for server := range shard_.servers {
			if _, ok := remainingServers[server]; ok {
				shard_list[shard_.Shard_id][server] = false
			}
		}
		if len(shard_list[shard_.Shard_id]) == 0 {
			addReq.Servers[backupServer] = append(addReq.Servers[backupServer], shard_.Shard_id)
		}
	}
	var effectedShards map[string]bool
	effectedShards = make(map[string]bool)
	for server, shards := range lb.server_shard_mapping {
		if _, ok := toRemove[server]; ok {
			for shard_ := range shards {
				effectedShards[shard_] = true
			}
		}
	}
	for shard_ := range effectedShards {
		lb.shards[shard_].rw.Lock()
		defer lb.shards[shard_].rw.Unlock()
	}
	if len(addReq.Servers[backupServer]) != 0 {
		// send add request to shard manager
		jsonValue, _ := json.Marshal(addReq)
		_, err = http.Post("http://shard_manager:5000/add", "application/json", bytes.NewBuffer(jsonValue))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error sending request to shard manager", "status": "failure"})
			return
		}
		for _, shard_ := range addReq.Servers[backupServer] {
			lb.insertServer(backupServer, shard_)
		}
	}
	//change payload to remove servers
	payload.Servers = make([]string, 0)
	for server := range toRemove {
		payload.Servers = append(payload.Servers, server)
	}

	jsonValue, _ := json.Marshal(payload)
	_, err = http.Post("http://shard_manager:5000/rm", "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error sending request to shard manager", "status": "failure"})
		return
	}
	//remove servers from load balancer
	for shard_ := range effectedShards {
		for _, server := range payload.Servers {
			lb.removeServer(server, shard_)
		}
	}
	for _, server := range payload.Servers {
		delete(lb.server_shard_mapping, server)
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
	mapdb = initDB()

	port := "5000"
	err := r.Run(":" + port)
	if err != nil {
		log.Fatalf("Error starting Load Balancer: %v", err)
	}
}
func initDB() *gorm.DB {
	dsn := "root:abc@tcp(map_db)/map_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	for err != nil {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	}
	return db
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
			// send delete request to primary servers
			var mapT MapT
			err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting primary server", "status": "failure"})
				return
			}
			var reqPayload delPayload
			reqPayload.Shard = shard_
			reqPayload.Stud_id = payload.Stud_id
			// send data to this server
			dataToSend, err := json.Marshal(reqPayload)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error marshalling data", "status": "failure"})
				return
			}
			request, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s:5000/del", mapT.Server_id), bytes.NewReader(dataToSend))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating delete request", "status": "failure"})
				return
			}
			request.Header.Set("Content-Type", "application/json")
			lb.shards[shard_].req_count++
			request.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
			log.Printf("sending delete request to %s\n", mapT.Server_id)
			do, err := http.DefaultClient.Do(request)
			for err != nil || do.StatusCode != http.StatusOK {
				var mapT MapT
				err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting primary server", "status": "failure"})
					return
				}
				request, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://%s:5000/del", mapT.Server_id), bytes.NewReader(dataToSend))
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating delete request", "status": "failure"})
					return
				}
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
				// retry
				log.Printf("retrying delete request to %s\n", mapT.Server_id)
				do, err = http.DefaultClient.Do(request)
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

			var mapT MapT
			err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting primary server", "status": "failure"})
				return
			}
			// send data to this server as put request
			var reqPayload updatePayload
			reqPayload.Shard = shard_
			reqPayload.Stud_id = payload.Stud_id
			reqPayload.Data = payload.Data
			dataToSend, err := json.Marshal(reqPayload)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error marshalling data", "status": "failure"})
				return
			}
			request, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s:5000/update", mapT.Server_id), bytes.NewReader(dataToSend))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating put request", "status": "failure"})
				return
			}
			request.Header.Set("Content-Type", "application/json")
			lb.shards[shard_].req_count++
			request.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
			log.Printf("sending update request to %s\n", mapT.Server_id)
			do, err := http.DefaultClient.Do(request)
			for err != nil || do.StatusCode != http.StatusOK {
				var mapT MapT
				err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting primary server", "status": "failure"})
					return
				}
				request, err := http.NewRequest(http.MethodPut, fmt.Sprintf("http://%s:5000/update", mapT.Server_id), bytes.NewReader(dataToSend))
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating put request", "status": "failure"})
					return
				}
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
				// retry
				log.Printf("retrying update request to %s\n", mapT.Server_id)
				do, err = http.DefaultClient.Do(request)
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

	// send write request to primary servers
	for shard_, data := range dataToWriteToShards {
		log.Printf("Writing to shard %s\n", shard_)
		var mapT MapT
		err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting primary server", "status": "failure"})
			return
		}
		// send data to this server
		var payload writePayload
		payload.Shard = shard_
		payload.Data = data
		dataToSend, err := json.Marshal(payload)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error marshalling data", "status": "failure"})
			return
		}
		request, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s:5000/write", mapT.Server_id), bytes.NewReader(dataToSend))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating write request", "status": "failure"})
			return
		}
		lb.shards[shard_].req_count++
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
		log.Printf("sending write request to %s\n", mapT.Server_id)
		do, err := http.DefaultClient.Do(request)
		for err != nil || do.StatusCode != http.StatusOK {
			var mapT MapT
			err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting primary server", "status": "failure"})
				return
			}
			request, err = http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s:5000/write", mapT.Server_id), bytes.NewReader(dataToSend))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating write request", "status": "failure"})
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Request-Count", strconv.Itoa(lb.shards[shard_].req_count))
			// retry
			log.Printf("retrying write request to %s\n", mapT.Server_id)
			do, err = http.DefaultClient.Do(request)
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
	response.Status = ""
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
			} else {
				// add failed shard to response.status
				response.Status += shard_ + " "
			}
		}
	}
	if response.Status == "" {
		response.Status = "success"
	} else {
		response.Status += "failed"
	}
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
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error closing response body", "status": "failure"})
			return
		}
	}(resp.Body)

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error reading server response", "status": "failure"})
		return
	}

	var data interface{}
	err = json.Unmarshal(bodyBytes, &data)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error parsing server response", "status": "failure"})
		return
	}

	// relay the response to the client
	c.JSON(http.StatusOK, data)
}
