package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	db                   *gorm.DB
	mapdb                *gorm.DB
	g_shard_log_map      = make(map[string]*LogT)
	g_shard_logIndex_map = make(map[string]*int)
	indexLock            = &sync.Mutex{}
)

func getJSONstring(c *gin.Context) string {
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Printf("Error reading request body:", err)
		return "{}"
	}
	return string(body)
}

func writeToLog(logItem logPayload, shard_ string) error {
	// convert logItem to a string
	jsonData, err := json.Marshal(logItem)
	if err != nil {
		log.Fatalln("Error marshalling logItem to JSON:", err)
		return err
	}
	jsonData = append(jsonData, []byte("\n")...)
	// write logItem to the log file
	err = g_shard_log_map[shard_].Write(jsonData)
	if err != nil {
		log.Fatalln("Error writing logItem to log file:", err)
		return err
	}
	return nil
}

func executeFromLog(shard_ string) error {
	logData := g_shard_log_map[shard_].data
	idx := g_shard_logIndex_map[shard_]
	// split the logData by \n
	logItems := strings.Split(string(logData), "\n")
	if logItems[len(logItems)-1] == "" {
		logItems = logItems[:len(logItems)-1]
	}
	for *idx < len(logItems) {
		var logItem logPayload
		err := json.Unmarshal([]byte(logItems[*idx]), &logItem)
		if err != nil {
			log.Fatalf("\nError unmarshalling log item: %v\n", err)
			return err
		}
		if logItem.Operation == "w" {
			result := db.Table(shard_).Create(&logItem.W_Data)
			if result.Error != nil {
				log.Fatalf("\nError performing write from log item: %v\n", result.Error)
				return err
			}
			*idx = *idx + 1
		}
		if logItem.Operation == "u" {
			result := db.Table(shard_).Where("Stud_id = ?", logItem.UD_Stud_id).Updates(&logItem.U_Data)
			if result.Error != nil {
				log.Fatalf("\nError performing update from log item: %v\n", result.Error)
				return err
			}
			*idx = *idx + 1
		}
		if logItem.Operation == "d" {
			result := db.Table(shard_).Where("Stud_id = ?", logItem.UD_Stud_id).Delete(&StudT{})
			if result.Error != nil {
				log.Fatalf("\nError performing delete from log item: %v\n", result.Error)
				return err
			}
			*idx = *idx + 1
		}
	}
	*idx = len(logItems)
	return nil
}

func heartbeatHandler(c *gin.Context) {
	// Respond with an empty body and 200 OK status for heartbeat
	c.JSON(http.StatusOK, gin.H{})
}

// have to create log files here
func configHandler(c *gin.Context) {
	indexLock.Lock()
	defer indexLock.Unlock()
	//get server name from environment variable
	name := os.Getenv("SERVER_ID")
	// get list of shards from the request
	var payload configPayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		log.Printf("Error decoding JSON:", err)
		return
	}
	var message = ""
	for _, shard_ := range payload.Shards {
		// create a new table for each shard
		err := db.Table(shard_).AutoMigrate(&StudT{})
		g_shard_log_map[shard_] = new(LogT)
		g_shard_log_map[shard_].file, err = os.OpenFile(fmt.Sprintf("/data/%s.log", shard_), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
		g_shard_log_map[shard_].data = []byte{}
		g_shard_logIndex_map[shard_] = new(int)
		if err != nil {
			log.Printf("Error opening log file for shard %s:%v", shard_, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// get primary server
		var mapT MapT
		err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			log.Printf("Error getting primary server for shard %s:%v", shard_, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if err == nil {
			// get data from primary server
			readEndpoint := fmt.Sprintf("http://%s:5000/copy", mapT.Server_id)
			jsonBody, _ := json.Marshal(copyPayload{Shard: shard_})
			get, err := http.NewRequest(http.MethodGet, readEndpoint, bytes.NewReader(jsonBody))
			if err != nil {
				log.Printf("Error making request for getting data from primary server for shard %s:%v", shard_, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			res, err := http.DefaultClient.Do(get)
			if err != nil {
				log.Printf("Error getting data from primary server for shard %s:%v", shard_, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			// write data to the shard
			var response struct {
				Data []StudT
				Logs []logPayload
			}
			err = json.NewDecoder(res.Body).Decode(&response)
			if err != nil {
				log.Printf("Error decoding data from primary server for shard %s:%v", shard_, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			db.Table(shard_).Create(&response.Data)
			// write log file to the shard
			for _, logItem := range response.Logs {
				err := writeToLog(logItem, shard_)
				if err != nil {
					log.Printf("Error writing to log for shard %s:%v", shard_, err)
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
					return
				}
			}
			commitsDoneTill := res.Header.Get("Commit-Index")
			*g_shard_logIndex_map[shard_], err = strconv.Atoi(commitsDoneTill)
			if err != nil {
				log.Printf("Error converting commit index to int for shard %s:%v", shard_, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			err = executeFromLog(shard_)
			if err != nil {
				log.Printf("Error executing from log for shard %s:%v", shard_, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}
		message += fmt.Sprintf("%s:%s, ", name, shard_)
	}
	message = message[:len(message)-2]
	message += "configured"
	c.JSON(http.StatusOK, gin.H{"message": message, "status": "success"})
}

// copy sends the logs as well as the data to the secondary servers
// change copy to handle single shard instead of shard array as we are not using that functionality

func copyHandler(c *gin.Context) {
	var payload copyPayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		log.Printf("Error decoding JSON: %v", err)
		return
	}
	// get the shard from the request
	shard_ := payload.Shard
	if g_shard_log_map[shard_] == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Shard does not exist"})
		return
	}
	// get all the records from the shard
	var studs []StudT
	indexLock.Lock()
	defer indexLock.Unlock()
	err = db.Table(shard_).Find(&studs).Error
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	logData := g_shard_log_map[shard_].data
	//split the logData by \n
	logItems := strings.Split(string(logData), "\n")
	if logItems[len(logItems)-1] == "" {
		logItems = logItems[:len(logItems)-1]
	}
	var logs []logPayload
	for _, logItem := range logItems {
		var logItemStruct logPayload
		err = json.Unmarshal([]byte(logItem), &logItemStruct)
		if err != nil {
			log.Printf("Error unmarshalling log item: %v\n", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		logs = append(logs, logItemStruct)
	}
	c.Header("Commit-Index", fmt.Sprintf("%v", *g_shard_logIndex_map[shard_]))
	// send the response
	c.JSON(http.StatusOK, gin.H{"Data": studs, "Logs": logs})
}

func readHandler(c *gin.Context) {
	var payload readPayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		log.Printf("Error decoding JSON:%v", err)
		return
	}
	// get the shard from the request
	shard_ := payload.Shard
	// get the student id from the request
	low := payload.Stud_id["low"]
	high := payload.Stud_id["high"]
	// get all the records from the shard
	var studs []StudT
	err = db.Table(shard_).Where("stud_id >= ? AND stud_id <= ?", low, high).Find(&studs).Error
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// send the response
	c.JSON(http.StatusOK, gin.H{"data": studs, "status": "success"})
}

func writeHandler(c *gin.Context) {

	var payload writePayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	log.Printf("Payload: %v\n\n%v\n", jsonData, payload)
	if err != nil {
		log.Printf("Error decoding JSON:%v", err)
		return
	}
	// get the shard from the request
	shard_ := payload.Shard
	// get the data from the request
	data := payload.Data

	// implement write ahead logging
	var logItem logPayload
	logItem.Operation = "w"
	logItem.W_Data = data
	indexLock.Lock()
	//get number of entries in log
	logItems := strings.Split(string(g_shard_log_map[shard_].data), "\n")
	if logItems[len(logItems)-1] == "" {
		logItems = logItems[:len(logItems)-1]
	}
	//get Request-Count from header
	parseInt, err := strconv.Atoi(c.GetHeader("Request-Count"))
	if err != nil {
		log.Printf("Error converting Request-Count to int:%v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		indexLock.Unlock()
		return
	}
	if parseInt == len(logItems) {
		log.Printf("Old Request %v for %s\n", parseInt, shard_)
		c.JSON(http.StatusOK, gin.H{"message": "Data entries added", "status": "success"})
		indexLock.Unlock()
		return
	}
	err = writeToLog(logItem, shard_)
	indexLock.Unlock()
	if err != nil {
		log.Printf("Error writing to log for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// check if server is primary
	var mapTs []MapT
	err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Find(&mapTs).Error
	if err != nil {
		log.Printf("Error getting primary server for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var shard_server_map = make(map[string]map[string]bool)
	for _, mapT := range mapTs {
		if _, ok := shard_server_map[mapT.Shard_id]; !ok {
			shard_server_map[mapT.Shard_id] = make(map[string]bool)
		}
		shard_server_map[mapT.Shard_id][mapT.Server_id] = mapT.Primary
	}
	if shard_server_map[shard_][os.Getenv("SERVER_ID")] {
		// send write request to secondary servers
		// get list of secondary servers
		log.Printf("Forwarding to secondary servers for shard:%s\n", shard_)
		// send write request to secondary servers
		for _, mapT := range mapTs {
			if shard_server_map[mapT.Shard_id][mapT.Server_id] {
				continue
			}
			// send write request to secondary server

			fmt.Printf("\nForwarding to %s\n", mapT.Server_id)
			writeEndpoint := fmt.Sprintf("http://%s:5000/write", mapT.Server_id)
			request, err := http.NewRequest(http.MethodPost, writeEndpoint, bytes.NewReader([]byte(jsonData)))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating write request", "status": "failure"})
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Request-Count", c.GetHeader("Request-Count"))
			res, err := http.DefaultClient.Do(request)
			for err != nil || res.StatusCode != http.StatusOK {
				res, err = http.DefaultClient.Do(request)
			}
		}
	}
	indexLock.Lock()
	defer indexLock.Unlock()
	err = executeFromLog(shard_)
	if err != nil {
		log.Printf("Error executing from log for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// send the response
	c.JSON(http.StatusOK, gin.H{"message": "Data entries added", "status": "success"})
}

func updateHandler(c *gin.Context) {
	var payload updatePayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		log.Printf("Error decoding JSON:", err)
		return
	}
	// get the shard from the request
	shard_ := payload.Shard
	// get the student id from the request
	Stud_id := payload.Stud_id
	// get the data from the request
	data := payload.Data

	// implement write ahead logging
	var logItem logPayload
	logItem.Operation = "u"
	logItem.UD_Stud_id = Stud_id
	logItem.U_Data = data
	indexLock.Lock()
	//get number of entries in log
	logItems := strings.Split(string(g_shard_log_map[shard_].data), "\n")
	if logItems[len(logItems)-1] == "" {
		logItems = logItems[:len(logItems)-1]
	}
	//get Request-Count from header
	parseInt, err := strconv.Atoi(c.GetHeader("Request-Count"))
	if err != nil {
		log.Printf("Error converting Request-Count to int:%v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		indexLock.Unlock()
		return
	}
	if parseInt == len(logItems) {
		log.Printf("Old Request %v for %s\n", parseInt, shard_)
		c.JSON(http.StatusOK, gin.H{"message": "Data entries added", "status": "success"})
		indexLock.Unlock()
		return
	}
	err = writeToLog(logItem, shard_)
	indexLock.Unlock()
	if err != nil {
		log.Printf("Error writing to log for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// check if server is primary
	var mapTs []MapT
	err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Find(&mapTs).Error
	if err != nil {
		log.Printf("Error getting primary server for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var shard_server_map = make(map[string]map[string]bool)
	for _, mapT := range mapTs {
		if _, ok := shard_server_map[mapT.Shard_id]; !ok {
			shard_server_map[mapT.Shard_id] = make(map[string]bool)
		}
		shard_server_map[mapT.Shard_id][mapT.Server_id] = mapT.Primary
	}
	if shard_server_map[shard_][os.Getenv("SERVER_ID")] {
		// send write request to secondary servers
		// get list of secondary servers
		log.Printf("Forwarding to secondary servers for shard:%s\n", shard_)
		// send write request to secondary servers
		for _, mapT := range mapTs {
			if shard_server_map[mapT.Shard_id][mapT.Server_id] {
				continue
			}
			// send update request to secondary server
			fmt.Printf("\nForwarding to %s\n", mapT.Server_id)
			updateEndpoint := fmt.Sprintf("http://%s:5000/update", mapT.Server_id)
			request, err := http.NewRequest(http.MethodPut, updateEndpoint, bytes.NewReader([]byte(jsonData)))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating put request", "status": "failure"})
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Request-Count", c.GetHeader("Request-Count"))
			do, err := http.DefaultClient.Do(request)
			for err != nil || do.StatusCode != http.StatusOK {
				do, err = http.DefaultClient.Do(request)
			}
		}
	}
	indexLock.Lock()
	defer indexLock.Unlock()
	err = executeFromLog(shard_)
	if err != nil {
		log.Printf("Error executing from log for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// send the response
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Data entry for Stud_id:%d updated", Stud_id), "status": "success"})
}

type delPayload struct {
	Shard   string `json:"shard" binding:"required"`
	Stud_id int    `json:"Stud_id" binding:"required"`
}

func delHandler(c *gin.Context) {
	var payload delPayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		log.Printf("Error decoding JSON:", err)
		return
	}
	// get the shard from the request
	shard_ := payload.Shard
	// get the student id from the request
	Stud_id := payload.Stud_id

	// implement write ahead logging
	var logItem logPayload
	logItem.Operation = "d"
	logItem.UD_Stud_id = Stud_id
	indexLock.Lock()
	//get number of entries in log
	logItems := strings.Split(string(g_shard_log_map[shard_].data), "\n")
	if logItems[len(logItems)-1] == "" {
		logItems = logItems[:len(logItems)-1]
	}
	//get Request-Count from header
	parseInt, err := strconv.Atoi(c.GetHeader("Request-Count"))
	if err != nil {
		log.Printf("Error converting Request-Count to int:%v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		indexLock.Unlock()
		return
	}
	if parseInt == len(logItems) {
		log.Printf("Old Request %v for %s\n", parseInt, shard_)
		c.JSON(http.StatusOK, gin.H{"message": "Data entries added", "status": "success"})
		indexLock.Unlock()
		return
	}
	err = writeToLog(logItem, shard_)
	indexLock.Unlock()
	if err != nil {
		log.Printf("Error writing to log for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// check if server is primary
	var mapTs []MapT
	err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Find(&mapTs).Error
	if err != nil {
		log.Printf("Error getting primary server for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var shard_server_map = make(map[string]map[string]bool)
	for _, mapT := range mapTs {
		if _, ok := shard_server_map[mapT.Shard_id]; !ok {
			shard_server_map[mapT.Shard_id] = make(map[string]bool)
		}
		shard_server_map[mapT.Shard_id][mapT.Server_id] = mapT.Primary
	}
	if shard_server_map[shard_][os.Getenv("SERVER_ID")] {
		// send write request to secondary servers
		// get list of secondary servers
		log.Printf("Forwarding to secondary servers for shard:%s\n", shard_)
		// send write request to secondary servers
		for _, mapT := range mapTs {
			if shard_server_map[mapT.Shard_id][mapT.Server_id] {
				continue
			}
			fmt.Printf("\nForwarding to %s\n", mapT.Server_id)
			delEndpoint := fmt.Sprintf("http://%s:5000/del", mapT.Server_id)

			request, err := http.NewRequest(http.MethodDelete, delEndpoint, bytes.NewReader([]byte(jsonData)))
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating put request", "status": "failure"})
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Request-Count", c.GetHeader("Request-Count"))
			do, err := http.DefaultClient.Do(request)
			for err != nil || do.StatusCode != http.StatusOK {
				do, err = http.DefaultClient.Do(request)
			}
		}
	}
	indexLock.Lock()
	defer indexLock.Unlock()
	err = executeFromLog(shard_)
	if err != nil {
		log.Printf("Error executing from log for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// send the response
	c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Data entry with Stud_id:%d removed", Stud_id), "status": "success"})
}

func lenLogHandler(c *gin.Context) {
	var payload struct {
		Shard string
	}
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		log.Printf("Error decoding JSON:", err)
		return
	}
	// get logs for the shard
	var logItems []string
	indexLock.Lock()
	defer indexLock.Unlock()
	logItems = strings.Split(string(g_shard_log_map[payload.Shard].data), "\n")
	if logItems[len(logItems)-1] == "" {
		logItems = logItems[:len(logItems)-1]
	}
	// send the response
	c.JSON(http.StatusOK, gin.H{"Length": len(logItems), "status": "success"})
}

func main() {
	r := gin.Default()

	r.GET("/heartbeat", heartbeatHandler)
	r.POST("/config", configHandler)
	r.GET("/copy", copyHandler)
	r.POST("/read", readHandler)
	r.POST("/write", writeHandler)
	r.PUT("/update", updateHandler)
	r.DELETE("/del", delHandler)
	r.POST("/lenlog", lenLogHandler)
	r.POST("/add", addHandler)
	r.GET("/getall", getAllHandler)

	mapdb = initDB()

	// Connect to the database
	dsn := "root:abc@tcp(localhost)/"
	database, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	for {
		if err == nil {
			break
		}
		database, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	}
	_ = database.Exec("CREATE DATABASE IF NOT EXISTS assign3")
	dsn += "assign3?charset=utf8mb4&parseTime=True&loc=Local"
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Printf("Error connecting to database:", err)
		return
	}

	port := 5000
	addr := fmt.Sprintf(":%d", port)

	log.Printf("Server is running on http://localhost:%d\n", port)
	err = r.Run(addr)
	if err != nil {
		return
	}
}
func initDB() *gorm.DB {
	dsn := "root:abc@tcp(map_db:3306)/map_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	return db
}

func getAllHandler(c *gin.Context) {
	response := gin.H{}
	for shard_ := range g_shard_log_map {
		indexLock.Lock()
		err := executeFromLog(shard_)
		indexLock.Unlock()
		if err != nil {
			log.Printf("Error executing from log for shard %s:%v", shard_, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// get all the records from the shard
		var studs []StudT
		err = db.Table(shard_).Find(&studs).Error
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		response[shard_] = studs
	}
	c.JSON(http.StatusOK, response)
}

func addHandler(c *gin.Context) {
	var payload struct {
		Shard string
	}
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		log.Printf("Error decoding JSON:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// get the shard from the request
	shard_ := payload.Shard
	if shard_ == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Shard not provided"})
		return
	}
	if _, ok := g_shard_log_map[shard_]; ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Shard already exists"})
		return
	}
	// create a new table for each shard
	err = db.Table(shard_).AutoMigrate(&StudT{})
	g_shard_log_map[shard_].file, err = os.OpenFile(fmt.Sprintf("/data/%s.log", shard_), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	g_shard_log_map[shard_].data = []byte{}
	g_shard_logIndex_map[shard_] = new(int)
	if err != nil {
		log.Printf("Error opening log file for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// get primary server
	var mapT MapT
	err = mapdb.Model(&MapT{}).Where("shard_id = ?", shard_).Not("primary", false).First(&mapT).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		log.Printf("Error getting primary server for shard %s:%v", shard_, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err == nil {
		//copy data from primary
		readEndpoint := fmt.Sprintf("http://%s:5000/copy", mapT.Server_id)
		jsonBody, _ := json.Marshal(copyPayload{Shard: shard_})
		get, err := http.NewRequest(http.MethodGet, readEndpoint, bytes.NewReader(jsonBody))
		if err != nil {
			log.Printf("Error making request for getting data from primary server for shard %s:%v", shard_, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		res, err := http.DefaultClient.Do(get)
		if err != nil {
			log.Printf("Error getting data from primary server for shard %s:%v", shard_, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		// write data to the shard
		var response struct {
			Data []StudT
			Logs []logPayload
		}
		err = json.NewDecoder(res.Body).Decode(&response)
		if err != nil {
			log.Printf("Error decoding data from primary server for shard %s:%v", shard_, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		db.Table(shard_).Create(&response.Data)
		// write log file to the shard
		for _, logItem := range response.Logs {
			err := writeToLog(logItem, shard_)
			if err != nil {
				log.Printf("Error writing to log for shard %s:%v", shard_, err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		}
		commitsDoneTill := res.Header.Get("Commit-Index")
		*g_shard_logIndex_map[shard_], err = strconv.Atoi(commitsDoneTill)
		err = executeFromLog(shard_)
		if err != nil {
			log.Printf("Error executing from log for shard %s:%v", shard_, err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return

		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "Shard added", "status": "success"})
}
