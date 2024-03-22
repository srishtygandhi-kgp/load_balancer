package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type StudT struct {
	Stud_id    int `gorm:"primaryKey"`
	Stud_name  string
	Stud_marks int
}

var db *gorm.DB

func getJSONstring(c *gin.Context) string {
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Println("Error reading request body:", err)
		return "}"
	}
	return string(body)
}

func heartbeatHandler(c *gin.Context) {
	// Respond with an empty body and 200 OK status for heartbeat
	c.JSON(http.StatusOK, gin.H{})
}

type configPayload struct {
	Schema map[string][]string `json:"schema" binding:"required"`
	Shards []string            `json:"shards" binding:"required"`
}

func configHandler(c *gin.Context) {
	//get server name from environment variable
	name := os.Getenv("SERVER_ID")
	// get list of shards from the request
	var payload configPayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}
	var message = ""
	var status = true
	for _, shard := range payload.Shards {
		// create a new table for each shard
		err := db.Table(shard).AutoMigrate(&StudT{})
		if err != nil {
			status = false
			continue
		}
		message += fmt.Sprintf("%s:%s, ", name, shard)
	}
	message = message[:len(message)-2]
	message += "configured"
	if status {
		c.JSON(http.StatusOK, gin.H{"message": message, "status": "success"})
	} else {
		c.JSON(http.StatusInternalServerError, gin.H{"message": message, "status": "failure"})
	}

}

type copyPayload struct {
	Shards []string `json:"shards" binding:"required"`
}

func copyHandler(c *gin.Context) {
	var payload copyPayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}
	//build json payload
	response := gin.H{}
	for _, shard := range payload.Shards {
		// get all the records from the shard
		var studs []StudT
		err := db.Table(shard).Find(&studs).Error
		if err != nil {
			continue
		}
		// write records to response
		response[shard] = studs
	}
	if len(response) == len(payload.Shards) {
		response["status"] = "success"
	} else {
		response["status"] = "failure"
	}
	// send the response
	c.JSON(http.StatusOK, response)
}

type readPayload struct {
	Shard   string         `json:"shard" binding:"required"`
	Stud_id map[string]int `json:"Stud_id" binding:"required"`
}

func readHandler(c *gin.Context) {
	var payload readPayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}
	// get the shard from the request
	shard := payload.Shard
	// get the student id from the request
	low := payload.Stud_id["low"]
	high := payload.Stud_id["high"]
	// get all the records from the shard
	var studs []StudT
	err = db.Table(shard).Where("stud_id >= ? AND stud_id <= ?", low, high).Find(&studs).Error
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// send the response
	c.JSON(http.StatusOK, gin.H{"data": studs, "status": "success"})
}

type writePayload struct {
	Shard    string  `json:"shard" binding:"required"`
	Curr_idx int     `json:"curr_idx" binding:"required"`
	Data     []StudT `json:"data" binding:"required"`
}

func writeHandler(c *gin.Context) {
	var payload writePayload
	jsonData := getJSONstring(c)
	err := json.Unmarshal([]byte(jsonData), &payload)
	fmt.Printf("Payload: %v\n\n%v\n", jsonData, payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		return
	}
	// get the shard from the request
	shard := payload.Shard
	// get the current index from the request
	curr_idx := payload.Curr_idx
	// get the data from the request
	data := payload.Data
	// write the data to the shard
	result := db.Table(shard).Create(&data)
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	curr_idx += int(result.RowsAffected)
	// send the response
	c.JSON(http.StatusOK, gin.H{"message": "Data entries added", "curr_idx": curr_idx, "status": "success"})
}

type updatePayload struct {
	Shard   string `json:"shard" binding:"required"`
	Stud_id int    `json:"Stud_id" binding:"required"`
	Data    StudT  `json:"data" binding:"required"`
}
