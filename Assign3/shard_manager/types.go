package main

import (
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"log"
	"sync"
)

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
type MapT struct {
	Shard_id  string
	Server_id string
	Primary   bool
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

type configPayload struct {
	Shards []string `json:"shards" binding:"required"`
}

type initPayload struct {
	N       int
	Shards  []shard
	Servers map[string][]string
}
type readPayload struct {
	Shard   string         `json:"shard" binding:"required"`
	Stud_id map[string]int `json:"Stud_id" binding:"required"`
}

type readResponse struct {
	Data   []student
	Status string
}

func getJSONstring(c *gin.Context) string {
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		log.Println("Error reading request body:", err)
		return "}"
	}
	return string(body)
}
