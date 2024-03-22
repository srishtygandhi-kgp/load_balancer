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