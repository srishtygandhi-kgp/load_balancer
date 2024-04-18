package main

import (
	"math/rand"
	"sync"
	"time"
)

type loadBalancer struct {
	shards               map[string]*shardMetaData
	server_shard_mapping map[string]map[string]bool
}
type shardMetaData struct {
	Stud_id_low  int
	Stud_id_high int
	Shard_id     string
	Shard_size   int
	req_count    int
	hashmap      *[M]string
	servers      map[string]bool
	rw           *sync.RWMutex
}

const (
	M   = 512
	K   = 9
	__p = 31
	__m = 1000000007
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

// method to insert server
func (lb *loadBalancer) insertServer(server string, shard_id string) {
	if _, ok := lb.shards[shard_id].servers[server]; ok {
		return
	}
	//add server to shard
	lb.shards[shard_id].servers[server] = false
	//add server to mapping
	if _, ok := lb.server_shard_mapping[server]; !ok {
		if lb.server_shard_mapping == nil {
			lb.server_shard_mapping = make(map[string]map[string]bool)
		}
		lb.server_shard_mapping[server] = make(map[string]bool)
	}
	lb.server_shard_mapping[server][shard_id] = false
	// fit into hashmap
	i := stringHash(server)
	for j := 0; j < K; j++ {
		pos := Phi(uint32(i), uint32(j)) % M
		// linear probing
		probe := pos
		if lb.shards[shard_id].hashmap[pos] != "" {
			probe++
			for probe != pos {
				if lb.shards[shard_id].hashmap[probe] == "" {
					lb.shards[shard_id].hashmap[probe] = server
					break
				}
				probe++
				probe %= M
			}

		} else {
			lb.shards[shard_id].hashmap[probe] = server
		}
	}
}

// method to remove server
func (lb *loadBalancer) removeServer(server string, shard_id string) {
	if _, ok := lb.shards[shard_id].servers[server]; !ok {
		return
	}
	// remove server from shard
	delete(lb.shards[shard_id].servers, server)
	// remove shard from server
	delete(lb.server_shard_mapping[server], shard_id)
	// fit into hashmap
	i := stringHash(server)
	for j := 0; j < K; j++ {
		pos := Phi(uint32(i), uint32(j)) % M
		// linear probing
		probe := pos
		if lb.shards[shard_id].hashmap[pos] == server {
			lb.shards[shard_id].hashmap[pos] = ""
		} else {
			probe++
			for probe != pos {
				if lb.shards[shard_id].hashmap[probe] == server {
					lb.shards[shard_id].hashmap[probe] = ""
					break
				}
				probe++
				probe %= M
			}
		}
	}
}

func (lb *loadBalancer) getServerID(shard_id string) string {
	// generate random number
	rand.Seed(time.Now().UnixNano())
	i := rand.Intn(512)
	for {
		if lb.shards[shard_id].hashmap[i] != "" {
			return lb.shards[shard_id].hashmap[i]
		}
		i++
		i %= M
	}
}
