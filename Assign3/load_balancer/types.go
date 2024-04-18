package main

type shard struct {
	Stud_id_low int
	Shard_id    string
	Shard_size  int
}

type addPayload struct {
	N          int
	New_shards []shard
	Servers    map[string][]string
}

type rmPayload struct {
	N       int
	Servers []string
}

type student struct {
	Stud_id    int
	Stud_name  string
	Stud_marks int
}

type MapT struct {
	Shard_id  string
	Server_id string
	Primary   bool
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
type writePayload struct {
	Shard string    `json:"shard" binding:"required"`
	Data  []student `json:"data" binding:"required"`
}

type updatePayload struct {
	Shard   string  `json:"shard" binding:"required"`
	Stud_id int     `json:"Stud_id" binding:"required"`
	Data    student `json:"data" binding:"required"`
}

type delPayload struct {
	Shard   string `json:"shard" binding:"required"`
	Stud_id int    `json:"Stud_id" binding:"required"`
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
