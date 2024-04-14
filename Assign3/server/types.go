package main

type StudT struct {
	Stud_id    int `gorm:"primaryKey"`
	Stud_name  string
	Stud_marks int
}

type MapT struct {
	Shard_id  string
	Server_id string
	Primary   bool
}

type configPayload struct {
	Shards   []string                   `json:"shards" binding:"required"`
	Map_data map[string]map[string]bool `json:"map_data" binding:"required"`
}

type readPayload struct {
	Shard   string         `json:"shard" binding:"required"`
	Stud_id map[string]int `json:"Stud_id" binding:"required"`
}

type copyPayload struct {
	Shard string `json:"shard" binding:"required"`
}
type writePayload struct {
	Shard string  `json:"shard" binding:"required"`
	Data  []StudT `json:"data" binding:"required"`
}

type updatePayload struct {
	Shard   string `json:"shard" binding:"required"`
	Stud_id int    `json:"Stud_id" binding:"required"`
	Data    StudT  `json:"data" binding:"required"`
}

type logPayload struct {
	Operation  string
	W_Data     []StudT
	UD_Stud_id int
	U_Data     StudT
}
