package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-co-op/gocron"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"net/http"
	"os/exec"
	"sync"
	"time"
)

var (
	mapdb             = &gorm.DB{}
	active_containers = make(map[string]bool)
	heartRmLock       = &sync.Mutex{}
)

func spawnContainer(server string) error {
	cmd := exec.Command("docker", "rm", "-f", server)
	err := cmd.Run()
	if err != nil {
		return err
	}
	cmd = exec.Command("docker", "run", "-d", "--name", server, "-p", ":5000", "--network", "assign3_net1", "--network-alias", server, "-e", fmt.Sprintf("SERVER_ID=%s", server), "server_image")
	err = cmd.Run()
	if err != nil {
		log.Printf("Error spawning new server container for %s: %v", server, err)
		return err
	}
	active_containers[server] = true
	fmt.Printf("\n%v %s spawned successfully. \n", time.Now(), server)
	return nil
}

func removeContainer(server string) error {
	cmd := exec.Command("docker", "rm", "-f", server)
	err := cmd.Run()
	if err != nil {
		log.Printf("Error removing server container for %s: %v", server, err)
		return err
	}
	fmt.Printf("\n%s removed successfully. \n", server)

	delete(active_containers, server)
	return nil
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
	var spawned []string
	for server := range payload.Servers {
		err := spawnContainer(server)
		if err != nil {
			log.Printf("%v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error spawning new servers", "status": "failure"})
			return
		}
		spawned = append(spawned, server)
	}
	for _, server := range spawned {
		shards := payload.Servers[server]
		var body configPayload
		body.Shards = shards
		jsonBody, err := json.Marshal(body)
		configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
		post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		for {
			if err == nil {
				break
			}
			post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		}
		if post.StatusCode != http.StatusOK {
			log.Printf("\nError configuring server: %v, %v\n", server, err)
		}
		var mapTs []MapT
		for _, shard_ := range shards {
			mapTs = append(mapTs, MapT{Shard_id: shard_, Server_id: server, Primary: false})
		}
		err = mapdb.Create(&mapTs).Error
		if err != nil {
			log.Printf("Error adding shards to server %s: %v", server, err)
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error adding shards to new servers", "status": "failure"})
			return
		}
	}
	for _, shard_ := range payload.Shards {
		reElect(shard_.Shard_id)
	}

	// return OK
	c.JSON(http.StatusOK, gin.H{"message": "Configured Database", "status": "success"})
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
	log.Printf("add Payload: %v", payload)
	var spawned map[string]configPayload
	spawned = make(map[string]configPayload)
	for server, shards := range payload.Servers {
		if _, ok := active_containers[server]; !ok {
			err := spawnContainer(server)
			if err != nil {
				log.Printf("%v", err)
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error spawning new servers", "status": "failure"})
				return
			}
			spawned[server] = configPayload{Shards: shards}
		} else {
			for _, shard_ := range shards {
				// check if shard already exists for server
				var mapT MapT
				err := mapdb.Where("shard_id = ? AND server_id = ?", shard_, server).First(&mapT).Error
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					log.Printf("Error getting shard %s for server %s: %v", shard_, server, err)
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error adding shard to new servers", "status": "failure"})
					return
				}
				if err != nil {
					res, err := http.Post(fmt.Sprintf("http://%s:5000/add", server), "application/json", bytes.NewBuffer([]byte(fmt.Sprintf(`{"shard": "%s"}`, shard_))))
					if err != nil || res.StatusCode != http.StatusOK {
						log.Printf("Error adding shard %s to server %s: %v", shard_, server, err)
						c.JSON(http.StatusInternalServerError, gin.H{"message": "Error adding shard to new servers", "status": "failure"})
						return
					}
					err = mapdb.Create(&MapT{Shard_id: shard_, Server_id: server, Primary: false}).Error
					if err != nil {
						log.Printf("Error adding shard %s to server %s: %v", shard_, server, err)
						c.JSON(http.StatusInternalServerError, gin.H{"message": "Error adding shard to new servers", "status": "failure"})
						return
					}
				}
			}
		}
	}
	for server, body := range spawned {
		jsonBody, err := json.Marshal(body)
		configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
		post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		for {
			if err == nil {
				break
			}
			post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		}
		if post.StatusCode != http.StatusOK {
			log.Printf("Error configuring server %s: %v", server, err)
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error configuring server", "status": "failure"})
			return
		}
		var mapTs []MapT
		for _, shard_ := range body.Shards {
			mapTs = append(mapTs, MapT{Shard_id: shard_, Server_id: server, Primary: false})
		}
		err = mapdb.Create(&mapTs).Error
		if err != nil {
			log.Printf("error adding shards to new server %s : %v", server, err)
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error adding shards to new servers", "status": "failure"})
			return
		}
	}
	for _, shard_ := range payload.New_shards {
		reElect(shard_.Shard_id)
	}

	c.JSON(http.StatusOK, gin.H{"message": "Added new servers", "status": "success"})
}

func rmHandler(c *gin.Context) {
	jsonString := getJSONstring(c)
	var payload struct {
		N       int
		Servers []string
	}
	err := json.Unmarshal([]byte(jsonString), &payload)
	if err != nil {
		fmt.Println("Error decoding JSON:", err)
		c.JSON(http.StatusBadRequest, gin.H{"message": "Error decoding JSON", "status": "failure"})
		return
	}
	heartRmLock.Lock()
	err = mapdb.Where("server_id IN ?", payload.Servers).Delete(&MapT{}).Error
	if err != nil {
		heartRmLock.Unlock()
		log.Printf("Error removing servers: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error removing servers", "status": "failure"})
		return
	}
	heartRmLock.Unlock()
	// remove servers
	for _, server := range payload.Servers {
		err := removeContainer(server)
		if err != nil {
			log.Printf("%v", err)
			continue
		}
	}
	// re-elect primary servers if necessary
	var mapTs []MapT
	err = mapdb.Find(&mapTs).Error
	if err != nil {
		log.Printf("Error getting servers: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "Error getting servers", "status": "failure"})
		return
	}
	// convert to map
	var shardServerMap = make(map[string]map[string]bool)
	for _, mapT := range mapTs {
		if shardServerMap[mapT.Shard_id] == nil {
			shardServerMap[mapT.Shard_id] = make(map[string]bool)
		}
		shardServerMap[mapT.Shard_id][mapT.Server_id] = mapT.Primary

	}
	for shard_, servers := range shardServerMap {
		noPrimary := true
		for server := range servers {
			if servers[server] {
				noPrimary = false
				break
			}
		}
		if noPrimary {
			reElect(shard_)
		}
	}
	c.JSON(http.StatusOK, gin.H{"message": "Removed servers", "status": "success"})
}

func reElect(shard string) {
	mostUpdated := ""
	longestLog := -1
	//get list of servers in shard
	var mapTs []MapT
	err := mapdb.Where("shard_id = ?", shard).Find(&mapTs).Error
	if err != nil {
		log.Printf("Error getting servers for shard %s: %v", shard, err)
		return
	}

	for _, mapT := range mapTs {
		server := mapT.Server_id
		// get log length
		jsonValue, _ := json.Marshal(gin.H{"shard": shard})
		res, err := http.Post(fmt.Sprintf("http://%s:5000/lenlog", server), "application/json", bytes.NewBuffer(jsonValue))
		if err != nil || res.StatusCode != http.StatusOK {
			log.Printf("Error getting log length from %s: %v", server, err)
			continue
		}
		// read response
		var resStruct struct {
			Length int
		}
		err = json.NewDecoder(res.Body).Decode(&resStruct)
		if err != nil {
			log.Printf("Error decoding response from %s: %v", server, err)
			continue
		}
		if resStruct.Length > longestLog {
			longestLog = resStruct.Length
			mostUpdated = server
		}
	}
	if mostUpdated == "" {
		log.Printf("No servers found for shard %s", shard)
		return
	}
	// set primary
	err = mapdb.Transaction(func(tx *gorm.DB) error {
		err := tx.Model(&MapT{}).Where("shard_id = ?", shard).Update("primary", false).Error
		if err != nil {
			log.Printf("Error updating primary for shard %s: %v", shard, err)
			return err
		}
		err = tx.Model(&MapT{}).Where("shard_id = ? AND server_id = ?", shard, mostUpdated).Update("primary", true).Error
		if err != nil {
			log.Printf("Error updating primary for shard %s: %v", shard, err)
			return err
		}
		return nil
	})
	if err != nil {
		log.Printf("Error updating primary for shard %s: %v", shard, err)
		return
	}
	fmt.Printf("\n%s elected as primary for shard %s\n", mostUpdated, shard)
}

func main() {

	r := gin.Default()

	r.POST("/init", initHandler)
	r.POST("/add", addHandler)
	r.POST("/rm", rmHandler)
	mapdb = initDB()

	//check heartbeat and respawn if needed
	s := gocron.NewScheduler(time.UTC)
	_, err := s.Every(5).Seconds().SingletonMode().Do(checkHeartbeat)
	if err != nil {
		return
	}
	s.StartAsync()

	port := "5000"
	err = r.Run(":" + port)
	if err != nil {
		log.Fatalf("Error starting Shard Manager: %v", err)
	}
}

func initDB() *gorm.DB {
	dsn := "root:abc@tcp(map_db)/map_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	for err != nil {
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	}
	err = db.AutoMigrate(&MapT{})
	if err != nil {
		log.Fatalf("Error migrating database: %v", err)
	}
	return db
}

func checkHeartbeat() {
	heartRmLock.Lock()
	defer heartRmLock.Unlock()
	server_shard_mapping := make(map[string]map[string]bool)
	var mapTs []MapT
	err := mapdb.Find(&mapTs).Error
	if err != nil {
		log.Printf("Error getting servers: %v", err)
		return
	}

	shardList := make(map[string]bool)
	for _, mapT := range mapTs {
		if server_shard_mapping[mapT.Server_id] == nil {
			server_shard_mapping[mapT.Server_id] = make(map[string]bool)
		}
		server_shard_mapping[mapT.Server_id][mapT.Shard_id] = mapT.Primary
		shardList[mapT.Shard_id] = true
	}
	var spawned map[string]configPayload
	spawned = make(map[string]configPayload)
	// get heartbeat from all servers and store unresponsive servers
	for server := range server_shard_mapping {
		_, err := http.Get(fmt.Sprintf("http://%s:5000/heartbeat", server))
		if err != nil {
			log.Printf("Error getting heartbeat from %s: %v", server, err)
			var body configPayload
			for shard_ := range server_shard_mapping[server] {
				if server_shard_mapping[server][shard_] {
					reElect(shard_)
				}
				body.Shards = append(body.Shards, shard_)
			}
			err = spawnContainer(server)
			if err != nil {
				log.Printf("%v", err)
				continue
			}
			spawned[server] = body
		}
	}
	if len(spawned) == 0 {
		return
	}
	// spawn new containers for failed servers
	for server, body := range spawned {
		jsonBody, err := json.Marshal(body)
		configEndpoint := fmt.Sprintf("http://%s:5000/config", server)
		post, err := http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		for {
			if err == nil {
				break
			}
			post, err = http.Post(configEndpoint, "application/json", bytes.NewReader(jsonBody))
		}
		if post.StatusCode != http.StatusOK {
			fmt.Printf("Error configuring server %s: %v", server, err)
			continue
		}
	}
}
