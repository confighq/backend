package main

import (
	"encoding/json"
	"net/http"
	"flag"
	"fmt"
	"log"

	"github.com/nitishm/go-rejson"
  "github.com/gomodule/redigo/redis"

	guuid "github.com/google/uuid"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
)

type Query struct {
	QueryId 		string		`json:"query_id"`
	Name  			string 		`json:"name"`
	Combinator 	string 		`json:"combinator"`
	Groups   		[]Group 	`json:"groups"`
	Default   	Response 	`json:"default"`
	Match   		Response 	`json:"match"`
}

type Group struct {
	Combinator 	string 		`json:"combinator"`
	Rules   		[]Rule 		`json:"rules"`
}

type Rule struct {
	Operator  	string 		`json:"operator"`
	Parameter 	string 		`json:"parameter"`
	Value   		string 		`json:"value"`
}

type Response struct {
	Type  			string 		`json:"type"`
	Value   		string 		`json:"value"`
}

var pool = newPool()

func newPool() *redis.Pool {
	var addr = flag.String("Server", "redis-13341.c60.us-west-1-2.ec2.cloud.redislabs.com:13341", "Redis server address")
	var password = "Q6iRl3Y8yq4MRRzSEJe5G8H6n1m75wzd"

	flag.Parse()

	return &redis.Pool{
		MaxIdle: 80,
		MaxActive: 12000,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", *addr)

			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}

			if err != nil {
				panic(err.Error())
			}

			return c, err
		},
	}
}

func createQuery(w http.ResponseWriter, r *http.Request) {
	var query Query

	err := json.NewDecoder(r.Body).Decode(&query)
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "invalid request structure")
		return
	}

	uuid := guuid.New().String()
	query.QueryId = uuid

	var key = "query:" + query.Name
	var conn = pool.Get()

	rh := rejson.NewReJSONHandler()
	rh.SetRedigoClient(conn)

	res, err := rh.JSONSet(key, ".", query)
	if err != nil {
		log.Fatalf("Failed to JSONSet")
		return
	}

	if res.(string) == "OK" {
		fmt.Printf("Success: %s\n", res)
	} else {
		respondWithError(w, http.StatusBadRequest, "Failed to set query")
		return
	}

	respondWithJSON(w, http.StatusOK, res)
}


func getQueries(w http.ResponseWriter, r *http.Request) {
	var conn = pool.Get()

	rh := rejson.NewReJSONHandler()
	rh.SetRedigoClient(conn)

	keys, err := redis.Strings(conn.Do("KEYS", "query:*"))
	if err != nil {
		// handle error
	}

	var query Query
	var queries []Query

	for _, key := range keys {
		queryJSON, err := redis.Bytes(rh.JSONGet(key, "."))
		if err != nil {
			log.Fatalf("Failed to JSONGet")
			return
		}

		err = json.Unmarshal(queryJSON, &query)
		if err != nil {
			log.Fatalf("Failed to JSON Unmarshal")
			return
		}

		queries = append(queries, query)
	}

	respondWithJSON(w, http.StatusOK, queries)
}

func getQuery(w http.ResponseWriter, r *http.Request) {
	var params = mux.Vars(r)
	var id = params["id"]
	var conn = pool.Get()
	var query Query

	rh := rejson.NewReJSONHandler()
  rh.SetRedigoClient(conn)

	queryJSON, err := redis.Bytes(rh.JSONGet("query:" + id, "."))
	if err != nil {
		log.Fatalf("Failed to JSONGet")
		return
	}

	err = json.Unmarshal(queryJSON, &query)

	if err != nil {
		log.Fatalf("Failed to JSON Unmarshal")
		return
	}

	respondWithJSON(w, http.StatusOK, query)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	w.Write(response)
}

func main() {
	r := mux.NewRouter()

	r.HandleFunc("/v1/query", createQuery).Methods("POST")
	r.HandleFunc("/v1/query", getQueries).Methods("GET")
	r.HandleFunc("/v1/query/{id}", getQuery).Methods("GET")

	headersOk := handlers.AllowedHeaders([]string{"X-Session-Token"})
	originsOk := handlers.AllowedOrigins([]string{"*"})
	methodsOk := handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "OPTIONS"})

	log.Fatal(http.ListenAndServe(":8090", handlers.CORS(originsOk, headersOk, methodsOk)(r)))
}
