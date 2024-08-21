package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

var dbConnections []*sql.DB

var clients int = 0

func init() {
	oddShard, oddErr := sql.Open("mysql", "root:123456@tcp(localhost:3306)/flickerodd")
	evenShard, evenErr := sql.Open("mysql", "root:123456@tcp(localhost:3306)/flickereven")
	if oddErr != nil {
		log.Fatal(oddErr)
	}
	if evenErr != nil {
		log.Fatal(evenErr)
	}
	dbConnections = append(dbConnections, oddShard, evenShard)
}

func generateID(db *sql.DB) (int, error) {
	var currId int
	err := db.QueryRow("SELECT counter FROM genid WHERE stub='a' FOR UPDATE").Scan(&currId)
	if err != nil {
		return -1, err
	}
	res, updateErr := db.Exec("UPDATE genid SET counter=? WHERE stub='a'", currId+2)
	if updateErr != nil {
		return -1, err
	}
	rowsAffected, rowErr := res.RowsAffected()
	if rowErr != nil {
		return -1, err
	}
	if rowsAffected > 0 {
		return currId + 2, nil
	} else {
		return -1, nil
	}
}

func handleIdGeneration(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	clients = clients + 1
	dbShardIdx := clients % 2
	db := dbConnections[dbShardIdx]
	id, genErr := generateID(db)
	if genErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Failed to generate ID"})
	}
	// send ID range in response
	json.NewEncoder(w).Encode(map[string]int{
		"id": id,
	})
}

func main() {
	log.Println("Flicker's way of odd-even id generation")
	router := mux.NewRouter()
	router.HandleFunc("/getId", handleIdGeneration).Methods("GET")
	log.Println("Server starting on port : 8000")
	log.Fatal(http.ListenAndServe(":8000", router))
}
