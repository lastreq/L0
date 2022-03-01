package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	stan "github.com/nats-io/stan.go"
	"github.com/patrickmn/go-cache"
	"github.com/pkg/errors"
	"log"
	"net/http"
	"os"
	"time"

	_ "github.com/lib/pq"
)

var myCache CacheItf

type Orders struct {
	Id   int    `json:"id"`
	Data string `json:"data"`
}

var conn *pgx.Conn

func printMsg(m *stan.Msg, i int) {
	log.Printf("[#%d] Received: %s\n", i, m)
}
func msgToPostgres(m *stan.Msg, conn *pgx.Conn) {
	var s string
	var msgData string
	msgData = string(m.Data)

	err := conn.QueryRow(context.Background(), fmt.Sprintf("INSERT INTO orders (data) VALUES ('%s')", msgData)).Scan(&s)
	if err != nil && err.Error() != "no rows in result set" {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

}

func home(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Write([]byte("Привет"))
}

// Обработчик для отображения содержимого
func showId(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	id := r.URL.Query().Get("id")

	var result Orders

	b, err := myCache.Get(id)
	if err != nil {

		log.Fatal(err)
	}

	if b != nil {
		// cache exist
		err := json.Unmarshal(b, &result)
		if err != nil {
			log.Fatal(err)
		}

		b, _ := json.Marshal(map[string]interface{}{
			"data":        result,
			"elapsed":     time.Since(start).Microseconds(),
			"cache exist": "True",
		})
		w.Write([]byte(b))
		return
	}

	// Get from DB

	sqlString := fmt.Sprintf("SELECT * FROM orders WHERE id='%v'", id)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	err = conn.QueryRow(context.Background(), sqlString).Scan(&result.Id, &result.Data)
	if err != nil && err.Error() != "no rows in result set" {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	if err != nil {
		http.NotFound(w, r)
		return
	}

	err = myCache.Set(id, result, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	b, err = json.Marshal(map[string]interface{}{
		"data":        result,
		"elapsed":     time.Since(start).Microseconds(),
		"cache exist": "False",
	})

	if err != nil {
		log.Fatal(err)
	}

	w.Write(b)
}

func showOrderUid(w http.ResponseWriter, r *http.Request) {

	start := time.Now()
	id := r.URL.Query().Get("id")

	//var result Orders
	var resultString string
	b, err := myCache.Get(id)
	if err != nil {
		// error
		log.Fatal(err)
	}

	if b != nil {
		// cache exist
		err := json.Unmarshal(b, &resultString)
		if err != nil {
			log.Fatal(err)
		}

		b, _ := json.Marshal(map[string]interface{}{
			"data":        resultString,
			"elapsed":     time.Since(start).Microseconds(),
			"cache exist": "True",
		})
		w.Write([]byte(b))
		return
	}

	// Get from DB

	sqlString := fmt.Sprintf("SELECT data FROM orders WHERE data  ->> 'order_uid' = '%v'", id)
	//sqlString = fmt.Sprintf("SELECT * FROM orders WHERE id='%v'", id)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	err = conn.QueryRow(context.Background(), sqlString).Scan(&resultString)
	if err != nil && err.Error() != "no rows in result set" {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	err = myCache.Set(id, resultString, 30*time.Second)
	if err != nil {
		log.Fatal(err)
	}

	b, err = json.Marshal(map[string]interface{}{
		"data":        resultString,
		"elapsed":     time.Since(start).Microseconds(),
		"cache exist": "False",
	})

	if err != nil {
		log.Fatal(err)
	}

	w.Write(b)

}

func connectToDb() *pgx.Conn {
	url := "postgres://postgres:123@localhost:5433/productdb"
	conn1, err := pgx.Connect(context.Background(), url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	return conn1

}

func InitCache() {
	myCache = &AppCache{
		client: cache.New(5*time.Minute, 10*time.Minute),
	}
}

type CacheItf interface {
	Set(key string, data interface{}, expiration time.Duration) error
	Get(key string) ([]byte, error)
}
type AppCache struct {
	client *cache.Cache
}

func (r *AppCache) Set(key string, data interface{}, expiration time.Duration) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	r.client.Set(key, b, expiration)
	return nil
}

func (r *AppCache) Get(key string) ([]byte, error) {
	res, exist := r.client.Get(key)
	if !exist {
		return nil, nil
	}

	resByte, ok := res.([]byte)
	if !ok {
		return nil, errors.New("Format is not arr of bytes")
	}

	return resByte, nil
}
func main() {
	InitCache()

	conn = connectToDb()
	defer conn.Close(context.Background())

	var clientID = "myID"
	var stanClusterID = "test-cluster"
	// Connect to a server
	sc, _ := stan.Connect(stanClusterID, clientID)
	defer sc.Close()

	subj, i := "wb", 0
	mcb := func(msg *stan.Msg) {
		i++
		printMsg(msg, i)
		msgToPostgres(msg, conn)

	}
	var qgroup string

	_, err := sc.QueueSubscribe(subj, qgroup, mcb)
	if err != nil {
		sc.Close()
		log.Fatal(err)
	}

	log.Printf("Listening on [%s], clientID=[%s], qgroup=[%s] \n", subj, clientID, qgroup)

	mux := http.NewServeMux()
	mux.HandleFunc("/", home)
	mux.HandleFunc("/showId", showId)
	mux.HandleFunc("/showOrderUid", showOrderUid)

	log.Println("Запуск веб-сервера на http://127.0.0.1:4000")
	err1 := http.ListenAndServe(":4000", mux)
	log.Fatal(err1)

}
