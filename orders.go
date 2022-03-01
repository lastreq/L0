package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

//var ctx = context.Background()

type JsonResponse struct {
	Data   []Orders `json:"data"`
	Source string   `json:"source"`
}

func getOrders() (*JsonResponse, error) {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6377",
		Password: "",
		DB:       0,
	})

	cachedOrders, err := redisClient.Get(context.Background(), "orders").Bytes()

	response := JsonResponse{}

	if err != nil {

		dbOrders, err := fetchFromDb()

		if err != nil {
			return nil, err
		}

		cachedOrders, err = json.Marshal(dbOrders)

		if err != nil {
			return nil, err
		}

		err = redisClient.Set(context.Background(), "orders", cachedOrders, 10*time.Second).Err()

		if err != nil {
			return nil, err
		}

		response = JsonResponse{Data: dbOrders, Source: "PostgreSQL"}

		return &response, err
	}

	orders := []Orders{}

	err = json.Unmarshal(cachedOrders, &orders)

	if err != nil {
		return nil, err
	}

	response = JsonResponse{Data: orders, Source: "Redis Cache"}

	return &response, nil
}

/*func fetchFromDb() ([]Orders, error) {

	conn = connectToDb()
	defer conn.Close(context.Background())

	queryString := `SELECT * FROM orders`

	rows, err := conn.Query(context.Background(), queryString)
	if err != nil && err.Error() != "no rows in result set" {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	if err != nil {
		return nil, err
	}

	var records []Orders

	for rows.Next() {

		var p Orders

		err = rows.Scan(&p.Id, &p.Data)

		records = append(records, p)

		if err != nil {
			return nil, err
		}

	}

	return records, nil
}*/
