package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/garyburd/redigo/redis"
)

// Report all errors to stdout.
func handle(err error) {
	if err != nil && err != redis.ErrNil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Scan and queue source keys.
func get(conn redis.Conn, queue chan<- map[string][]byte) {
	var (
		cursor int64
		keys   []string
	)

	for {
		// Scan a batch of keys.
		values, err := redis.Values(conn.Do("SCAN", cursor))
		handle(err)
		values, err = redis.Scan(values, &cursor, &keys)
		handle(err)
		batch := make(map[string][]byte)
		// Get pipelined dumps.
		for i, key := range keys {
			fmt.Println("key:", key)
			dumps, err := redis.Bytes(conn.Do("GET", key))
			handle(err)
			// Build batch map.
			batch[keys[i]] = dumps
		}

		// Last iteration of scan.
		if cursor == 0 {
			// queue last batch.
			select {
			case queue <- batch:
			}
			close(queue)
			break
		}

		fmt.Printf(">")
		// queue current batch.
		queue <- batch
	}
}

// Restore a batch of keys on destination.
func put(conn redis.Conn, queue <-chan map[string][]byte) {
	for batch := range queue {
		for key, value := range batch {
			_, err := conn.Do("SETEX", key, 86400, value)
			handle(err)
		}

		fmt.Printf(".")
	}
}

func main() {
	from := flag.String("from", "", "example: redis://127.0.0.1:6379/0")
	to := flag.String("to", "", "example: redis://127.0.0.1:6379/1")
	flag.Parse()

	source, err := redis.DialURL(*from)
	handle(err)
	destination, err := redis.DialURL(*to)
	handle(err)
	defer source.Close()
	defer destination.Close()

	// Channel where batches of keys will pass.
	queue := make(chan map[string][]byte, 100)

	// Scan and send to queue.
	go get(source, queue)

	// Restore keys as they come into queue.
	put(destination, queue)

	fmt.Println("Sync done.")
}
