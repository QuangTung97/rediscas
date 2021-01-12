package main

import (
	"context"
	"rediscas"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	client := redis.NewClient(&redis.Options{})
	cache := rediscas.NewCache(nil)

	ctx := context.Background()
	for {
		err := cache.Set(ctx, client, "tung", 101, "value003")
		if err != nil {
			panic(err)
		}
		time.Sleep(3 * time.Second)
	}
}
