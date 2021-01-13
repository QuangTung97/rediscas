package main

import (
	"context"
	"fmt"
	"rediscas"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	client := redis.NewClient(&redis.Options{})
	cache := rediscas.NewCache(nil)

	ctx := context.Background()

	start := time.Now()
	for i := 0; i < 100000; i++ {
		// err := cache.Set(ctx, client, "tung", uint32(i+1), "some long string abcdafdafdsfaf")
		// if err != nil {
		// 	panic(err)
		// }
		_, err := cache.GetLease(ctx, client, "tung")
		if err == redis.Nil {
			cache.Set(ctx, client, "tung", 10, "taquangtung")
			continue
		}
		if err == rediscas.ErrLeasing {
			fmt.Println("LEASING")
			continue
		}
		if err != nil {
			panic(err)
		}
	}
	fmt.Println(time.Since(start))
}
