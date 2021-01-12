package rediscas

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"
)

// Cache ...
type Cache struct {
	logger func(err error)

	hash    string
	hashMut sync.RWMutex
}

const versionField = "version"

const dataField = "data"

const script = `
local key = KEYS[1]
local new_version = tonumber(KEYS[2])
local old_version = redis.call('hget', key, 'version')
old_version = tonumber(old_version)

if not old_version or new_version > old_version then
    redis.call('hset', key, 'version', new_version)
    redis.call('hset', key, 'data', KEYS[3])
    -- 15 * 60 seconds
    redis.call('expire', key, 900)
end

return {ok = 'OK'}
`

// NewCache creates a CAS cache, logger can be nil
func NewCache(logger func(err error)) *Cache {
	return &Cache{
		logger: logger,
	}
}

// Get ...
func (c *Cache) Get(ctx context.Context, client *redis.Client, key string) (string, error) {
	return client.HGet(ctx, key, dataField).Result()
}

// Set ...
func (c *Cache) Set(
	ctx context.Context, client *redis.Client,
	key string, version uint32, value string,
) error {
	for {
		c.hashMut.RLock()
		cacheHash := c.hash
		c.hashMut.RUnlock()

		if cacheHash == "" {
			c.hashMut.Lock()
			if c.hash == "" {
				hash, err := client.ScriptLoad(ctx, script).Result()
				if err != nil {
					c.hashMut.Unlock()
					return err
				}
				c.hash = hash
			}
			cacheHash = c.hash
			c.hashMut.Unlock()
		}

		fmt.Println("HASH:", cacheHash)

		err := client.EvalSha(ctx, cacheHash, []string{
			key,
			strconv.FormatInt(int64(version), 10),
			value,
		}).Err()
		if err != nil {
			if c.logger != nil {
				c.logger(err)
			} else {
				fmt.Println("Cache.Set:", err)
			}

			c.hashMut.Lock()
			c.hash = ""
			c.hashMut.Unlock()
			continue
		}
		return nil
	}
}
