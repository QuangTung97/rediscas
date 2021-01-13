package rediscas

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"
)

// Cache ...
type Cache struct {
	logger func(err error)

	setScriptHash    string
	setScriptHashMut sync.RWMutex

	getScriptHash    string
	getScriptHashMut sync.RWMutex
}

const versionField = "version"
const dataField = "data"
const cacheLeasingError = "cache leasing"

const cacheSetScript = `
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

const cacheGetScript = `
local key = KEYS[1]
local data = redis.call('hget', key, 'data')
if data then
    return data
end

local leasing = redis.call('hget', key, 'leasing')
if leasing then
    return {err = 'cache leasing'}
end

redis.call('hset', key, 'leasing', 'true')
redis.call('expire', key, 30)

return nil
`

// ErrLeasing ...
var ErrLeasing = errors.New("rediscas: cache leasing")

// ErrInvalidDataType ...
var ErrInvalidDataType = errors.New("rediscas: invalid data type")

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

// GetLease ...
func (c *Cache) GetLease(ctx context.Context, client *redis.Client, key string) (string, error) {
	for {
		c.getScriptHashMut.RLock()
		cacheHash := c.getScriptHash
		c.getScriptHashMut.RUnlock()

		if cacheHash == "" {
			c.getScriptHashMut.Lock()
			if c.getScriptHash == "" {
				hash, err := client.ScriptLoad(ctx, cacheGetScript).Result()
				if err != nil {
					c.getScriptHashMut.Unlock()
					return "", err
				}
				c.getScriptHash = hash
			}
			cacheHash = c.getScriptHash
			c.getScriptHashMut.Unlock()
		}

		data, err := client.EvalSha(ctx, cacheHash, []string{key}).Result()
		if err == redis.Nil {
			return "", redis.Nil
		}
		if err != nil {
			if err.Error() == cacheLeasingError {
				return "", ErrLeasing
			}
			if c.logger != nil {
				c.logger(err)
			} else {
				fmt.Println("Cache.GetLease:", err)
			}

			c.getScriptHashMut.Lock()
			c.getScriptHash = ""
			c.getScriptHashMut.Unlock()
			continue
		}

		str, ok := data.(string)
		if !ok {
			return "", ErrInvalidDataType
		}

		return str, nil
	}
}

// Set ...
func (c *Cache) Set(
	ctx context.Context, client *redis.Client,
	key string, version uint32, value string,
) error {
	for {
		c.setScriptHashMut.RLock()
		cacheHash := c.setScriptHash
		c.setScriptHashMut.RUnlock()

		if cacheHash == "" {
			c.setScriptHashMut.Lock()
			if c.setScriptHash == "" {
				hash, err := client.ScriptLoad(ctx, cacheSetScript).Result()
				if err != nil {
					c.setScriptHashMut.Unlock()
					return err
				}
				c.setScriptHash = hash
			}
			cacheHash = c.setScriptHash
			c.setScriptHashMut.Unlock()
		}

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

			c.setScriptHashMut.Lock()
			c.setScriptHash = ""
			c.setScriptHashMut.Unlock()
			continue
		}
		return nil
	}
}
