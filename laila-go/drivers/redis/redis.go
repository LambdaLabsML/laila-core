// Package redis is the ``redis`` extra (``pip install laila-core[redis]``):
// it binds laila.RedisPool to github.com/redis/go-redis. Import it for its
// side effect:
//
//	import _ "github.com/amirzadehlambda/laila/laila-go/drivers/redis"
package redis

import (
	"context"
	"errors"
	"time"

	laila "github.com/amirzadehlambda/laila/laila-go"
	"github.com/amirzadehlambda/laila/laila-go/py"
	goredis "github.com/redis/go-redis/v9"
)

// Driver implements laila.Redis_driver.
type Driver struct{}

// Redis is ``redis.Redis(unix_socket_path=..., password=...,
// socket_connect_timeout=..., decode_responses=True)``. Like the Python
// constructor it does not connect; the first command does.
func (Driver) Redis(unix_socket_path string, password *string, socket_connect_timeout float64) (laila.Redis_client, error) {
	opts := &goredis.Options{Network: "unix", Addr: unix_socket_path}
	if password != nil {
		opts.Password = *password
	}
	if socket_connect_timeout > 0 {
		opts.DialTimeout = time.Duration(socket_connect_timeout * float64(time.Second))
	}
	// redis-py retries nothing by default; keep the probe loop in the pool
	// responsible for retrying.
	opts.MaxRetries = -1
	return &client{rdb: goredis.NewClient(opts)}, nil
}

type client struct{ rdb *goredis.Client }

func wrap(err error) error {
	if err == nil {
		return nil
	}
	return py.ConnectionError.New(err.Error()).From(err)
}

func (c *client) Ping() error { return wrap(c.rdb.Ping(context.Background()).Err()) }

func (c *client) Hget(name string, key string) (*string, error) {
	v, err := c.rdb.HGet(context.Background(), name, key).Result()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, wrap(err)
	}
	return &v, nil
}

func (c *client) Hset(name string, key string, value string) error {
	return wrap(c.rdb.HSet(context.Background(), name, key, value).Err())
}

func (c *client) Hdel(name string, key string) error {
	return wrap(c.rdb.HDel(context.Background(), name, key).Err())
}

func (c *client) Delete(name string) error {
	return wrap(c.rdb.Del(context.Background(), name).Err())
}

func (c *client) Hexists(name string, key string) (bool, error) {
	ok, err := c.rdb.HExists(context.Background(), name, key).Result()
	return ok, wrap(err)
}

func (c *client) Hkeys(name string) ([]string, error) {
	keys, err := c.rdb.HKeys(context.Background(), name).Result()
	return keys, wrap(err)
}

func (c *client) Hscan(name string, cursor uint64) (uint64, []string, error) {
	kv, next, err := c.rdb.HScan(context.Background(), name, cursor, "", 0).Result()
	if err != nil {
		return 0, nil, wrap(err)
	}
	keys := make([]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		keys = append(keys, kv[i])
	}
	return next, keys, nil
}

func (c *client) Shutdown(save bool) error {
	ctx := context.Background()
	var err error
	if save {
		err = c.rdb.ShutdownSave(ctx).Err()
	} else {
		err = c.rdb.ShutdownNoSave(ctx).Err()
	}
	// A successful SHUTDOWN closes the socket before replying, which
	// go-redis reports as an error; redis-py raises too and the pool
	// suppresses it.
	return wrap(err)
}

func (c *client) Close() error { return wrap(c.rdb.Close()) }

func init() { laila.Register_driver("redis", Driver{}) }
