package myredis

import (
	goredis "github.com/garyburd/redigo/redis"
	"log"
	"time"
)

var (
	defaultAddr = "127.0.0.1:6379"
	pool        *goredis.Pool //conn pool
)

/*
typedef void (*cmd_sub_t)(const char *key, const char *value, size_t size, void *data);
void cmd_sub(const char *key, cmd_sub_t cb, void *data);
*/
type cmdSubT func(key string, value interface{}, data interface{})

func CmdSub(key string, cb cmdSubT, data interface{}) {query_per_ap
	conn := pool.Get()
	psc := goredis.PubSubConn{conn}
	psc.Subscribe(key)
	go func(cb cmdSubT, data interface{}) {
		for {
			switch v := psc.Receive().(type) {
			case goredis.Message:
				//log.Printf("[Message]%s: message: %s", v.Channel, v.Data)
				cb(v.Channel, v.Data, data)
			case goredis.Subscription:
				log.Printf("[Subscription]%s: %s %d", v.Channel, v.Kind, v.Count)
			case error:
				log.Printf("sub error:%s", v)
				//log error
				psc.Unsubscribe(key)
				psc.Close()
				time.Sleep(1 * time.Second)
				CmdSub(key, cb, data)
				return
			}
		}
	}(cb, data)

}

/*
int cmd_pub(const char *key, const char *value, size_t size);
*/
func CmdPub(key string, value interface{}) error {
	conn := pool.Get()
	err := conn.Send("PUBLISH", key, value)
	conn.Close()
	return err
}

/*
int cmd_set(const char *key, const char *value, size_t size);
*/
func CmdSet(key string, value interface{}) error {
	conn := pool.Get()
	err := conn.Send("SET", key, value)
	conn.Close()
	return err
}

/*int cmd_get(const char *key, char **pvalue, size_t *psize);*/
func CmdGet(key string) (interface{}, error) {
	conn := pool.Get()
	reply, err := conn.Do("GET", key)
	conn.Close()
	return reply, err
}

func newPool(server string) *goredis.Pool {
	return &goredis.Pool{
		MaxIdle:     8,
		IdleTimeout: 240 * time.Second,
		Dial: func() (goredis.Conn, error) {
			c, err := goredis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c goredis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func init() {
	pool = newPool(defaultAddr)
}

