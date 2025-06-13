package elect

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	uuid "github.com/satori/go.uuid"
	"testing"
	"time"
)

func TestElect(t *testing.T) {
	redisPool := &redis.Pool{
		MaxIdle:         2,
		MaxActive:       0, //when zero,there's no limit. https://godoc.org/github.com/garyburd/redigo/redis#Pool
		IdleTimeout:     time.Minute * 3,
		MaxConnLifetime: time.Minute * 10,
		Wait:            true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "127.0.0.1:6379")
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: redisOnBorrow,
	}

	e, err := NewRedisElect("testElect", uuid.NewV4().String(), redisPool)
	if err != nil {
		fmt.Println("err:", err)
	}

	e2, err := NewRedisElect("testElect", uuid.NewV4().String(), redisPool)
	if err != nil {
		fmt.Println("err:", err)
	}

	e3, err := NewRedisElect("testElect", uuid.NewV4().String(), redisPool)
	if err != nil {
		fmt.Println("err:", err)
	}

	e.LoopInElectV2(nil, func(err error) {
		fmt.Println("loop err:", err)
	})
	time.Sleep(time.Second)
	e2.LoopInElectV2(nil, nil)
	e3.LoopInElectV2(nil, nil)

	go func() {
		time.Sleep(time.Second * 10)
		e.StopElect()
	}()

	for {
		fmt.Printf("e is master:%v\n", e.IsMaster())
		fmt.Printf("e2 is master:%v\n", e2.IsMaster())
		fmt.Printf("e3 is master:%v\n", e3.IsMaster())
		time.Sleep(time.Second * 3)
	}
}

func redisOnBorrow(c redis.Conn, t time.Time) error {
	if time.Since(t) < time.Minute {
		return nil
	}
	_, err := c.Do("PING")
	if err != nil {
		return err
	}
	return nil
}
