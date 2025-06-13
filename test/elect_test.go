package test

import (
	"context"
	"fmt"
	"github.com/995933447/autoelectv2/factory"
	"github.com/995933447/log-go"
	"github.com/995933447/log-go/impl/loggerwriter"
	"github.com/995933447/std-go/print"
	"github.com/gomodule/redigo/redis"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestLoopInRedisElect(t *testing.T) {
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

	elect, err := factory.NewAutoElection(factory.ElectDriverDistribMuRedis, factory.NewDistribMuRedisCfg(redisPool, "test_cluster", "123"))
	if err != nil {
		t.Log(err)
		return
	}

	elect.LoopInElectV2(nil, nil)

	time.Sleep(time.Second * 3)

	fmt.Println(elect.IsMaster())
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

func TestLoopInEtcdv3Elect(t *testing.T) {
	logger := log.NewLogger(loggerwriter.NewStdoutLoggerWriter(print.ColorBlue))
	logger.SetLogLevel(log.LevelPanic)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:12379"},
	})
	if err != nil {
		t.Log(err)
		return
	}

	elect, err := factory.NewAutoElection(factory.ElectDriverDistribMuEtcdv3, factory.NewDistribMuEtcdv3Cfg("abc", etcdCli, 5))
	if err != nil {
		t.Log(err)
		return
	}

	errDuringLoopCh := make(chan error)
	go func() {
		stopTk := time.NewTicker(time.Second * 10)
		checkMasterTk := time.NewTicker(time.Second * 5)
		defer stopTk.Stop()
		defer checkMasterTk.Stop()
		for {
			select {
			//case _ = <-stopTk.C:
			//	fmt.Println("stop")
			//	elect.StopElect()
			//	fmt.Println("stopped")
			case _ = <-checkMasterTk.C:
				fmt.Println(elect.IsMaster())
			case err := <-errDuringLoopCh:
				t.Log(err)
			}
		}
	}()

	elect.LoopInElect(context.Background(), errDuringLoopCh)
}
