package test

import (
	"context"
	"fmt"
	"github.com/995933447/autoelect/factory"
	mufactory "github.com/995933447/distribmu/factory"
	"github.com/995933447/log-go"
	"github.com/995933447/log-go/impl/loggerwriter"
	"github.com/995933447/redisgroup"
	"github.com/995933447/std-go/print"
	"testing"
	"time"
)

func TestLoopInGitDistribElect(t *testing.T) {
	logger := log.NewLogger(loggerwriter.NewStdoutLoggerWriter(print.ColorBlue))
	logger.SetLogLevel(log.LevelPanic)
	elect, err := factory.NewAutoElection(
		factory.ElectDriverGitDistribMu,
		factory.NewDistribMuElectDriverConf(
			"abc",
			time.Second * 5,
			mufactory.MuTypeRedis,
			mufactory.NewRedisMuDriverConf(
				redisgroup.NewGroup(
					[]*redisgroup.Node{redisgroup.NewNode("127.0.0.1", 6379, "")},
					logger,
					),
					100,
				),
			),
		)
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
			case _ = <- stopTk.C:
				fmt.Println("stop")
				elect.StopElect()
				fmt.Println("stopped")
			case _ = <- checkMasterTk.C:
				fmt.Println(elect.IsMaster())
			case err := <- errDuringLoopCh:
				t.Log(err)
			}
		}
	}()

	if err = elect.LoopInElect(context.Background(), errDuringLoopCh); err != nil {
		t.Log(err)
	}
}
