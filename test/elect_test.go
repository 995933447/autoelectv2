package test

import (
	"context"
	"fmt"
	"github.com/995933447/autoelectv2/factory"
	"github.com/995933447/log-go"
	"github.com/995933447/log-go/impl/loggerwriter"
	"github.com/995933447/std-go/print"
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
	"time"
)

func TestLoopInEtcdv3Elect(t *testing.T) {
	logger := log.NewLogger(loggerwriter.NewStdoutLoggerWriter(print.ColorBlue))
	logger.SetLogLevel(log.LevelPanic)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{"127.0.0.1:2379"},
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
			case _ = <-stopTk.C:
				fmt.Println("stop")
				elect.StopElect()
				fmt.Println("stopped")
			case _ = <-checkMasterTk.C:
				fmt.Println(elect.IsMaster())
			case err := <-errDuringLoopCh:
				t.Log(err)
			}
		}
	}()

	elect.LoopInElect(context.Background(), errDuringLoopCh)
}
