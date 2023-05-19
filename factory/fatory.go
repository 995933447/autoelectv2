package factory

import (
	"fmt"
	"github.com/995933447/autoelectv2"
	"github.com/995933447/autoelectv2/impl/distribmu/etcdv3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ElectDriver int

const (
	ElectDriverNil = iota
	ElectDriverDistribMuEtcdv3
)

type DistribMuEtcdv3Cfg struct {
	etcdCli          *clientv3.Client
	electIntervalSec uint32
	cluster          string
}

func NewDistribMuEtcdv3Cfg(cluster string, etcdCli *clientv3.Client, electIntervalSec uint32) *DistribMuEtcdv3Cfg {
	return &DistribMuEtcdv3Cfg{
		etcdCli:          etcdCli,
		electIntervalSec: electIntervalSec,
		cluster:          cluster,
	}
}

func NewAutoElection(driver ElectDriver, cfg any) (autoelectv2.AutoElection, error) {
	switch driver {
	case ElectDriverDistribMuEtcdv3:
		specCfg := cfg.(*DistribMuEtcdv3Cfg)
		return etcdv3.New(specCfg.cluster, specCfg.etcdCli, specCfg.electIntervalSec)
	}
	return nil, fmt.Errorf("invalid driver:%d", driver)
}
