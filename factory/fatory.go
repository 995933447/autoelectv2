package factory

import (
	"fmt"
	"github.com/995933447/autoelectv2"
	"github.com/995933447/autoelectv2/impl/distribmu/etcdv3"
	rediselect "github.com/995933447/autoelectv2/impl/distribmu/redis"
	"github.com/gomodule/redigo/redis"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ElectDriver int

const (
	ElectDriverNil = iota
	ElectDriverDistribMuEtcdv3
	ElectDriverDistribMuRedis
)

type DistribMuEtcdv3Cfg struct {
	etcdCli          *clientv3.Client
	masterTenancySec uint32
	cluster          string
}

func NewDistribMuEtcdv3Cfg(cluster string, etcdCli *clientv3.Client, masterTenancySec uint32) *DistribMuEtcdv3Cfg {
	return &DistribMuEtcdv3Cfg{
		etcdCli:          etcdCli,
		masterTenancySec: masterTenancySec,
		cluster:          cluster,
	}
}

type DistribMuRedisCfg struct {
	redisPool *redis.Pool
	cluster   string
	nodeId    string
}

func NewDistribMuRedisCfg(redisPool *redis.Pool, cluster string, nodeId string) *DistribMuRedisCfg {
	return &DistribMuRedisCfg{
		redisPool: redisPool,
		cluster:   cluster,
		nodeId:    nodeId,
	}
}

func NewAutoElection(driver ElectDriver, cfg any) (autoelectv2.AutoElection, error) {
	switch driver {
	case ElectDriverDistribMuEtcdv3:
		specCfg := cfg.(*DistribMuEtcdv3Cfg)
		return etcdv3.New(specCfg.cluster, specCfg.etcdCli, specCfg.masterTenancySec)
	case ElectDriverDistribMuRedis:
		specCfg := cfg.(*DistribMuRedisCfg)
		return rediselect.NewRedisElect(specCfg.cluster, specCfg.nodeId, specCfg.redisPool)
	}
	return nil, fmt.Errorf("invalid driver:%d", driver)
}
