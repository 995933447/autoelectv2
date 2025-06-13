package elect

import (
	"context"
	"errors"
	"fmt"
	"github.com/995933447/autoelectv2"
	"github.com/995933447/runtimeutil"
	"github.com/gomodule/redigo/redis"
	"strconv"
	"strings"
	"time"
)

type RedisElect struct {
	name        string
	isMaster    bool
	nodeId      string
	clusterKey  string
	stopCh      chan struct{}
	redisPool   *redis.Pool
	onBeMaster  func() bool
	onLostMater func()
	onErr       func(err error)
}

var _ autoelectv2.AutoElection = (*RedisElect)(nil)

func NewRedisElect(name, nodeId string, redisPool *redis.Pool) (*RedisElect, error) {
	if name == "" {
		return nil, errors.New("name is empty")
	}
	if nodeId == "" {
		return nil, errors.New("node id is empty")
	}
	return &RedisElect{
		name:       name,
		nodeId:     nodeId,
		clusterKey: fmt.Sprintf("electCluster.%s.MasterNodeId", name),
		stopCh:     make(chan struct{}),
		redisPool:  redisPool,
	}, nil
}

func (r *RedisElect) LoopInElect(_ context.Context, errCh chan error) {
}

func (r *RedisElect) LoopInElectV2(_ context.Context, onErr func(err error)) {
	r.Run(onErr)
}

func (r *RedisElect) StopElect() {
	r.Stop()
}

func (r *RedisElect) OnBeMaster(fun func() bool) {
	r.onBeMaster = fun
}

func (r *RedisElect) OnLostMaster(fun func()) {
	r.onLostMater = fun
}

func (r *RedisElect) Stop() {
	r.stopCh <- struct{}{}
}

func (r *RedisElect) getConn() redis.Conn {
	return r.redisPool.Get()
}

func (r *RedisElect) doRedisGetStr(key string) (string, error) {
	res, err := redis.String(r.doRedisCmd("GET", key))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			return "", nil
		}
		return "", err
	}

	return res, nil
}

func (r *RedisElect) getMasterNodeIdFromRedis() (string, error) {
	var nodeId string
	val, err := r.doRedisGetStr(r.clusterKey)
	if err != nil {
		return "", err
	}
	info := strings.SplitN(val, ":", 2)
	if len(info) > 1 {
		nodeId = info[1]
	} else {
		nodeId = val
	}
	return nodeId, nil
}

func (r *RedisElect) doRedisCmd(cmd, key string, args ...interface{}) (interface{}, error) {
	conn := r.getConn()

	if err := conn.Err(); err != nil {
		return 0, err
	}

	defer conn.Close()

	return conn.Do(cmd, append([]interface{}{key}, args...)...)
}

func (r *RedisElect) Run(onErr func(err error)) {
	r.onErr = onErr

	pTraceId := "redisElect:" + runtimeutil.GenTraceStrId()
	runtimeutil.Go(func() {
		firstLoop := true
		for {
			trace := pTraceId + "." + runtimeutil.GenTraceStrId()
			runtimeutil.StoreTrace(trace)
			if firstLoop {
				firstLoop = false
			} else {
				tm := time.NewTimer(time.Second * 2)
				select {
				case <-tm.C:
				case <-r.stopCh:
					r.isMaster = false
					tm.Stop()
					goto out
				}
				tm.Stop()
			}

			// 当前不是master
			if !r.isMaster {
				succ, err := redis.Int(r.doRedisCmd("SETNX", r.clusterKey, fmt.Sprintf("%d:%s", time.Now().Unix(), r.nodeId)))
				if err != nil {
					r.OnErr(fmt.Errorf("do redis sexnx err:%v", err))
					continue
				}

				if succ == 0 {
					ttl, err := redis.Int(r.doRedisCmd("TTL", r.clusterKey))
					if err != nil {
						r.OnErr(fmt.Errorf("do redis ttl err:%v", err))
						continue
					}

					// 有可能上次进程退出时候还没来的及设置expire就退出了
					if ttl == -1 || ttl > 60 {
						val, err := r.doRedisGetStr(r.clusterKey)
						if err != nil {
							r.OnErr(fmt.Errorf("do redis get err:%v", err))
							continue
						}

						info := strings.SplitN(val, ":", 2)
						if len(info) < 2 {
							continue
						}

						lastSuccAtStr := info[0]
						lastSuccAt, err := strconv.ParseInt(lastSuccAtStr, 10, 64)
						if err != nil {
							r.OnErr(fmt.Errorf("conv last master success timestamp err:%v", err))
							continue
						}

						if time.Now().Unix()-lastSuccAt > 60 {
							if _, err = r.doRedisCmd("DEL", r.clusterKey); err != nil {
								r.OnErr(fmt.Errorf("do redis del err:%v", err))
								continue
							}
						}
					}

					continue
				}

				if !r.becomeMaster() {
					continue
				}

				_, err = r.doRedisCmd("EXPIRE", r.clusterKey, 15)
				if err != nil {
					r.OnErr(fmt.Errorf("do redis expire err:%v", err))
					continue
				}

				continue
			}

			masterNodeId, err := r.getMasterNodeIdFromRedis()
			if err != nil {
				r.OnErr(fmt.Errorf("get master node id err:%v", err))
				continue
			}

			if masterNodeId != r.nodeId {
				r.lostMaster()
				continue
			}

			_, err = r.doRedisCmd("EXPIRE", r.clusterKey, 10)
			if err != nil {
				r.OnErr(fmt.Errorf("do redis expire err:%v", err))
				continue
			}

			masterNodeId, err = r.getMasterNodeIdFromRedis()
			if err != nil {
				r.OnErr(fmt.Errorf("get master node id err:%v", err))
				continue
			}

			if masterNodeId != r.nodeId {
				r.lostMaster()
				continue
			}
		}
	out:
		return
	})
}

func (r *RedisElect) OnErr(err error) {
	if r.onErr != nil {
		r.onErr(err)
	}
}

func (r *RedisElect) GetClusterKey() string {
	return r.clusterKey
}

func (r *RedisElect) GetName() string {
	return r.name
}

func (r *RedisElect) GetNodeId() string {
	return r.nodeId
}

func (r *RedisElect) IsMaster() bool {
	return r.isMaster
}

func (r *RedisElect) lostMaster() {
	r.isMaster = false
	if r.onLostMater != nil {
		r.onLostMater()
	}
}

func (r *RedisElect) becomeMaster() bool {
	r.isMaster = true
	if r.onBeMaster != nil {
		if !r.onBeMaster() {
			r.isMaster = false
			return false
		}
	}
	return true
}
