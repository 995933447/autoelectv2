package etcdv3

import (
	"context"
	"github.com/995933447/autoelectv2"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

var _ autoelectv2.AutoElection = (*AutoElection)(nil)

func New(cluster string, etcdCli *clientv3.Client, electIntervalSec uint32) (autoelectv2.AutoElection, error) {
	elect := &AutoElection{
		etcdCli:          etcdCli,
		electIntervalSec: electIntervalSec,
		stopSignCh:       make(chan struct{}),
	}

	sess, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(int(electIntervalSec)))
	if err != nil {
		return nil, err
	}

	elect.etcdMuCli = concurrency.NewMutex(sess, cluster)

	return elect, nil
}

type AutoElection struct {
	etcdCli               *clientv3.Client
	etcdMuCli             *concurrency.Mutex
	isMaster              bool
	electIntervalSec      uint32
	masterExpireTime      *time.Time
	stopSignCh            chan struct{}
	CheckConditionDoElect func() bool
}

func (a AutoElection) IsMaster() bool {
	return a.isMaster
}

func (a AutoElection) LoopInElect(ctx context.Context, errCh chan error) {
	for {
		select {
		case _ = <-a.stopSignCh:
			goto out
		default:
		}

		if a.isMaster {
			// 续期
			if time.Now().Add(8 * time.Second).After(*a.masterExpireTime) {
				err := a.etcdMuCli.TryLock(ctx)
				if err != nil {
					if err != concurrency.ErrLocked {
						errCh <- err
					}
					// 刷新失败，当失去了 master 地位
					a.lostMaster()
				}
				continue
			}

			time.Sleep(time.Second)
			continue
		}

		if a.CheckConditionDoElect != nil && !a.CheckConditionDoElect() {
			time.Sleep(time.Second)
			continue
		}

		err := a.etcdMuCli.Lock(ctx)
		if err != nil {
			if err != concurrency.ErrSessionExpired {
				errCh <- err
				time.Sleep(time.Second)
			}
			continue
		}

		a.becomeMaster()
	}
out:
	return
}

func (a *AutoElection) lostMaster() {
	a.isMaster = false
	a.masterExpireTime = nil
}

func (a *AutoElection) becomeMaster() {
	a.isMaster = true
	*a.masterExpireTime = time.Now().Add(time.Second * time.Duration(a.electIntervalSec))
}

func (a AutoElection) StopElect() {
	a.stopSignCh <- struct{}{}
}
