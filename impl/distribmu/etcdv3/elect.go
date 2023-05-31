package etcdv3

import (
	"context"
	"github.com/995933447/autoelectv2"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

var _ autoelectv2.AutoElection = (*AutoElection)(nil)

func New(cluster string, etcdCli *clientv3.Client, masterTenancySec uint32) (autoelectv2.AutoElection, error) {
	elect := &AutoElection{
		cluster:          cluster,
		etcdCli:          etcdCli,
		masterTenancySec: masterTenancySec,
		stopSignCh:       make(chan struct{}),
	}

	var err error
	elect.etcdConcurSess, err = concurrency.NewSession(etcdCli, concurrency.WithTTL(int(masterTenancySec)))
	if err != nil {
		return nil, err
	}

	elect.etcdMuCli = concurrency.NewMutex(elect.etcdConcurSess, cluster)

	return elect, nil
}

type AutoElection struct {
	cluster               string
	etcdCli               *clientv3.Client
	etcdMuCli             *concurrency.Mutex
	etcdConcurSess        *concurrency.Session
	isMaster              bool
	masterTenancySec      uint32
	masterExpireTime      *time.Time
	stopSignCh            chan struct{}
	CheckConditionDoElect func() bool
	onBeMaster            func()
	onLostMater           func()
}

func (a AutoElection) OnBeMaster(fun func()) {
	a.onBeMaster = fun
}

func (a AutoElection) OnLostMaster(fun func()) {
	a.onLostMater = fun
}

func (a AutoElection) SetCheckConditionDoElectFunc(fun func() bool) {
	a.CheckConditionDoElect = fun
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

		if a.etcdMuCli == nil {
			if err := a.resetEtcdMu(); err != nil {
				errCh <- err
				time.Sleep(time.Second)
				continue
			}
		}

		if a.isMaster {
			// make sure the session is not expired, and the owner key still exists.
			resp, err := a.etcdCli.Get(ctx, a.etcdMuCli.Key())
			if err != nil {
				errCh <- err
				if err = a.etcdMuCli.Unlock(a.etcdCli.Ctx()); err != nil {
					errCh <- err
				}
				a.lostMaster()
				continue
			}

			if len(resp.Kvs) == 0 {
				a.lostMaster()
				if err = a.resetEtcdMu(); err != nil {
					errCh <- err
					time.Sleep(time.Second)
					continue
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
				continue
			}

			if err = a.resetEtcdMu(); err != nil {
				errCh <- err
				time.Sleep(time.Second)
				continue
			}
		}

		a.becomeMaster()
	}
out:
	return
}

func (a *AutoElection) resetEtcdMu() error {
	a.etcdMuCli = nil
	a.etcdConcurSess = nil

	var err error
	a.etcdConcurSess, err = concurrency.NewSession(a.etcdCli, concurrency.WithTTL(int(a.masterTenancySec)))
	if err != nil {
		return err
	}

	a.etcdMuCli = concurrency.NewMutex(a.etcdConcurSess, a.cluster)
	return nil
}

func (a *AutoElection) lostMaster() {
	a.isMaster = false
	a.masterExpireTime = nil
	if a.onLostMater != nil {
		a.onLostMater()
	}
}

func (a *AutoElection) becomeMaster() {
	a.isMaster = true
	*a.masterExpireTime = time.Now().Add(time.Second * time.Duration(a.masterTenancySec))
	if a.onBeMaster != nil {
		a.onBeMaster()
	}
}

func (a AutoElection) StopElect() {
	a.stopSignCh <- struct{}{}
}
