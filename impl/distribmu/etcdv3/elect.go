package etcdv3

import (
	"context"
	"errors"
	"github.com/995933447/autoelectv2"
	"github.com/995933447/runtimeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	cluster          string
	etcdCli          *clientv3.Client
	etcdMuCli        *concurrency.Mutex
	etcdConcurSess   *concurrency.Session
	isMaster         bool
	masterTenancySec uint32
	stopSignCh       chan struct{}
	onBeMaster       func() bool
	onLostMater      func()
	onErr            func(err error)
}

func (a *AutoElection) LoopInElectV2(ctx context.Context, onErr func(err error)) {
	a.Run(ctx, onErr)
}

func (a *AutoElection) OnErr(err error) {
	if a.onErr != nil {
		a.onErr(runtimeutil.NewStackErrWithSkip(2, err))
	}
}

func (a *AutoElection) OnBeMaster(fun func() bool) {
	a.onBeMaster = fun
}

func (a *AutoElection) OnLostMaster(fun func()) {
	a.onLostMater = fun
}

func (a *AutoElection) IsMaster() bool {
	return a.isMaster
}

func (a *AutoElection) LoopInElect(ctx context.Context, errCh chan error) {
	a.Run(ctx, func(err error) {
		if errCh != nil {
			errCh <- err
		}
	})
}

func (a *AutoElection) Run(ctx context.Context, onErr func(err error)) {
	a.onErr = onErr

	for {
		select {
		case _ = <-a.stopSignCh:
			goto out
		default:
		}

		if a.etcdMuCli == nil {
			if err := a.resetEtcdMu(); err != nil {
				a.OnErr(err)
				time.Sleep(time.Second)
				continue
			}
		}

		if a.isMaster {
			// make sure the session is not expired, and the owner key still exists.
			resp, err := a.etcdCli.Get(ctx, a.etcdMuCli.Key())
			if err != nil {
				a.OnErr(err)
				if err = a.etcdMuCli.Unlock(a.etcdCli.Ctx()); err != nil {
					a.OnErr(err)
				}
				a.lostMaster()
				continue
			}

			if len(resp.Kvs) == 0 {
				a.lostMaster()
				if err = a.resetEtcdMu(); err != nil {
					a.OnErr(err)
					time.Sleep(time.Second)
					continue
				}
				continue
			}

			time.Sleep(time.Second)
			continue
		}

		err := a.etcdMuCli.Lock(ctx)
		if err != nil {
			if !errors.Is(err, concurrency.ErrSessionExpired) && status.Code(err) != codes.NotFound {
				a.OnErr(err)
				time.Sleep(time.Second)
				continue
			}

			if err = a.resetEtcdMu(); err != nil {
				a.OnErr(err)
				time.Sleep(time.Second)
				continue
			}
		}

		if !a.becomeMaster() {
			if err = a.etcdMuCli.Unlock(a.etcdCli.Ctx()); err != nil {
				a.OnErr(err)
			}
		}
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
	if a.onLostMater != nil {
		a.onLostMater()
	}
}

func (a *AutoElection) becomeMaster() bool {
	a.isMaster = true
	if a.onBeMaster != nil {
		if !a.onBeMaster() {
			a.isMaster = false
			return false
		}
	}
	return true
}

func (a AutoElection) StopElect() {
	a.stopSignCh <- struct{}{}
}
