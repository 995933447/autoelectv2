package autoelectv2

import (
	"context"
)

type AutoElection interface {
	IsMaster() bool
	LoopInElect(ctx context.Context, errCh chan error) // Deprecated
	LoopInElectV2(ctx context.Context, onErr func(err error))
	StopElect()
	OnBeMaster(func() bool)
	OnLostMaster(func())
}
