package autoelectv2

import (
	"context"
)

type AutoElection interface {
	IsMaster() bool
	LoopInElect(ctx context.Context, errCh chan error)
	StopElect()
	OnBeMaster(func() bool)
	OnLostMaster(func())
}
