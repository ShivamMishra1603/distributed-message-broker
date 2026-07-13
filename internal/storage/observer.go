package storage

import "time"

// Observer interface defines callback hooks for storage operations.
type Observer interface {
	ObserveAppend(duration time.Duration, records int, bytes int, err error)
	ObserveRead(duration time.Duration, records int, bytes int, err error)
	ObserveFsync(duration time.Duration, err error)
	ObserveRecovery(duration time.Duration, err error)
	IndexRebuilt()
	SegmentDeleted()
	SetPartitionState(logBytes int64, segmentCount int)
}

// Compile-time assertion that NoopObserver implements Observer.
var _ Observer = NoopObserver{}

// NoopObserver implements Observer with value receivers performing no operations.
type NoopObserver struct{}

func (NoopObserver) ObserveAppend(duration time.Duration, records int, bytes int, err error) {}
func (NoopObserver) ObserveRead(duration time.Duration, records int, bytes int, err error)   {}
func (NoopObserver) ObserveFsync(duration time.Duration, err error)                        {}
func (NoopObserver) ObserveRecovery(duration time.Duration, err error)                     {}
func (NoopObserver) IndexRebuilt()                                                         {}
func (NoopObserver) SegmentDeleted()                                                       {}
func (NoopObserver) SetPartitionState(logBytes int64, segmentCount int)                    {}
