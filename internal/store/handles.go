package store

import (
	"context"
	"time"

	"github.com/adonm/pgvs3/internal/ns"
)

type Cache struct {
	s    *Store
	name string
	sc   string
	err  error
}

type Queue struct {
	s    *Store
	name string
	sc   string
	err  error
}

func (s *Store) Cache(name string) Cache {
	sc, err := nsSQLIdent(ns.Cache, name)
	return Cache{s: s, name: name, sc: sc, err: err}
}

func (s *Store) Queue(name string) QueueHandle {
	return s.queue(name)
}

func (s *Store) queue(name string) Queue {
	sc, err := nsSQLIdent(ns.Queue, name)
	return Queue{s: s, name: name, sc: sc, err: err}
}

type QueueHandle interface {
	Stats(context.Context) (QueueStats, error)
	Info(context.Context) (StreamInfo, error)
	AppendMessages(context.Context, []StreamMessageInput) ([]StreamMessage, error)
	ReceiveBatch(context.Context, string, int) (*StreamBatch, error)
	ReceiveCoopBatch(context.Context, string, string, int) (*StreamBatch, error)
	GetBatch(context.Context, int64) (*StreamBatch, error)
	AckBatch(context.Context, int64) (int64, error)
	NackMessage(context.Context, int64, string, time.Duration, string) (string, error)
	ListConsumers(context.Context) ([]StreamConsumerInfo, error)
	Subscribe(context.Context, string, *int64) (StreamSubscription, error)
	Unsubscribe(context.Context, string) (bool, error)
	Config(context.Context) (StreamConfig, error)
	SetConfig(context.Context, int, time.Duration) (StreamConfig, error)
	ListDeadLetters(context.Context, int) ([]DeadLetter, error)
	PurgeDeadLetters(context.Context, time.Duration) (int64, error)
	ReplayAllDeadLetters(context.Context) (int64, error)
	ReplayDeadLetter(context.Context, int64) (string, error)
}

var _ QueueHandle = Queue{}
