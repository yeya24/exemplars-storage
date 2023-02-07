package storage

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/trace"

	"github.com/yeya24/exemplars-storage/pkg/storage/frostdb"
)

type ExemplarStoreType string

const (
	FrostDBExemplarStore ExemplarStoreType = "frostdb"
)

type ExemplarStore interface {
	ExemplarAppender
	ExemplarQuerier
}

type ExemplarAppender interface {
	AppendExemplar(ctx context.Context, lset labels.Labels, e exemplar.Exemplar) error
}

type ExemplarQuerier interface {
	Select(ctx context.Context, start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error)
}

func NewExemplarStore(
	logger log.Logger,
	tracer trace.Tracer,
	reg prometheus.Registerer,
	storeType ExemplarStoreType,
) (ExemplarStore, error) {
	switch storeType {
	case FrostDBExemplarStore:
		return frostdb.NewFrostDBStore(logger, tracer, reg, "exemplars")
	}
	return nil, nil
}
