package server

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/thanos-io/thanos/pkg/exemplars/exemplarspb"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yeya24/exemplars-storage/pkg/storage"
)

type ExemplarServer struct {
	store storage.ExemplarStore

	logger log.Logger
	reg    *prometheus.Registry
	Mux    *chi.Mux
}

func NewExemplarServer(logger log.Logger, reg *prometheus.Registry, store storage.ExemplarStore) *ExemplarServer {
	es := &ExemplarServer{
		store:  store,
		logger: logger,
		reg:    reg,
	}
	mux := chi.NewRouter()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}).ServeHTTP(w, r)
	})
	mux.Post("/api/v1/write", es.RemoteWrite)
	mux.Post("/api/v1/query_exemplars", es.QueryExemplars)
	mux.Get("/api/v1/query_exemplars", es.QueryExemplars)
	es.Mux = mux
	return es
}

func (e *ExemplarServer) Exemplars(r *exemplarspb.ExemplarsRequest, s exemplarspb.Exemplars_ExemplarsServer) error {
	expr, err := parser.ParseExpr(r.Query)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	matchers := parser.ExtractSelectors(expr)
	results, err := e.store.Select(context.Background(), r.Start, r.End, matchers...)
	if err != nil {
		return err
	}

	for _, res := range results {
		err = s.Send(&exemplarspb.ExemplarsResponse{
			Result: &exemplarspb.ExemplarsResponse_Data{Data: &exemplarspb.ExemplarData{
				SeriesLabels: labelpb.ZLabelSet{
					Labels: labelpb.ZLabelsFromPromLabels(res.SeriesLabels),
				},
				Exemplars: exemplarsToThanosExemplars(res.Exemplars),
			}},
		})
	}

	return nil
}

func exemplarsToThanosExemplars(exemplars []exemplar.Exemplar) []*exemplarspb.Exemplar {
	res := make([]*exemplarspb.Exemplar, 0, len(exemplars))
	for _, e := range exemplars {
		res = append(res, &exemplarspb.Exemplar{
			Labels: labelpb.ZLabelSet{
				Labels: labelpb.ZLabelsFromPromLabels(e.Labels),
			},
			Value: e.Value,
			Ts:    e.Ts,
		})
	}
	return res
}
