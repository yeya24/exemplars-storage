package main

import (
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel/trace"

	"github.com/yeya24/exemplars-storage/pkg/server"
	"github.com/yeya24/exemplars-storage/pkg/storage"
)

func main() {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewBuildInfoCollector(),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	tracer := trace.NewNoopTracerProvider().Tracer("")
	store, err := storage.NewExemplarStore(logger, tracer, reg, storage.FrostDBExemplarStore)
	if err != nil {
		os.Exit(1)
	}

	s := server.NewExemplarServer(logger, reg, store)
	if err := s.ListenAndServe(":8081"); err != nil {
		level.Error(logger).Log("msg", "Program exited with error", "err", err)
		os.Exit(1)
	}
}
