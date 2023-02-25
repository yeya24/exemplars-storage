// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/thanos-io/thanos/pkg/exemplars"
	"github.com/thanos-io/thanos/pkg/info"
	"github.com/thanos-io/thanos/pkg/prober"
	grpcserver "github.com/thanos-io/thanos/pkg/server/grpc"
	httpserver "github.com/thanos-io/thanos/pkg/server/http"
	"go.opentelemetry.io/otel/trace"

	"github.com/yeya24/exemplars-storage/pkg/server"
	"github.com/yeya24/exemplars-storage/pkg/storage"
)

// A lot of code copy-pasted from https://github.com/thanos-io/thanos/blob/main/cmd/thanos/main.go.

type ExemplarsComponent struct{}

func (c ExemplarsComponent) String() string { return "exemplars-store" }

func main() {
	httpAddr := flag.String("http-address", ":10902", "Listen host:port for HTTP endpoints.")
	grpcAddr := flag.String("grpc-address", ":10901", "Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components.")
	enableThanos := flag.Bool("thanos", false, "Use exemplars storage in Thanos. Make it a Thanos Store and serve Info and Exemplars Requests via gRPC.")

	flag.Parse()
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
	es := server.NewExemplarServer(logger, reg, store)

	comp := ExemplarsComponent{}

	var g run.Group

	grpcProbe := prober.NewGRPC()
	httpProbe := prober.NewHTTP()
	statusProber := prober.Combine(
		httpProbe,
		grpcProbe,
		prober.NewInstrumentation(comp, logger, reg),
	)

	srv := httpserver.New(logger, reg, comp, httpProbe,
		httpserver.WithListen(*httpAddr),
		httpserver.WithGracePeriod(2*time.Minute),
	)
	srv.Handle("/", es.Mux)

	g.Add(func() error {
		statusProber.Healthy()

		return srv.ListenAndServe()
	}, func(err error) {
		statusProber.NotReady(err)
		defer statusProber.NotHealthy(err)

		srv.Shutdown(err)
	})

	if *enableThanos {
		infoSrv := info.NewInfoServer(
			"exemplars-store",
			info.WithExemplarsInfoFunc(),
		)
		gs := grpcserver.New(logger, reg, opentracing.GlobalTracer(), nil, nil, comp, grpcProbe,
			grpcserver.WithServer(exemplars.RegisterExemplarsServer(es)),
			grpcserver.WithServer(info.RegisterInfoServer(infoSrv)),
			grpcserver.WithListen(*grpcAddr),
			grpcserver.WithGracePeriod(2*time.Minute),
		)

		g.Add(func() error {
			statusProber.Ready()
			return gs.ListenAndServe()
		}, func(err error) {
			statusProber.NotReady(err)
			gs.Shutdown(err)
		})
	}

	// Listen for termination signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(logger, cancel)
		}, func(error) {
			close(cancel)
		})
	}

	if err := g.Run(); err != nil {
		// Use %+v for github.com/pkg/errors error to print with stack.
		level.Error(logger).Log("err", fmt.Sprintf("%+v", err))
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "exiting")
}

func interrupt(logger log.Logger, cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-c:
		level.Info(logger).Log("msg", "caught signal. Exiting.", "signal", s)
		return nil
	case <-cancel:
		return errors.New("canceled")
	}
}
