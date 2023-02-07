# exemplars-storage

## Supported Features

- Prometheus Remote Write Receiver to ingest exemplars
- [Querying exemplars API](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-exemplars)

TODO:
1. Work as a Thanos store that serves Exemplars API only.

## Supported Storages

- FrostDB

## How to use

### Build

```bash
go build -o main main.go
./main
```
