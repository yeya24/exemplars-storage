# exemplars-storage

Storing Prometheus exemplars using different storage systems.

## Supported Features

- Prometheus Remote Write Receiver to ingest exemplars
- [Querying exemplars API](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-exemplars)
- Work as a Thanos store that serves Info and Exemplars API.

## Supported Storages

- [FrostDB](https://github.com/polarsignals/frostdb)

## How to use

### Build

```bash
go build -o main main.go
./main
```

### Prometheus Setup

Add following section to your Prometheus config file to send exemplars to the server.

```yaml
remote_write:
  - url: http://localhost:8081/api/v1/write
    send_exemplars: true
```
