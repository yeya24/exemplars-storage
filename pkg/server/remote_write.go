package server

import (
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/klauspost/compress/snappy"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

func (e *ExemplarServer) RemoteWrite(w http.ResponseWriter, r *http.Request) {
	req, err := DecodeWriteRequest(r.Body)
	if err != nil {
		level.Error(e.logger).Log("msg", "Error decoding remote write request", "err", err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var exemplarErr error
	for _, ts := range req.Timeseries {
		lbls := labelProtosToLabels(ts.Labels)
		for _, ep := range ts.Exemplars {
			exemplar := exemplarProtoToExemplar(ep)
			if err := e.store.AppendExemplar(r.Context(), lbls, exemplar); err != nil {
				level.Error(e.logger).Log("msg", "Error while adding exemplar in AddExemplar", "exemplar", fmt.Sprintf("%+v", exemplar), "err", exemplarErr)
			}
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// DecodeWriteRequest from an io.Reader into a prompb.WriteRequest, handling
// snappy decompression.
func DecodeWriteRequest(r io.Reader) (*prompb.WriteRequest, error) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, err
	}

	return &req, nil
}

func labelProtosToLabels(labelPairs []prompb.Label) labels.Labels {
	b := labels.ScratchBuilder{}
	for _, l := range labelPairs {
		b.Add(l.Name, l.Value)
	}
	b.Sort()
	return b.Labels()
}

func exemplarProtoToExemplar(ep prompb.Exemplar) exemplar.Exemplar {
	timestamp := ep.Timestamp

	return exemplar.Exemplar{
		Labels: labelProtosToLabels(ep.Labels),
		Value:  ep.Value,
		Ts:     timestamp,
		HasTs:  timestamp != 0,
	}
}
