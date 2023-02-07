package server

import (
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
)

var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func (e *ExemplarServer) QueryExemplars(w http.ResponseWriter, r *http.Request) {
	start, err := parseTimeParam(r, "start", minTime)
	if err != nil {
		render.Render(w, r, ErrBadData(errors.Wrapf(err, "invalid parameter start")))
		return
	}
	end, err := parseTimeParam(r, "end", maxTime)
	if err != nil {
		render.Render(w, r, ErrBadData(errors.Wrapf(err, "invalid parameter end")))
		return
	}
	if end.Before(start) {
		err := errors.New("end timestamp must not be before start timestamp")
		render.Render(w, r, ErrBadData(err))
		return
	}

	expr, err := parser.ParseExpr(r.FormValue("query"))
	if err != nil {
		render.Render(w, r, ErrBadData(err))
		return
	}

	selectors := parser.ExtractSelectors(expr)
	if len(selectors) < 1 {
		render.Render(w, r, SuccessResponse(nil))
		return
	}

	res, err := e.store.Select(r.Context(), timestamp.FromTime(start), timestamp.FromTime(end), selectors...)
	if err != nil {
		render.Render(w, r, returnAPIErrorWrapper(err))
		return
	}

	render.Render(w, r, SuccessResponse(res))
}

func parseTimeParam(r *http.Request, paramName string, defaultValue time.Time) (time.Time, error) {
	val := r.FormValue(paramName)
	if val == "" {
		return defaultValue, nil
	}
	result, err := parseTime(val)
	if err != nil {
		return time.Time{}, errors.Wrapf(err, "Invalid time value for '%s'", paramName)
	}
	return result, nil
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, errors.Errorf("cannot parse %q to a valid timestamp", s)
}
