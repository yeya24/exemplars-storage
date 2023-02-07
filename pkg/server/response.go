package server

import (
	"context"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"net/http"
)

type response struct {
	HTTPStatusCode int         `json:"-"` // http response status code
	Status         string      `json:"status"`
	Data           interface{} `json:"data,omitempty"`
	ErrorType      string      `json:"errorType,omitempty"`
	Error          string      `json:"error,omitempty"`
	Warnings       []string    `json:"warnings,omitempty"`
}

func (e *response) Render(w http.ResponseWriter, r *http.Request) error {
	render.Status(r, e.HTTPStatusCode)
	return nil
}

func SuccessResponse(data interface{}) render.Renderer {
	return &response{
		Status:         "success",
		HTTPStatusCode: 200,
		Data:           data,
	}
}

func ErrBadData(err error) render.Renderer {
	return &response{
		Status:         "error",
		HTTPStatusCode: http.StatusBadRequest,
		Error:          err.Error(),
		ErrorType:      "bad_data",
	}
}

func returnAPIErrorWrapper(err error) render.Renderer {
	cause := errors.Unwrap(err)
	if cause == nil {
		cause = err
	}
	if errors.Is(err, context.Canceled) {
		return ErrCanceled(err)
	}
	return ErrInternal(err)
}

func ErrInternal(err error) render.Renderer {
	return &response{
		Status:         "error",
		HTTPStatusCode: http.StatusInternalServerError,
		Error:          err.Error(),
		ErrorType:      "internal",
	}
}

func ErrCanceled(err error) render.Renderer {
	return &response{
		Status:         "error",
		HTTPStatusCode: 499,
		Error:          err.Error(),
		ErrorType:      "canceled",
	}
}
