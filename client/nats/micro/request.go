// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package micro

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kubemq-io/broker/client/nats"
)

type (
	// Request represents service request available in the service handler.
	// It exposes methods to respond to the request, as well as
	// getting the request data and headers.
	Request struct {
		msg          *nats.Msg
		respondError error
	}

	// RequestHandler is a function used as a Handler for a service.
	RequestHandler func(*Request)

	// Headers is a wrapper around [*nats.Header]
	Headers nats.Header
)

var (
	ErrRespond         = errors.New("NATS error when sending response")
	ErrMarshalResponse = errors.New("marshaling response")
	ErrArgRequired     = errors.New("argument required")
)

// RespondOpt is a
type RespondOpt func(*nats.Msg)

func (r *Request) Respond(response []byte, opts ...RespondOpt) error {
	respMsg := &nats.Msg{
		Data: response,
	}
	for _, opt := range opts {
		opt(respMsg)
	}

	if err := r.msg.RespondMsg(respMsg); err != nil {
		r.respondError = fmt.Errorf("%w: %s", ErrRespond, err)
		return r.respondError
	}

	return nil
}

func (r *Request) RespondJSON(response interface{}, opts ...RespondOpt) error {
	resp, err := json.Marshal(response)
	if err != nil {
		return ErrMarshalResponse
	}
	return r.Respond(resp, opts...)
}

// Error prepares and publishes error response from a handler.
// A response error should be set containing an error code and description.
// Optionally, data can be set as response payload.
func (r *Request) Error(code, description string, data []byte, opts ...RespondOpt) error {
	if code == "" {
		return fmt.Errorf("%w: error code", ErrArgRequired)
	}
	if description == "" {
		return fmt.Errorf("%w: description", ErrArgRequired)
	}
	response := &nats.Msg{
		Header: nats.Header{
			ErrorHeader:     []string{description},
			ErrorCodeHeader: []string{code},
		},
	}
	for _, opt := range opts {
		opt(response)
	}

	response.Data = data
	if err := r.msg.RespondMsg(response); err != nil {
		r.respondError = err
		return err
	}
	return nil
}

func WithHeaders(headers Headers) RespondOpt {
	return func(m *nats.Msg) {
		if m.Header == nil {
			m.Header = nats.Header(headers)
			return
		}

		for k, v := range headers {
			m.Header[k] = v
		}
	}
}

func (r *Request) Data() []byte {
	return r.msg.Data
}

func (r *Request) Headers() Headers {
	return Headers(r.msg.Header)
}

// Get gets the first value associated with the given key.
// It is case-sensitive.
func (h Headers) Get(key string) string {
	return nats.Header(h).Get(key)
}

// Values returns all values associated with the given key.
// It is case-sensitive.
func (h Headers) Values(key string) []string {
	return nats.Header(h).Values(key)
}
