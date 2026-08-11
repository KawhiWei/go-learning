package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/google/uuid"

	"github.com/luck/go-learning/internal/biz"
)

type fakeHTTPService struct {
	created   *biz.User
	got       *biz.User
	createErr error
	getErr    error
}

func (f *fakeHTTPService) CreateUser(_ context.Context, name, email string) (*biz.User, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.created = &biz.User{ID: uuid.New(), Name: name, Email: email, CreatedAt: time.Unix(1, 0).UTC()}
	return f.created, nil
}

func (f *fakeHTTPService) GetUser(_ context.Context, _ uuid.UUID) (*biz.User, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.got, nil
}

func performRequest(service UserService, method, path, body string) *ut.ResponseRecorder {
	var requestBody *ut.Body
	if body != "" {
		requestBody = &ut.Body{Body: strings.NewReader(body), Len: len(body)}
	}
	h := NewHTTPServer()
	RegisterHTTPRoutes(h, HTTPServices{User: service})
	return ut.PerformRequest(
		h.Engine,
		method,
		path,
		requestBody,
		ut.Header{Key: "Content-Type", Value: "application/json"},
	)
}

func TestHTTPCreateUser(t *testing.T) {
	response := performRequest(
		&fakeHTTPService{},
		http.MethodPost,
		"/v1/users",
		`{"name":"Alice","email":"alice@example.com"}`,
	)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusCreated)
	}
	var body userResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Name != "Alice" || body.Email != "alice@example.com" {
		t.Fatalf("body = %#v", body)
	}
}

func TestHTTPRejectsInvalidJSON(t *testing.T) {
	cases := []string{
		`{"name":"Alice","email":"alice@example.com","extra":true}`,
		`{"name":"Alice","email":"alice@example.com"}{}`,
		`not-json`,
	}
	for _, body := range cases {
		response := performRequest(&fakeHTTPService{}, http.MethodPost, "/v1/users", body)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body %q status = %d, want %d", body, response.Code, http.StatusBadRequest)
		}
	}
}

func TestHTTPMapsServiceErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{name: "invalid", err: biz.ErrInvalidArgument, want: http.StatusBadRequest},
		{name: "exists", err: biz.ErrAlreadyExists, want: http.StatusConflict},
		{name: "missing", err: biz.ErrNotFound, want: http.StatusNotFound},
		{name: "internal", err: errors.New("boom"), want: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := performRequest(
				&fakeHTTPService{createErr: tc.err},
				http.MethodPost,
				"/v1/users",
				`{"name":"Alice","email":"alice@example.com"}`,
			)
			if response.Code != tc.want {
				t.Fatalf("status = %d, want %d", response.Code, tc.want)
			}
			var body errorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if body.Error.Code == "" {
				t.Fatal("error code is empty")
			}
		})
	}
}

func TestHTTPGetUserAndRouterErrors(t *testing.T) {
	id := uuid.New()
	service := &fakeHTTPService{got: &biz.User{
		ID: id, Name: "Alice", Email: "alice@example.com", CreatedAt: time.Unix(1, 0).UTC(),
	}}
	response := performRequest(service, http.MethodGet, "/v1/users/"+id.String(), "")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	response = performRequest(service, http.MethodGet, "/v1/users/not-a-uuid", "")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid ID status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	response = performRequest(service, http.MethodGet, "/missing", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing route status = %d, want %d", response.Code, http.StatusNotFound)
	}
	response = performRequest(service, http.MethodPut, "/v1/users/"+id.String(), "")
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("wrong method status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

func TestHTTPServesSwaggerDocumentation(t *testing.T) {
	service := &fakeHTTPService{}
	response := performRequest(service, http.MethodGet, "/openapi.yaml", "")
	if response.Code != http.StatusOK {
		t.Fatalf("openapi status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), "openapi: 3.0.3") {
		t.Fatalf("openapi body does not contain version: %q", response.Body.String())
	}

	response = performRequest(service, http.MethodGet, "/swagger/", "")
	if response.Code != http.StatusOK {
		t.Fatalf("swagger status = %d, want %d", response.Code, http.StatusOK)
	}
	if !strings.Contains(response.Body.String(), "SwaggerUIBundle") {
		t.Fatal("swagger UI bootstrap is missing")
	}
}
