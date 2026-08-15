package httpserver

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

type fakeMessagePublisher struct {
	topic string
	key   []byte
	body  any
	err   error
}

func (f *fakeMessagePublisher) Publish(_ context.Context, topic string, key []byte, body any) error {
	f.topic = topic
	f.key = append([]byte(nil), key...)
	f.body = body
	return f.err
}

type fakeEventService struct {
	event biz.Event
	err   error
}

func (f *fakeEventService) Publish(_ context.Context, event biz.Event) error {
	f.event = event
	return f.err
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
	return performRequestWithServices(HTTPServices{User: service, Publisher: &fakeMessagePublisher{}}, method, path, body)
}

func performRequestWithServices(services HTTPServices, method, path, body string) *ut.ResponseRecorder {
	var requestBody *ut.Body
	if body != "" {
		requestBody = &ut.Body{Body: strings.NewReader(body), Len: len(body)}
	}
	h := NewHTTPServer()
	RegisterHTTPRoutes(h, services)
	return ut.PerformRequest(
		h.Engine,
		method,
		path,
		requestBody,
		ut.Header{Key: "Content-Type", Value: "application/json"},
	)
}

func TestHTTPPublishesEvent(t *testing.T) {
	service := &fakeEventService{}
	response := performRequestWithServices(
		HTTPServices{User: &fakeHTTPService{}, Event: service},
		http.MethodPost,
		"/v1/events/user-events",
		`{"key":"user-1","payload":{"action":"created"}}`,
	)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusAccepted, response.Body.String())
	}
	if service.event.Topic != "user-events" || string(service.event.Key) != "user-1" || string(service.event.Payload) != `{"action":"created"}` {
		t.Fatalf("event = %#v", service.event)
	}
}

func TestHTTPMapsEventPublishErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{name: "topic", err: biz.ErrEventTopicNotAllowed, want: http.StatusBadRequest},
		{name: "producer", err: errors.New("broker unavailable"), want: http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := performRequestWithServices(
				HTTPServices{User: &fakeHTTPService{}, Event: &fakeEventService{err: tc.err}},
				http.MethodPost, "/v1/events/user-events", `{"payload":{"id":1}}`,
			)
			if response.Code != tc.want {
				t.Fatalf("status = %d, want %d", response.Code, tc.want)
			}
		})
	}
}

func TestHTTPCreateUser(t *testing.T) {
	queryService := &fakeHTTPService{}
	publisher := &fakeMessagePublisher{}
	response := performRequestWithServices(
		HTTPServices{User: queryService, Publisher: publisher},
		http.MethodPost,
		"/v1/users",
		`{"name":"Alice","email":"alice@example.com"}`,
	)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
	var body biz.PublishAccepted
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Status != "accepted" || body.ResourceID == "" || body.MessageID == "" {
		t.Fatalf("body = %#v", body)
	}
	message, ok := publisher.body.(biz.UserCreateMessage)
	if !ok || message.Name != "Alice" || message.Email != "alice@example.com" {
		t.Fatalf("message = %#v", publisher.body)
	}
	if publisher.topic != biz.UserCreateTopic || string(publisher.key) != message.UserID {
		t.Fatalf("publish metadata topic=%q key=%q message=%#v", publisher.topic, publisher.key, message)
	}
	if queryService.created != nil {
		t.Fatal("HTTP create must not call synchronous UserService.CreateUser")
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

func TestHTTPMapsAsyncUserMessageErrors(t *testing.T) {
	invalid := performRequest(&fakeHTTPService{}, http.MethodPost, "/v1/users", `{"name":"","email":"alice@example.com"}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d", invalid.Code)
	}
	publisherFailure := performRequestWithServices(HTTPServices{
		User: &fakeHTTPService{}, Publisher: &fakeMessagePublisher{err: errors.New("broker unavailable")},
	}, http.MethodPost, "/v1/users", `{"name":"Alice","email":"alice@example.com"}`)
	if publisherFailure.Code != http.StatusServiceUnavailable {
		t.Fatalf("publisher status = %d", publisherFailure.Code)
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
