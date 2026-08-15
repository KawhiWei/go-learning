FROM golang:1.23-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/api-server ./cmd/api-server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/grpc-server ./cmd/grpc-server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/consumer ./cmd/consumer

FROM alpine:3.21

RUN apk add --no-cache ca-certificates wget netcat-openbsd \
    && addgroup -S app \
    && adduser -S -G app app
WORKDIR /app
COPY --from=build /out/api-server /app/api-server
COPY --from=build /out/grpc-server /app/grpc-server
COPY --from=build /out/consumer /app/consumer
COPY configs /app/configs
COPY migrations /app/migrations
USER app
EXPOSE 8080 9090
ENTRYPOINT ["/app/api-server"]
