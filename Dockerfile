FROM golang:1.25 AS build-env
RUN mkdir -p /go/src/github.com/eumel8/app
WORKDIR /go/src/github.com/eumel8/app
COPY  . .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o static-app
# release stage
FROM alpine:latest
COPY --from=build-env /etc/passwd /etc/passwd
RUN adduser -u 1000 -h appuser -D appuser
WORKDIR /appuser
COPY --from=build-env /go/src/github.com/eumel8/app/static-app static-app
USER appuser
ENTRYPOINT ["/appuser/static-app"]
